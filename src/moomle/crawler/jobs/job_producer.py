"""JobProducer for managing buffered job queue, batch dispatching, and database refills."""

import asyncio
from collections import deque
from typing import Any

from sqlalchemy import select, update

from moomle.database.connection import SessionLocal
from moomle.database.models import Job, JobStatus


class JobProducer:
    """Produces batches of crawler jobs from database using a dynamic in-memory buffer.

    Maintains an in-memory buffer of jobs (default 1000). When the buffer drains to half
    its capacity, it asynchronously queries the database to refill the buffer. When getJob()
    is called by a worker, it returns a batch of jobs (default 10).

    Attributes:
        batchSize: Number of jobs dispatched per batch (defaults to 10).
        bufferSize: Maximum target capacity of the in-memory job buffer (defaults to 1000).
        cap: Maximum number of total jobs to produce (None for unlimited).
        telemetry: Optional telemetry instance for streaming system logs.
    """

    def __init__(
        self,
        batchSize: int = 10,
        bufferSize: int = 1000,
        cap: int | None = None,
        telemetry: Any = None,
    ) -> None:
        """Initializes the JobProducer with batch size, buffer capacity, and optional crawl cap.

        Args:
            batchSize: Number of jobs returned per getJob() call (defaults to 10).
            bufferSize: Target buffer size threshold for queued jobs (defaults to 1000).
            cap: Maximum total jobs to produce before stopping (defaults to None).
            telemetry: Optional telemetry UI instance for system event logging.
        """
        self.batchSize = batchSize
        self.bufferSize = bufferSize
        self.cap = cap
        self.telemetry = telemetry
        self._buffer: deque[Job] = deque()
        self._bufferedIds: set[int] = set()
        self._inFlightIds: set[int] = set()
        self._producedCount = 0
        self._completedCount = 0
        self._failedCount = 0
        self._noMoreJobsInDb = False
        self._stopped = False
        self._refillLock = asyncio.Lock()
        self._refillTask: asyncio.Task | None = None

    @property
    def producedCount(self) -> int:
        """Total number of jobs leased out to workers."""
        return self._producedCount

    @property
    def completedCount(self) -> int:
        """Total number of jobs successfully crawled."""
        return self._completedCount

    @property
    def failedCount(self) -> int:
        """Total number of jobs that encountered errors during crawl."""
        return self._failedCount

    @property
    def bufferLength(self) -> int:
        """Current number of jobs waiting in the in-memory buffer."""
        return len(self._buffer)

    @property
    def inFlightCount(self) -> int:
        """Number of jobs currently being crawled across all workers."""
        return len(self._inFlightIds)

    @property
    def isRefilling(self) -> bool:
        """Whether a database refill task is actively in flight."""
        return self._refillTask is not None and not self._refillTask.done()

    def stop(self) -> None:
        """Signals JobProducer to stop, cancels any background refill task,
        and releases all buffered/in-flight jobs back to PENDING in the database."""
        self._stopped = True
        if self._refillTask is not None and not self._refillTask.done():
            self._refillTask.cancel()
        self._releaseWorkingJobs()

    def _releaseWorkingJobs(self) -> None:
        """Resets all buffered and in-flight jobs back to PENDING in the database.

        Called during graceful shutdown so that jobs claimed by this instance
        are not permanently stuck as WORKING.
        """
        jobIds = self._bufferedIds | self._inFlightIds
        if not jobIds:
            return
        try:
            with SessionLocal() as session:
                session.execute(
                    update(Job)
                    .where(Job.id.in_(jobIds))
                    .where(Job.status == JobStatus.WORKING)
                    .values(status=JobStatus.PENDING)
                )
                session.commit()
        except Exception:
            pass  # Best-effort during shutdown

    def _triggerRefill(self) -> None:
        """Spawns a background task to refill the buffer if one is not already running."""
        if self._stopped or self._noMoreJobsInDb:
            return
        if self._refillTask is None or self._refillTask.done():
            self._refillTask = asyncio.create_task(self._refillBuffer())

    def _fetchJobsFromDb(self, limitCount: int, excludeIds: set[int]) -> list[Job]:
        """Queries the database for PENDING jobs, excluding buffered/in-flight IDs,
        and atomically marks them as WORKING to prevent other instances from claiming them.

        Uses SELECT ... FOR UPDATE SKIP LOCKED to ensure safe concurrent access
        across multiple instances.

        Args:
            limitCount: Maximum number of job rows to fetch.
            excludeIds: Set of job primary keys to exclude from the query.

        Returns:
            List of fetched Job model instances (now marked WORKING in the database).
        """
        with SessionLocal() as session:
            query = select(Job).where(Job.status == JobStatus.PENDING)
            if excludeIds:
                query = query.where(~Job.id.in_(excludeIds))
            query = (
                query.order_by(Job.last_update)
                .limit(limitCount)
                .with_for_update(skip_locked=True) # avoiding duplicate jobs
            )
            jobs = list(session.scalars(query).all())

            if jobs:
                for job in jobs:
                    job.status = JobStatus.WORKING
                session.commit()

            return jobs

    async def _refillBuffer(self) -> None:
        """Refills the internal job buffer from the database in a non-blocking thread."""
        async with self._refillLock:
            if self._stopped:
                return

            currentSize = len(self._buffer)
            if currentSize > self.bufferSize // 2:
                return

            needed = self.bufferSize - currentSize
            if self.cap is not None:
                remainingCap = self.cap - self._producedCount - currentSize
                needed = min(needed, max(0, remainingCap))

            if needed <= 0:
                return

            excludeIds = self._bufferedIds | self._inFlightIds
            fetchedJobs = await asyncio.to_thread(self._fetchJobsFromDb, needed, excludeIds)

            if not fetchedJobs:
                self._noMoreJobsInDb = True
                return

            for job in fetchedJobs:
                self._buffer.append(job)
                self._bufferedIds.add(job.id)

            if self.telemetry:
                self.telemetry.logMessage(
                    f"JobProducer refilled buffer (+{len(fetchedJobs)} jobs, total: {len(self._buffer)})",
                    level="INFO",
                )

    async def _ensureBuffer(self) -> bool:
        """Ensures jobs are available in the buffer, initiating and awaiting refills if necessary.

        Returns:
            True if jobs are available in the buffer, False if stopped or exhausted.
        """
        if len(self._buffer) <= self.bufferSize // 2:
            self._triggerRefill()

        while not self._buffer and not self._stopped:
            if self._refillTask is not None and not self._refillTask.done():
                await self._refillTask
            elif self._noMoreJobsInDb:
                if not self._inFlightIds:
                    return False
                await asyncio.sleep(0.2)
                self._noMoreJobsInDb = False
                self._triggerRefill()
            else:
                self._triggerRefill()
                if self._refillTask is not None:
                    await self._refillTask

        return bool(self._buffer) and not self._stopped

    def recordJobResult(self, jobId: int | None, success: bool, errorMsg: str | None = None) -> None:
        """Records the completion of a job crawl and updates internal counters.

        Args:
            jobId: Database ID of the job completed (if known).
            success: Whether the crawl was successful.
            errorMsg: Optional error message if the crawl failed.
        """
        if jobId is not None:
            self._inFlightIds.discard(jobId)

        if success:
            self._completedCount += 1
        else:
            self._failedCount += 1

    async def getJob(self) -> list[Job] | None:
        """Dispatches a batch of jobs (up to batchSize) for a worker to crawl.

        Returns None when stopped, when the cap is reached, or when all jobs are exhausted.

        Returns:
            List of Job objects representing the batch to crawl, or None if exhausted/stopped.
        """
        if self._stopped:
            return None

        if self.cap is not None and self._producedCount >= self.cap:
            return None

        try:
            hasJobs = await self._ensureBuffer()
            if not hasJobs:
                return None
        except (asyncio.CancelledError, KeyboardInterrupt):
            self._stopped = True
            return None

        countToTake = self.batchSize
        if self.cap is not None:
            countToTake = min(countToTake, self.cap - self._producedCount)

        batch: list[Job] = []
        while self._buffer and len(batch) < countToTake:
            job = self._buffer.popleft()
            self._bufferedIds.discard(job.id)
            self._inFlightIds.add(job.id)
            self._producedCount += 1
            batch.append(job)

        # Trigger background refill if buffer dropped to or below half capacity
        if len(self._buffer) <= self.bufferSize // 2:
            self._triggerRefill()

        return batch if batch else None
