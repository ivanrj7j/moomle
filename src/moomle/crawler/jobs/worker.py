"""Worker implementation for consuming batches of jobs and crawling them sequentially."""

import asyncio
from time import time
from typing import Any

from moomle.crawler.async_crawler import AsyncCrawler
from moomle.crawler.jobs.job_producer import JobProducer
from moomle.database.connection import SessionLocal
from moomle.database.models import Job, JobStatus


class Worker:
    """Worker that consumes batches of jobs from JobProducer and crawls them one by one.

    Attributes:
        workerId: Unique integer identifier for this worker.
        producer: The JobProducer instance supplying job batches.
        telemetry: Optional telemetry UI instance for streaming progress and logs.
        jobsProcessed: Total number of individual URLs crawled by this worker.
        batchesProcessed: Total number of batches processed by this worker.
        currentUrl: The exact URL string currently being crawled (or None if idle).
        currentJobStartTime: Epoch timestamp when crawling the current URL began.
        batchProgress: String indicating progress within the current batch (e.g. '3/10').
        status: Current operational status ('IDLE', 'FETCHING BATCH', 'CRAWLING', 'STOPPED', 'FINISHED').
    """

    def __init__(
        self,
        workerId: int,
        producer: JobProducer,
        telemetry: Any = None,
    ) -> None:
        """Initializes a Worker with an ID, a JobProducer instance, and optional telemetry.

        Args:
            workerId: Unique identifier for the worker.
            producer: The JobProducer supplying job batches.
            telemetry: Optional telemetry UI instance for event reporting.
        """
        self.workerId = workerId
        self.producer = producer
        self.telemetry = telemetry
        self.jobsProcessed = 0
        self.batchesProcessed = 0
        self.currentUrl: str | None = None
        self.currentJobStartTime: float | None = None
        self.batchProgress: str = "-"
        self.status: str = "IDLE"
        self._stopped = False

    def _setFetchingBatch(self) -> None:
        """Updates worker state to indicate it is requesting a new batch from the producer."""
        self.status = "FETCHING BATCH"
        self.currentUrl = "Fetching batch..."
        self.batchProgress = "-"

    def _setActiveJob(self, targetUrl: str, jobIndex: int, totalInBatch: int) -> None:
        """Updates worker state to indicate an active crawl is in progress.

        Args:
            targetUrl: The URL being crawled.
            jobIndex: 1-based index of the job in the current batch.
            totalInBatch: Total number of jobs in the current batch.
        """
        self.status = "CRAWLING"
        self.currentUrl = targetUrl
        self.batchProgress = f"{jobIndex}/{totalInBatch}"
        self.currentJobStartTime = time()

    def _setIdle(self) -> None:
        """Resets worker state to idle upon completing a batch."""
        self.status = "IDLE"
        self.currentUrl = None
        self.currentJobStartTime = None
        self.batchProgress = "-"

    def _setFinished(self) -> None:
        """Sets worker state to finished when no more jobs remain."""
        self.status = "FINISHED"
        self.currentUrl = None
        self.currentJobStartTime = None
        self.batchProgress = "-"

    def _setStopped(self) -> None:
        """Sets worker state to stopped when shutdown signal is received."""
        self.status = "STOPPED"
        self.currentUrl = None
        self.currentJobStartTime = None
        self.batchProgress = "-"

    def _reportJobResult(
        self,
        targetUrl: str,
        success: bool,
        duration: float,
        errorMsg: str | None = None,
    ) -> None:
        """Reports the outcome of an individual crawl to the telemetry UI and log file.

        Args:
            targetUrl: The crawled URL.
            success: Whether the crawl succeeded.
            duration: Crawl duration in seconds.
            errorMsg: Optional error message if the crawl failed.
        """
        if not self.telemetry:
            return

        if success:
            self.telemetry.logMessage(
                f"Worker #{self.workerId} crawled in {duration:.2f}s: {targetUrl}",
                level="INFO",
            )
        else:
            self.telemetry.logMessage(
                f"Worker #{self.workerId} failed ({errorMsg or 'Error'}): {targetUrl}",
                level="WARN",
            )

    async def _crawlJob(self, jobId: int | None, targetUrl: str) -> tuple[bool, str | None, float]:
        """Spawns an AsyncCrawler for a single URL, awaits it, and updates database status.

        Args:
            jobId: Database primary key ID of the job (if present).
            targetUrl: The URL string to crawl.

        Returns:
            Tuple containing boolean success flag, optional error message string, and duration.
        """
        startTime = time()
        crawler = AsyncCrawler(targetUrl)
        try:
            await crawler.crawl()
            if jobId is not None:
                with SessionLocal() as session:
                    job = session.get(Job, jobId)
                    if job:
                        job.status = JobStatus.DONE
                        job.last_update = time()
                        session.commit()
            duration = time() - startTime
            self.producer.recordJobResult(jobId, success=True)
            return True, None, duration
        except Exception as err:
            if jobId is not None:
                with SessionLocal() as session:
                    job = session.get(Job, jobId)
                    if job:
                        job.last_update = time()
                        session.commit()
            duration = time() - startTime
            self.producer.recordJobResult(jobId, success=False, errorMsg=str(err))
            return False, str(err), duration

    async def _processBatch(self, batch: list[Any]) -> None:
        """Sequentially crawls each job in a leased batch, updating status and reporting telemetry.

        Args:
            batch: List of Job model instances or URL strings to crawl.
        """
        totalInBatch = len(batch)
        for index, job in enumerate(batch):
            if self._stopped:
                break

            jobId = getattr(job, "id", None)
            url = getattr(job, "url", str(job))

            self._setActiveJob(url, index + 1, totalInBatch)
            success, errorMsg, duration = await self._crawlJob(jobId, url)
            self.jobsProcessed += 1
            self._reportJobResult(url, success, duration, errorMsg)

        if not self._stopped:
            self._setIdle()

    async def run(self) -> None:
        """Continuously pulls batches of jobs from producer and crawls URLs one by one."""
        try:
            while not self._stopped:
                self._setFetchingBatch()
                batch = await self.producer.getJob()
                if not batch or self._stopped:
                    self._setFinished() if not self._stopped else self._setStopped()
                    break

                self.batchesProcessed += 1
                await self._processBatch(batch)
        except (asyncio.CancelledError, KeyboardInterrupt):
            self.stop()
        finally:
            if self.status != "FINISHED":
                self._setStopped()

    def stop(self) -> None:
        """Signals the worker to stop processing new jobs."""
        self._stopped = True
        self._setStopped()
