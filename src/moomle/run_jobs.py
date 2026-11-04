"""Orchestrates concurrent batch execution of crawler jobs with telemetry dashboard."""

import asyncio
from time import time

from sqlalchemy import select
from sqlalchemy.orm import Session

from moomle.crawler.async_crawler import AsyncCrawler
from moomle.database.connection import SessionLocal
from moomle.database.models import Job, JobStatus
from moomle.tui.run_jobs import PitwallTelemetryUI


class RunJobs:
    """Orchestrates batched execution of crawler jobs fetched from the database.

    Jobs within each batch execute concurrently, while batches execute sequentially.
    Telemetry is streamed live via a terminal dashboard and persisted to file logs.

    Attributes:
        cap: Maximum number of jobs to crawl before stopping (None for unlimited).
        batchSize: Number of jobs to process concurrently in each sequential batch.
    """

    def __init__(self, cap: int | None = None, batchSize: int = 10) -> None:
        """Initializes RunJobs with an optional crawl cap and batch size.

        Args:
            cap: Maximum number of crawls to perform before stopping (defaults to None).
            batchSize: Number of concurrent crawls per batch (defaults to 10).
        """
        self.cap = cap
        self.batchSize = batchSize

    def _fetchJobs(self, session: Session, limitCount: int | None = None) -> list[Job]:
        """Fetches pending and older jobs from the database ordered by priority.

        Prioritizes PENDING jobs first, followed by oldest updated jobs.

        Args:
            session: Active SQLAlchemy database session.
            limitCount: Maximum number of job records to retrieve.

        Returns:
            List of Job model instances matching the query.
        """
        query = select(Job).order_by(Job.status, Job.last_update)
        if limitCount is not None and limitCount > 0:
            query = query.limit(limitCount)

        return list(session.scalars(query).all())

    async def _processJob(self, jobId: int, targetUrl: str) -> tuple[bool, str | None]:
        """Executes a single crawl job asynchronously and updates its database status upon completion.

        Args:
            jobId: Primary key ID of the job in the database.
            targetUrl: The URL string to crawl.

        Returns:
            Tuple containing boolean success flag and optional error message string.
        """
        crawler = AsyncCrawler(targetUrl)
        try:
            await crawler.crawl()
            with SessionLocal() as session:
                job = session.get(Job, jobId)
                if job:
                    job.status = JobStatus.DONE
                    job.last_update = time()
                    session.commit()
            return True, None
        except Exception as err:
            with SessionLocal() as session:
                job = session.get(Job, jobId)
                if job:
                    job.last_update = time()
                    session.commit()
            return False, str(err)

    async def run(self, cap: int | None = None) -> int:
        """Runs all fetched jobs in sequential batches, with jobs inside each batch executing concurrently.

        Streams real-time metrics to the telemetry interface and writes logs to file.

        Args:
            cap: Optional override for the maximum number of crawls to perform.

        Returns:
            The total number of jobs successfully crawled.
        """
        activeCap = cap if cap is not None else self.cap

        with SessionLocal() as session:
            jobs = self._fetchJobs(session, activeCap)

        telemetry = PitwallTelemetryUI(cap=activeCap, batchSize=self.batchSize)

        if not jobs:
            telemetry.logMessage("No jobs found in database to process.", level="WARN")
            print("[run_jobs] No jobs found in database.")
            return 0

        crawledCount = 0
        totalBatches = (len(jobs) + self.batchSize - 1) // self.batchSize
        semaphore = asyncio.Semaphore(self.batchSize)

        async def _runJobTask(jobId: int, targetUrl: str) -> bool:
            async with semaphore:
                jobStart = time()
                telemetry.jobStarted(jobId, targetUrl)
                success, errorMsg = await self._processJob(jobId, targetUrl)
                duration = time() - jobStart
                telemetry.jobFinished(jobId, targetUrl, success, duration, errorMsg)
                return success

        with telemetry:
            telemetry.logMessage(
                f"Starting session: {len(jobs)} jobs queued across {totalBatches} batches "
                f"(batch size: {self.batchSize}, cap: {activeCap})",
                level="INFO",
            )

            for batchIndex in range(0, len(jobs), self.batchSize):
                if activeCap is not None and crawledCount >= activeCap:
                    telemetry.logMessage(f"Reached crawl cap of {activeCap}. Stopping.", level="INFO")
                    break

                currentBatch = jobs[batchIndex : batchIndex + self.batchSize]
                if activeCap is not None:
                    remainingSlots = activeCap - crawledCount
                    if remainingSlots <= 0:
                        break
                    currentBatch = currentBatch[:remainingSlots]

                batchNumber = (batchIndex // self.batchSize) + 1
                telemetry.batchStarted(batchNumber, totalBatches, len(currentBatch))

                tasks = [
                    _runJobTask(job.id, job.url)
                    for job in currentBatch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                batchSuccesses = sum(1 for res in results if res is True)
                batchErrors = len(currentBatch) - batchSuccesses
                crawledCount += batchSuccesses
                telemetry.batchCompleted(batchNumber, batchSuccesses, batchErrors)

        return crawledCount


if __name__ == "__main__":
    runner = RunJobs()
    asyncio.run(runner.run())
