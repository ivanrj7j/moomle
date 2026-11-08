"""Worker implementation for consuming batches of jobs and crawling them sequentially."""

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
        status: Current operational status ('IDLE', 'FETCHING BATCH', 'CRAWLING', 'FINISHED').
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

    async def run(self) -> None:
        """Continuously pulls batches of jobs from producer and crawls URLs one by one."""
        while not self._stopped:
            self.status = "FETCHING BATCH"
            self.currentUrl = "Fetching batch..."
            batch = await self.producer.getJob()
            if not batch:
                self.status = "FINISHED"
                self.currentUrl = None
                self.batchProgress = "-"
                break

            self.batchesProcessed += 1
            totalInBatch = len(batch)

            for index, job in enumerate(batch):
                if self._stopped:
                    break

                jobId = getattr(job, "id", None)
                url = getattr(job, "url", str(job))

                self.currentUrl = url
                self.currentJobStartTime = time()
                self.status = "CRAWLING"
                self.batchProgress = f"{index + 1}/{totalInBatch}"

                success, errorMsg, duration = await self._crawlJob(jobId, url)
                self.jobsProcessed += 1

                if self.telemetry:
                    if success:
                        self.telemetry.logMessage(
                            f"Worker #{self.workerId} crawled in {duration:.2f}s: {url}",
                            level="INFO",
                        )
                    else:
                        self.telemetry.logMessage(
                            f"Worker #{self.workerId} failed ({errorMsg or 'Error'}): {url}",
                            level="WARN",
                        )

            self.currentUrl = None
            self.currentJobStartTime = None
            self.status = "IDLE"
            self.batchProgress = "-"

    def stop(self) -> None:
        """Signals the worker to stop processing new jobs."""
        self._stopped = True
