"""Orchestrator for managing concurrent crawler workers and batch job production with live telemetry."""

import asyncio
from time import time
from typing import Any

from moomle.crawler.jobs.job_producer import JobProducer
from moomle.crawler.jobs.worker import Worker
from moomle.tui.run_jobs import TelemetryUI


class RunJobs:
    """Orchestrates producer-consumer execution of crawler jobs across concurrent workers.

    Acts as the primary data provider for telemetry UI, aggregating and returning live
    metrics from both the JobProducer and active Worker instances.

    Attributes:
        numWorkers: Number of concurrent worker coroutines to run.
        batchSize: Number of URLs dispatched to each worker per batch (defaults to 10).
        bufferSize: Capacity of the JobProducer in-memory buffer (defaults to 1000).
        cap: Maximum number of jobs to crawl before stopping (None for unlimited).
    """

    def __init__(
        self,
        numWorkers: int = 10,
        batchSize: int = 10,
        bufferSize: int = 1000,
        cap: int | None = None,
    ) -> None:
        """Initializes RunJobs with worker count, batch size, buffer size, and crawl cap.

        Args:
            numWorkers: Number of concurrent workers (defaults to 10).
            batchSize: Number of URLs dispatched per worker batch (defaults to 10).
            bufferSize: Target capacity for JobProducer buffer (defaults to 1000).
            cap: Maximum crawls to perform before stopping (defaults to None).
        """
        self.numWorkers = numWorkers
        self.batchSize = batchSize
        self.bufferSize = bufferSize
        self.cap = cap

        self.producer = JobProducer(
            batchSize=self.batchSize,
            bufferSize=self.bufferSize,
            cap=self.cap,
        )
        self.telemetry = TelemetryUI(runner=self, cap=self.cap, batchSize=self.batchSize)
        self.producer.telemetry = self.telemetry
        self.workers = [
            Worker(workerId=i + 1, producer=self.producer, telemetry=self.telemetry)
            for i in range(self.numWorkers)
        ]

    def getTelemetryData(self) -> dict[str, Any]:
        """Gathers and returns a telemetry snapshot of producer stats and active worker states.

        Returns:
            Dictionary containing producer metrics and worker status snapshots.
        """
        now = time()
        workerSnapshots = []
        for worker in self.workers:
            duration = "-"
            if worker.currentJobStartTime is not None and worker.status == "CRAWLING":
                duration = f"{now - worker.currentJobStartTime:.1f}s"

            workerSnapshots.append({
                "workerId": worker.workerId,
                "currentUrl": worker.currentUrl,
                "batchProgress": worker.batchProgress,
                "status": worker.status,
                "duration": duration,
            })

        return {
            "bufferLength": self.producer.bufferLength,
            "bufferSize": self.producer.bufferSize,
            "producedCount": self.producer.producedCount,
            "completedCount": self.producer.completedCount,
            "failedCount": self.producer.failedCount,
            "inFlightCount": self.producer.inFlightCount,
            "isRefilling": self.producer.isRefilling,
            "cap": self.cap,
            "workers": workerSnapshots,
        }

    async def run(self) -> int:
        """Launches all workers asynchronously with live telemetry and awaits completion.

        Returns:
            The total number of jobs successfully crawled.
        """
        with self.telemetry:
            self.telemetry.logMessage(
                f"Starting crawler session: {self.numWorkers} workers, "
                f"batch size {self.batchSize}, buffer {self.bufferSize}, cap {self.cap}",
                level="INFO",
            )
            tasks = [asyncio.create_task(worker.run()) for worker in self.workers]
            await asyncio.gather(*tasks)

        return self.producer.completedCount


if __name__ == "__main__":
    runner = RunJobs(numWorkers=10, batchSize=10, bufferSize=1000, cap=100)
    asyncio.run(runner.run())
