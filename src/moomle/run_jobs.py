"""Root entrypoint re-exporting JobProducer, Worker, and RunJobs from moomle.crawler.jobs."""

import asyncio

from moomle.crawler.jobs import JobProducer, RunJobs, Worker

__all__ = ["JobProducer", "Worker", "RunJobs"]


if __name__ == "__main__":
    runner = RunJobs(numWorkers=10, batchSize=10, bufferSize=1000, cap=None)
    try:
        asyncio.run(runner.run())
    except (KeyboardInterrupt, SystemExit):
        pass
