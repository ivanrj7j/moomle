"""Jobs module providing producer-consumer crawling components."""

from moomle.crawler.jobs.job_producer import JobProducer
from moomle.crawler.jobs.run_jobs import RunJobs
from moomle.crawler.jobs.worker import Worker

__all__ = ["JobProducer", "Worker", "RunJobs"]
