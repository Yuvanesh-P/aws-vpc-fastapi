
import asyncio
import logging
import uuid

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("private-worker")

PROCESSING_SECONDS = 15
QUEUE_VISIBILITY_DELAY = 3

jobs = {}
job_queue = asyncio.PriorityQueue()
worker_task = None
sequence = 0

PRIORITY_MAP = {
    "high": 1,
    "medium": 2,
    "low": 3
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------
# REQUEST MODEL
# --------------------------------------------------

class JobRequest(BaseModel):
    task_name: str = Field(min_length=3, max_length=100)
    description: str = Field(min_length=5, max_length=500)
    priority: Literal["low", "medium", "high"] = "medium"


# --------------------------------------------------
# BACKGROUND QUEUE WORKER
# --------------------------------------------------

async def process_jobs():

    logger.info("Worker queue processor started")

    while True:

        priority, order, job_id = await job_queue.get()

        try:
            job = jobs.get(job_id)

            if not job or job["status"] != "queued":
                continue

            # Keep queued state visible for demo
            await asyncio.sleep(QUEUE_VISIBILITY_DELAY)

            job["status"] = "processing"
            job["started_at"] = utc_now()

            logger.info("Processing job: %s", job_id)

            # Simulate actual workload
            await asyncio.sleep(PROCESSING_SECONDS)

            job["status"] = "completed"
            job["completed_at"] = utc_now()
            job["progress"] = 100

            job["result"] = {
                "message": "Task processed successfully",
                "processed_by": "private-worker",
                "execution_time_seconds": PROCESSING_SECONDS
            }

            logger.info("Completed job: %s", job_id)

        except asyncio.CancelledError:
            raise

        except Exception as error:
            logger.exception("Job failed: %s", job_id)

            if job_id in jobs:
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = str(error)
                jobs[job_id]["failed_at"] = utc_now()

        finally:
            job_queue.task_done()


# --------------------------------------------------
# APPLICATION LIFECYCLE
# --------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):

    global worker_task

    worker_task = asyncio.create_task(process_jobs())

    yield

    worker_task.cancel()

    try:
        await worker_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Private Worker Service",
    description="Private task processing and queue management API",
    version="2.0.0",
    lifespan=lifespan
)


# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------

@app.get("/")
async def home():
    return {
        "service": "Private Worker",
        "version": "2.0.0",
        "status": "running"
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "private-worker",
        "queue_size": job_queue.qsize(),
        "timestamp": utc_now()
    }


# --------------------------------------------------
# CREATE JOB
# --------------------------------------------------

@app.post("/jobs", status_code=202)
async def create_job(data: JobRequest):

    global sequence

    job_id = str(uuid.uuid4())
    sequence += 1

    jobs[job_id] = {
        "job_id": job_id,
        "task_name": data.task_name,
        "description": data.description,
        "priority": data.priority,
        "status": "queued",
        "progress": 0,
        "created_at": utc_now(),
        "started_at": None,
        "completed_at": None,
        "result": None,
        "error": None,
        "retry_count": 0
    }

    await job_queue.put(
        (PRIORITY_MAP[data.priority], sequence, job_id)
    )

    logger.info("Job queued: %s", job_id)

    return {
        "message": "Job successfully added to queue",
        "job_id": job_id,
        "status": "queued",
        "queue_position": job_queue.qsize()
    }


# --------------------------------------------------
# GET SINGLE JOB
# --------------------------------------------------

@app.get("/jobs/{job_id}")
async def get_job(job_id: str):

    if job_id not in jobs:
        raise HTTPException(404, "Job not found")

    return jobs[job_id]


# --------------------------------------------------
# LIST AND FILTER JOBS
# --------------------------------------------------

@app.get("/jobs")
async def list_jobs(
    status: str | None = Query(default=None),
    priority: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100)
):

    results = list(jobs.values())

    if status:
        results = [j for j in results if j["status"] == status]

    if priority:
        results = [j for j in results if j["priority"] == priority]

    results.sort(
        key=lambda j: j["created_at"],
        reverse=True
    )

    return {
        "total": len(results),
        "jobs": results[:limit]
    }


# --------------------------------------------------
# RETRY FAILED JOB
# --------------------------------------------------

@app.post("/jobs/{job_id}/retry", status_code=202)
async def retry_job(job_id: str):

    global sequence

    if job_id not in jobs:
        raise HTTPException(404, "Job not found")

    job = jobs[job_id]

    if job["status"] != "failed":
        raise HTTPException(
            409,
            "Only failed jobs can be retried"
        )

    sequence += 1

    job["status"] = "queued"
    job["progress"] = 0
    job["retry_count"] += 1
    job["error"] = None
    job["created_at"] = utc_now()
    job["started_at"] = None
    job["completed_at"] = None
    job["result"] = None

    await job_queue.put(
        (PRIORITY_MAP[job["priority"]], sequence, job_id)
    )

    return {
        "message": "Job added to retry queue",
        "job_id": job_id,
        "status": "queued"
    }


# --------------------------------------------------
# WORKER STATISTICS
# --------------------------------------------------

@app.get("/stats")
async def statistics():

    counts = {
        "queued": 0,
        "processing": 0,
        "completed": 0,
        "failed": 0
    }

    for job in jobs.values():
        if job["status"] in counts:
            counts[job["status"]] += 1

    return {
        "total_jobs": len(jobs),
        "queue_size": job_queue.qsize(),
        "job_statistics": counts,
        "worker_status": "running"
    }
