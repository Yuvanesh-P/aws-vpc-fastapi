
import logging
import os

import httpx

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Literal


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("public-api")

WORKER_URL = os.getenv(
    "WORKER_URL",
    "http://127.0.0.1:9000"
)

TIMEOUT = 10


# --------------------------------------------------
# HTTP CLIENT
# --------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):

    app.state.client = httpx.AsyncClient(
        base_url=WORKER_URL,
        timeout=TIMEOUT
    )

    yield

    await app.state.client.aclose()


app = FastAPI(
    title="Task Management API",
    description="Public API for managing private worker tasks",
    version="2.0.0",
    lifespan=lifespan
)


# --------------------------------------------------
# REQUEST MODEL
# --------------------------------------------------

class TaskRequest(BaseModel):
    task_name: str = Field(min_length=3, max_length=100)
    description: str = Field(min_length=5, max_length=500)
    priority: Literal["low", "medium", "high"] = "medium"


# --------------------------------------------------
# WORKER COMMUNICATION
# --------------------------------------------------

async def worker_request(method, endpoint, **kwargs):

    try:
        response = await app.state.client.request(
            method,
            endpoint,
            **kwargs
        )

        response.raise_for_status()

        return response.json()

    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail="Private worker request timed out"
        )

    except httpx.HTTPStatusError as error:
        raise HTTPException(
            status_code=error.response.status_code,
            detail=error.response.text
        )

    except httpx.RequestError:
        logger.exception("Worker communication failed")

        raise HTTPException(
            status_code=502,
            detail="Private worker is unavailable"
        )


# --------------------------------------------------
# ROOT
# --------------------------------------------------

@app.get("/")
async def home():

    return {
        "application": "Task Management API",
        "version": "2.0.0",
        "architecture": "Public API -> Private Worker",
        "documentation": "/docs"
    }


# --------------------------------------------------
# PUBLIC HEALTH
# --------------------------------------------------

@app.get("/health")
async def health():

    return {
        "status": "healthy",
        "service": "public-api"
    }


# --------------------------------------------------
# PRIVATE WORKER HEALTH
# --------------------------------------------------

@app.get("/worker-health")
async def worker_health():

    result = await worker_request("GET", "/health")

    return {
        "public_api": "healthy",
        "private_worker": result
    }


# --------------------------------------------------
# CREATE TASK
# --------------------------------------------------

@app.post("/tasks", status_code=202)
async def create_task(data: TaskRequest):

    result = await worker_request(
        "POST",
        "/jobs",
        json=data.model_dump()
    )

    return {
        "message": "Task submitted successfully",
        "worker_response": result
    }


# --------------------------------------------------
# GET TASK DETAILS
# --------------------------------------------------

@app.get("/tasks/{task_id}")
async def get_task(task_id: str):

    return await worker_request(
        "GET",
        f"/jobs/{task_id}"
    )


# --------------------------------------------------
# LIST AND FILTER TASKS
# --------------------------------------------------

@app.get("/tasks")
async def list_tasks(
    status: str | None = Query(default=None),
    priority: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100)
):

    params = {
        "status": status,
        "priority": priority,
        "limit": limit
    }

    params = {
        key: value
        for key, value in params.items()
        if value is not None
    }

    return await worker_request(
        "GET",
        "/jobs",
        params=params
    )


# --------------------------------------------------
# RETRY FAILED TASK
# --------------------------------------------------

@app.post("/tasks/{task_id}/retry", status_code=202)
async def retry_task(task_id: str):

    return await worker_request(
        "POST",
        f"/jobs/{task_id}/retry"
    )


# --------------------------------------------------
# TASK STATISTICS
# --------------------------------------------------

@app.get("/statistics")
async def statistics():

    return await worker_request(
        "GET",
        "/stats"
    )
