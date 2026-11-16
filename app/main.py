"""FastAPI 应用入口与 HTTP 路由。"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from . import db as dbmod
from .manager import SlotBusyError, manager
from .models import CreateJobRequest, JobOut
from .sqlvalidation import SqlValidationError

app = FastAPI(
    title="电表日结 SQL 核对 API",
    description=(
        "提交两条只读 SELECT，基于同一已提交快照并行读取，"
        "按行内容与出现次数做有序多重集核对（忽略行序，NULL≠空串），"
        "差异最多展示 20 种。单轮 30 秒，全局同时只允许一个任务。"
    ),
    version="1.0.0",
)


@app.get("/health", tags=["运维"])
def health() -> dict:
    """存活探针；不触碰数据库。"""
    return {"status": "ok"}


@app.get("/jobs", response_model=list[JobOut], tags=["核对任务"])
def list_jobs() -> list[JobOut]:
    """列出近期任务（含终态），状态查询始终可用。"""
    return manager.list_jobs()


@app.post("/jobs", response_model=JobOut, status_code=201, tags=["核对任务"])
def create_job(payload: CreateJobRequest) -> JobOut:
    try:
        job = manager.create_job(payload.left_sql, payload.right_sql, payload.name)
    except SqlValidationError as exc:
        raise HTTPException(status_code=400, detail=f"SQL 校验失败：{exc}") from exc
    except SlotBusyError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "当前已有核对任务在运行，一次只允许一组",
                "active_job_id": exc.active_job_id,
            },
        ) from exc
    return job.snapshot()


@app.get("/jobs/{job_id}", response_model=JobOut, tags=["核对任务"])
def get_job(job_id: str) -> JobOut:
    job = manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return job.snapshot()


@app.post("/jobs/{job_id}/cancel", response_model=JobOut, tags=["核对任务"])
def cancel_job(job_id: str) -> JobOut:
    try:
        job = manager.request_cancel(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
    return job.snapshot()


@app.exception_handler(dbmod.DbError)
def _db_error_handler(_request, exc: dbmod.DbError) -> JSONResponse:
    return JSONResponse(status_code=422, detail={"message": str(exc)})
