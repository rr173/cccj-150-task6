"""FastAPI 入口。"""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .db import init_db
from .routers import parcels, sync as sync_router, disputes, lifecycle, certificates, corrections, plans as plans_router, snapshots as snapshots_router
from .services import plans as plans_svc

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="地籍界址测量与宗地版本管理平台", version="1.3.0")


@app.on_event("startup")
def _startup():
    init_db()
    # 生效计划执行器：启动即回收上一进程的在途租约并补跑（持久租约/完成令牌）
    plans_svc.get_executor().start()


@app.on_event("shutdown")
def _shutdown():
    plans_svc.get_executor().stop()


app.include_router(parcels.router)
app.include_router(sync_router.router)
app.include_router(disputes.router)
app.include_router(lifecycle.router)
app.include_router(certificates.router)
app.include_router(corrections.router)
app.include_router(plans_router.router)
app.include_router(snapshots_router.router)


@app.get("/api/health")
def health():
    return {"ok": True, "service": "cadastre"}


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))
