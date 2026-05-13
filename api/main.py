# -*- coding: utf-8 -*-
"""
api/main.py
FastAPI 应用入口 + 定时清理任务
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.models.schemas import HealthResponse
from api.utils.session_manager import SessionManager, setup_cleanup_scheduler
from api.routes import upload, validation, analysis


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WEB_DIST = PROJECT_ROOT / "web" / "dist"
WEB_INDEX = WEB_DIST / "index.html"

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 全局 scheduler 实例
scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global scheduler
    
    # 启动时
    logger.info("Starting game_retention_framework API...")
    
    # 启动定时清理任务
    try:
        scheduler = setup_cleanup_scheduler()
        if scheduler:
            logger.info("Cleanup scheduler started")
        else:
            logger.warning("Cleanup scheduler not available (APScheduler not installed)")
    except Exception as e:
        logger.error(f"Failed to start cleanup scheduler: {e}")
    
    yield
    
    # 关闭时
    logger.info("Shutting down...")
    if scheduler:
        scheduler.shutdown()
        logger.info("Cleanup scheduler stopped")


# 创建 FastAPI 应用
app = FastAPI(
    title="Game Retention Framework API",
    description="""
## 游戏留存分析 Web API

提供动态 UI 驱动的留存分析流程：

### 1. 文件上传 (`/api/upload`)
上传 CSV/Excel/Parquet 文件，返回 Schema Discovery 结果（列名、类型、样本预览、映射建议）。

### 2. 字段映射预校验 (`/api/validate-mapping`)
验证字段映射是否正确，测试日期格式解析。

### 3. 执行分析 (`/api/analyze`)
执行完整留存分析，返回留存率、Cohort 矩阵、行为路径等。

### 4. Session 管理
- `/api/session/{session_id}` - 获取 Session 状态
- `/api/sessions` - 列出所有活跃 Session
- `/api/session/{session_id}` (DELETE) - 销毁 Session
    """,
    version="1.0.0",
    lifespan=lifespan,
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应限制为具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(upload.router)
app.include_router(validation.router)
app.include_router(analysis.router)

if (WEB_DIST / "assets").exists():
    app.mount("/assets", StaticFiles(directory=WEB_DIST / "assets"), name="web-assets")


@app.get("/", tags=["Root"])
async def root():
    """根路径"""
    if WEB_INDEX.exists():
        return FileResponse(WEB_INDEX)

    return {
        "name": "Game Retention Framework API",
        "version": "1.0.0",
        "frontend": "请先在 web 目录运行 npm run build，或打开 Vite 开发服务地址",
        "docs": "/docs",
        "redoc": "/redoc"
    }


@app.get("/{full_path:path}", include_in_schema=False)
async def spa_fallback(full_path: str):
    """Serve the built React app for non-API browser routes."""
    reserved_prefixes = ("api/", "docs", "redoc", "openapi.json", "health", "cleanup")
    if WEB_INDEX.exists() and not full_path.startswith(reserved_prefixes):
        return FileResponse(WEB_INDEX)

    return {
        "name": "Game Retention Framework API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }


@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check() -> HealthResponse:
    """健康检查 + 统计信息"""
    # 尝试清理过期文件
    cleaned = 0
    try:
        cleaned = SessionManager.cleanup_expired_files(max_age_hours=1)
    except Exception:
        pass
    
    return HealthResponse(
        status="ok",
        version="1.0.0",
        temp_files_count=SessionManager.get_temp_files_count(),
        cleaned_files_count=cleaned
    )


@app.post("/cleanup", tags=["System"])
async def manual_cleanup() -> dict:
    """手动触发清理任务"""
    cleaned = SessionManager.cleanup_expired_files(max_age_hours=1)
    return {
        "success": True,
        "cleaned_files": cleaned,
        "message": f"已清理 {cleaned} 个过期文件"
    }


# ── 开发服务器入口 ──────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
