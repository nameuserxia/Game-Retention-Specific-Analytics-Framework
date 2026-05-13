# -*- coding: utf-8 -*-
"""
api/utils/session_manager.py
Session 管理 + 自动清理定时任务
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# 配置
TEMP_DIR = Path(__file__).parent.parent.parent / "temp"
TEMP_DIR.mkdir(exist_ok=True)
MAX_AGE_HOURS = 1  # 临时文件有效期（小时）

# Session 内存缓存（轻量级）
_sessions: Dict[str, Dict] = {}


class SessionManager:
    """
    管理上传文件的 Session，支持 UUID 标识、自动清理
    
    Session 状态机：
    - pending: 创建中（文件上传中）
    - ready: 就绪（文件已转换完成）
    - analyzing: 分析中
    - done: 分析完成
    - expired: 已过期（等待清理）
    """

    # ── Session 创建 ───────────────────────────────────────

    @staticmethod
    def create_session(file_name: str = "") -> str:
        """
        创建新 session，返回 UUID
        
        Args:
            file_name: 原始文件名（可选）
            
        Returns:
            session_id: UUID 字符串
        """
        session_id = str(uuid.uuid4())
        
        _sessions[session_id] = {
            "session_id": session_id,
            "file_name": file_name,
            "created_at": datetime.now(),
            "expires_at": datetime.now() + timedelta(hours=MAX_AGE_HOURS),
            "status": "pending",
            "file_size_mb": None,
            "total_rows": None,
            "total_columns": None,
        }
        
        logger.info(f"Session created: {session_id} (file: {file_name})")
        return session_id

    @staticmethod
    def get_session(session_id: str) -> Optional[Dict]:
        """获取 session 信息"""
        return _sessions.get(session_id)

    @staticmethod
    def update_session(session_id: str, **kwargs) -> bool:
        """更新 session 信息"""
        if session_id in _sessions:
            _sessions[session_id].update(kwargs)
            return True
        return False

    # ── 文件路径管理 ───────────────────────────────────────

    @staticmethod
    def get_file_path(session_id: str, ext: str = "parquet") -> Path:
        """
        获取 session 对应的文件路径
        
        Args:
            session_id: 会话 ID
            ext: 文件扩展名 (csv/parquet)
        """
        return TEMP_DIR / f"upload_{session_id}.{ext}"

    @staticmethod
    def get_csv_path(session_id: str) -> Path:
        """获取原始 CSV 文件路径"""
        return SessionManager.get_file_path(session_id, "csv")

    @staticmethod
    def get_parquet_path(session_id: str) -> Path:
        """获取 Parquet 文件路径"""
        return SessionManager.get_file_path(session_id, "parquet")

    # ── Session 状态检查 ───────────────────────────────────

    @staticmethod
    def is_valid(session_id: str) -> bool:
        """检查 session 是否有效（文件存在且未过期）"""
        session = _sessions.get(session_id)
        if not session:
            return False
        
        # 检查过期时间
        if datetime.now() > session["expires_at"]:
            return False
        
        # 检查文件是否存在
        parquet_path = SessionManager.get_parquet_path(session_id)
        return parquet_path.exists()

    @staticmethod
    def is_ready(session_id: str) -> bool:
        """检查 session 是否就绪（Parquet 文件已转换完成）"""
        if not SessionManager.is_valid(session_id):
            return False
        session = _sessions.get(session_id)
        return session and session.get("status") == "ready"

    # ── 清理操作 ───────────────────────────────────────────

    @staticmethod
    def cleanup_session(session_id: str) -> Dict[str, bool]:
        """
        删除指定 session 的所有临时文件
        
        Returns:
            {"csv": True/False, "parquet": True/False} - 每个文件的删除状态
        """
        deleted = {"csv": False, "xlsx": False, "xls": False, "parquet": False}
        
        for ext in ["csv", "xlsx", "xls", "parquet"]:
            path = SessionManager.get_file_path(session_id, ext)
            if path.exists():
                try:
                    path.unlink()
                    deleted[ext] = True
                    logger.info(f"Deleted: {path}")
                except Exception as e:
                    logger.error(f"Failed to delete {path}: {e}")
        
        # 从内存缓存移除
        if session_id in _sessions:
            del _sessions[session_id]
        
        return deleted

    @staticmethod
    def cleanup_expired_files(max_age_hours: int = MAX_AGE_HOURS) -> int:
        """
        删除超过 max_age_hours 的临时文件
        
        Args:
            max_age_hours: 文件最大有效期（小时）
            
        Returns:
            删除的文件数量
        """
        count = 0
        now = datetime.now()
        expired_session_ids = []
        
        for f in TEMP_DIR.glob("upload_*.parquet"):
            # 检查文件修改时间
            file_age = timedelta(seconds=(now.timestamp() - f.stat().st_mtime))
            if file_age > timedelta(hours=max_age_hours):
                session_id = f.stem.replace("upload_", "")
                deleted = SessionManager.cleanup_session(session_id)
                if deleted["parquet"] or deleted["csv"]:
                    count += 1
                expired_session_ids.append(session_id)
        
        # 清理内存中过期的 sessions
        for session_id in expired_session_ids:
            if session_id in _sessions:
                del _sessions[session_id]
        
        logger.info(f"Cleaned up {count} expired session files")
        return count

    # ── Session 信息查询 ──────────────────────────────────

    @staticmethod
    def list_active_sessions() -> List[Dict]:
        """列出所有活跃（非过期）的 session"""
        active = []
        now = datetime.now()
        
        for session_id, session in _sessions.items():
            if now <= session["expires_at"]:
                # 检查文件是否存在
                parquet_path = SessionManager.get_parquet_path(session_id)
                if parquet_path.exists():
                    session["file_size_mb"] = round(parquet_path.stat().st_size / 1024 / 1024, 2)
                    active.append(session)
        
        return sorted(active, key=lambda x: x["created_at"], reverse=True)

    @staticmethod
    def get_temp_dir_size() -> float:
        """获取 temp 目录总大小 (MB)"""
        total = 0
        for f in TEMP_DIR.glob("upload_*"):
            total += f.stat().st_size
        return round(total / 1024 / 1024, 2)

    @staticmethod
    def get_temp_files_count() -> int:
        """获取临时文件数量"""
        return len(list(TEMP_DIR.glob("upload_*")))


# ── 定时清理任务 ─────────────────────────────────────────────

def setup_cleanup_scheduler():
    """
    设置定时清理任务（每小时执行一次）
    
    使用方法：
        if __name__ == "__main__":
            scheduler = setup_cleanup_scheduler()
            # ... 运行 FastAPI ...
            scheduler.shutdown()
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            func=SessionManager.cleanup_expired_files,
            trigger="interval",
            hours=1,
            id="cleanup_temp_files",
            name="清理过期临时文件",
            replace_existing=True,
        )
        scheduler.start()
        logger.info("Cleanup scheduler started (runs every hour)")
        return scheduler
    except ImportError:
        logger.warning("APScheduler not installed, auto-cleanup disabled")
        return None


# ── 便捷函数 ─────────────────────────────────────────────────

def destroy_session(session_id: str) -> bool:
    """
    销毁 session（前端调用的便捷函数）
    
    Args:
        session_id: 会话 ID
        
    Returns:
        是否成功
    """
    return SessionManager.cleanup_session(session_id).get("parquet", False) or \
           SessionManager.cleanup_session(session_id).get("csv", False)
