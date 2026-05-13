# -*- coding: utf-8 -*-
"""
api/utils/file_parser.py
CSV → Parquet 转换 + 大文件引擎选择
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# 配置
LARGE_FILE_THRESHOLD = 500 * 1024 * 1024  # 500MB

# 尝试导入可选依赖
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    logger.warning("Polars not installed, using pandas only")

import pandas as pd


class FileParser:
    """
    统一文件解析器，自动选择引擎
    
    策略：
    - 小文件 (< 500MB): pandas 足够
    - 大文件 (>= 500MB): Polars 更高效
    """

    @staticmethod
    def is_large_file(file_path: Path) -> bool:
        """判断是否是大文件"""
        return file_path.stat().st_size >= LARGE_FILE_THRESHOLD

    @staticmethod
    def get_engine() -> str:
        """获取当前可用的引擎"""
        return "polars" if HAS_POLARS else "pandas"

    # ── 预览读取（只读 head） ────────────────────────────────

    @staticmethod
    def read_preview(
        file_path: Path,
        nrows: int = 100,
        use_engine: Optional[str] = None
    ) -> pd.DataFrame:
        """
        预览模式：读取前 n 行（用于 Schema Discovery）
        
        Args:
            file_path: 文件路径
            nrows: 读取行数
            use_engine: 强制使用指定引擎 (pandas/polars)
            
        Returns:
            DataFrame (pandas)
        """
        suffix = file_path.suffix.lower()
        
        if suffix == ".csv":
            return pd.read_csv(
                file_path,
                nrows=nrows,
                encoding="utf-8-sig",
                low_memory=False
            )
        elif suffix in (".xlsx", ".xls"):
            return pd.read_excel(
                file_path,
                nrows=nrows
            )
        elif suffix == ".parquet":
            return pd.read_parquet(
                file_path,
                **({"filters": None} if HAS_POLARS else {}),
            ).head(nrows)
        else:
            raise ValueError(f"Unsupported file format: {suffix}")

    @staticmethod
    def read_preview_pandas(file_path: Path, nrows: int = 100) -> pd.DataFrame:
        """使用 pandas 读取预览，兼容 CSV/Excel/Parquet。"""
        return FileParser.read_preview(file_path, nrows=nrows, use_engine="pandas")

    # ── CSV → Parquet 转换 ────────────────────────────────

    @staticmethod
    def convert_to_parquet(
        csv_path: Path,
        session_id: str,
        delete_original: bool = True
    ) -> Tuple[Path, str]:
        """
        将 CSV 转换为 Parquet
        
        Args:
            csv_path: CSV 文件路径
            session_id: 会话 ID
            delete_original: 是否删除原始 CSV
            
        Returns:
            (parquet_path, engine_used)
        """
        from api.utils.session_manager import SessionManager

        suffix = csv_path.suffix.lower()
        parquet_path = SessionManager.get_parquet_path(session_id)
        if suffix == ".parquet":
            if csv_path.resolve() != parquet_path.resolve():
                shutil.copyfile(csv_path, parquet_path)
            if delete_original and csv_path.exists() and csv_path.resolve() != parquet_path.resolve():
                csv_path.unlink()
            return parquet_path, "parquet"

        file_size = csv_path.stat().st_size
        is_large = file_size >= LARGE_FILE_THRESHOLD
        
        if suffix == ".csv" and is_large and HAS_POLARS:
            # 大文件：Polars 更高效
            logger.info(f"Converting large file ({file_size/1024/1024:.1f}MB) using Polars")
            df = pl.read_csv(csv_path)
            df.write_parquet(parquet_path)
            engine = "polars"
        elif suffix == ".csv":
            # 中小文件：pandas 足够
            logger.info(f"Converting file ({file_size/1024/1024:.1f}MB) using pandas")
            df = pd.read_csv(csv_path, encoding="utf-8-sig", low_memory=False)
            df.to_parquet(parquet_path, index=False, engine="pyarrow")
            engine = "pandas"
        elif suffix in (".xlsx", ".xls"):
            logger.info(f"Converting Excel file ({file_size/1024/1024:.1f}MB) using pandas")
            df = pd.read_excel(csv_path)
            df.to_parquet(parquet_path, index=False, engine="pyarrow")
            engine = "pandas"
        else:
            raise ValueError(f"Unsupported file format: {suffix}")

        # 转换成功后删除原始 CSV
        if delete_original and csv_path.exists() and csv_path.resolve() != parquet_path.resolve():
            csv_path.unlink()
            logger.info(f"Original CSV deleted: {csv_path}")

        return parquet_path, engine

    # ── 全量读取 ──────────────────────────────────────────

    @staticmethod
    def read_full(session_id: str, file_path: Optional[Path] = None) -> pd.DataFrame:
        """
        全量读取：用于完整分析
        
        Args:
            session_id: 会话 ID
            file_path: 文件路径（如果已知）
            
        Returns:
            DataFrame (pandas)
        """
        if file_path is None:
            from api.utils.session_manager import SessionManager
            file_path = SessionManager.get_parquet_path(session_id)
        
        file_size = file_path.stat().st_size if file_path.exists() else 0
        is_large = file_size >= LARGE_FILE_THRESHOLD
        
        if is_large and HAS_POLARS:
            # 大文件用 Polars
            logger.info(f"Reading large file ({file_size/1024/1024:.1f}MB) using Polars")
            return pl.read_parquet(file_path).to_pandas()
        else:
            # 中小文件用 pandas
            logger.info(f"Reading file ({file_size/1024/1024:.1f}MB) using pandas")
            return pd.read_parquet(file_path)

    # ── 统计信息 ──────────────────────────────────────────

    @staticmethod
    def get_file_info(file_path: Path) -> dict:
        """
        获取文件基本信息
        
        Returns:
            dict: {rows, columns, size_mb, engine, is_large}
        """
        suffix = file_path.suffix.lower()
        
        if suffix == ".csv":
            # 快速统计行数（不加载全部数据）
            with open(file_path, encoding="utf-8-sig") as f:
                row_count = sum(1 for _ in f) - 1  # 减去 header
            
            df_preview = pd.read_csv(file_path, nrows=10, encoding="utf-8-sig")
            col_count = len(df_preview.columns)
            engine = "polars" if HAS_POLARS and file_path.stat().st_size >= LARGE_FILE_THRESHOLD else "pandas"
            
        elif suffix == ".parquet":
            if HAS_POLARS:
                pf = pl.scan_parquet(file_path)
                row_count = pf.select(pl.len()).collect().item()
                col_count = len(pf.schema)
            else:
                df = pd.read_parquet(file_path)
                row_count = len(df)
                col_count = len(df.columns)
            engine = "polars"
        elif suffix in (".xlsx", ".xls"):
            df = pd.read_excel(file_path)
            row_count = len(df)
            col_count = len(df.columns)
            engine = "pandas"
        else:
            raise ValueError(f"Unsupported format: {suffix}")
        
        return {
            "rows": row_count,
            "columns": col_count,
            "size_mb": round(file_path.stat().st_size / 1024 / 1024, 2),
            "engine": engine,
            "is_large": file_path.stat().st_size >= LARGE_FILE_THRESHOLD
        }
