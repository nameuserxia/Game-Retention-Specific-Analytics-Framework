# -*- coding: utf-8 -*-
"""
run_analysis.py
通用游戏留存分析主入口

使用方式：
    python run_analysis.py --config config/example_game_config.yaml --data example_events.csv
    python run_analysis.py --config config/game_b_template_config.yaml --data your_events.csv --output output/

功能：
    1. 加载配置文件（字段映射、日期格式、分析口径）
    2. 执行 sanity_check（校验先行）
    3. 计算 D+N 留存率（全体 + 按国家分群 + 按渠道分群）
    4. 识别流失用户，提取最后5步行为路径
    5. 输出报告 Markdown 文件
"""

from __future__ import annotations

# Windows 控制台 UTF-8 修复
import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# 确保 core 包可以被导入
sys.path.insert(0, str(Path(__file__).parent))

from core.analytics import (
    FieldConfig,
    SanityCheckError,
    build_event_sequences,
    calculate_retention,
    get_churn_users,
    get_last_n_events,
    get_top_paths,
    sanity_check,
)
from core.config_loader import (
    apply_field_mapping,
    load_config,
    parse_dates_from_config,
)


# ============================================================
# 主分析流程
# ============================================================

def run(
    config_path: str,
    data_path: str,
    output_dir: str = 'output',
    skip_sanity_check: bool = False,
) -> None:
    """
    完整的留存分析主流程。
    
    Parameters
    ----------
    config_path : str
        游戏配置文件路径（YAML）
    data_path : str
        日志数据文件路径（CSV）
    output_dir : str
        报告输出目录
    skip_sanity_check : bool
        是否跳过数据质量校验（不推荐，仅用于探索性分析）
    """
    print(f"{'='*60}")
    print(f"  游戏留存分析框架 v1.0")
    print(f"  配置文件: {config_path}")
    print(f"  数据文件: {data_path}")
    print(f"  输出目录: {output_dir}")
    print(f"{'='*60}\n")

    # ── Step 1: 加载配置 ──
    print("[1/6] 加载配置文件...")
    try:
        cfg, game_config = load_config(config_path)
    except Exception as e:
        print(f"[FAIL] 配置文件加载失败：{e}")
        sys.exit(1)

    game_name = game_config.get('game', {}).get('name', '未知游戏')
    analysis_cfg = game_config.get('analysis', {})
    reg_start = pd.Timestamp(analysis_cfg.get('reg_start', '2026-03-08'))
    reg_end   = pd.Timestamp(analysis_cfg.get('reg_end',   '2026-04-08'))
    retention_days = int(analysis_cfg.get('retention_days', 1))
    print(f"  游戏: {game_name}")
    print(f"  注册窗口: {reg_start.date()} ~ {reg_end.date()}")
    print(f"  留存定义: D+{retention_days}")

    # ── Step 2: 加载 & 预处理数据 ──
    print("\n[2/6] 加载数据...")
    try:
        df_raw = pd.read_csv(data_path, encoding='utf-8-sig', low_memory=False)
        print(f"  原始数据: {len(df_raw):,} 行 × {len(df_raw.columns)} 列")
    except Exception as e:
        print(f"[FAIL] 数据加载失败：{e}")
        sys.exit(1)

    # 字段映射 + 日期解析
    try:
        df = apply_field_mapping(df_raw, game_config['field_mapping'])
        df = parse_dates_from_config(df, game_config, cfg)
    except Exception as e:
        print(f"[FAIL] 数据预处理失败：{e}")
        sys.exit(1)

    # ── Step 3: 校验先行 ──
    print("\n[3/6] 执行数据质量校验（sanity_check）...")
    if not skip_sanity_check:
        try:
            report = sanity_check(
                df, cfg,
                min_sample_size=analysis_cfg.get('min_sample_size', 30),
                raise_on_failure=True,
            )
        except SanityCheckError as e:
            print(f"\n[FAIL] 数据质量校验失败，分析中止：\n  {e}")
            print("\n建议：")
            print("  1. 检查日期格式是否与 date_formats 配置一致")
            print("  2. 确认 field_mapping 中的字段名与 CSV 列名一致")
            print("  3. 若要强制跳过校验，使用 --skip-sanity-check（不推荐）")
            sys.exit(1)
    else:
        print("  [SKIP] 校验已跳过（--skip-sanity-check）")

    # ── Step 4: 流失 & 留存判定 ──
    print(f"\n[4/6] 计算 D+{retention_days} 留存率...")

    # 全体留存率
    retention_result = calculate_retention(
        df, cfg, reg_start, reg_end,
        retention_days=retention_days,
    )
    total_row = retention_result.iloc[0]
    n_total = int(total_row['n_total'])
    n_retained = int(total_row['n_retained'])
    n_churn = n_total - n_retained
    retention_rate = total_row['retention_rate']
    print(f"  总用户: {n_total} | 留存: {n_retained} ({retention_rate}%) | 流失: {n_churn}")

    # 按国家分群
    country_retention = calculate_retention(
        df, cfg, reg_start, reg_end,
        retention_days=retention_days,
        segment_col=cfg.country,
    )

    # 按渠道分群
    channel_retention = calculate_retention(
        df, cfg, reg_start, reg_end,
        retention_days=retention_days,
        segment_col=cfg.channel,
    )

    # 识别流失用户
    churn_users, retained_users = get_churn_users(
        df, cfg, reg_start, reg_end, retention_days
    )

    # ── Step 5: 行为路径分析 ──
    print(f"\n[5/6] 分析流失用户行为路径（最后5步）...")
    df_churn = df[df[cfg.user_id].isin(set(churn_users))].copy()

    # 最后5步序列
    sequences = build_event_sequences(df_churn, cfg, user_ids=churn_users, n=5)
    top_paths = get_top_paths(sequences, n_total=n_churn, top_n=5)
    print(f"  流失用户: {n_churn} 人，不同行为路径: {len(set(sequences.values()))} 种")
    for p in top_paths:
        print(f"  Top{p['rank']}: [{p['count']}人, {p['pct']}%] {p['path']}")

    # ── Step 6: 生成报告 ──
    print(f"\n[6/6] 生成报告...")
    os.makedirs(output_dir, exist_ok=True)

    report_path = os.path.join(output_dir, f"retention_report_{datetime.now().strftime('%Y%m%d_%H%M')}.md")
    _generate_report(
        path=report_path,
        game_name=game_name,
        reg_start=reg_start,
        reg_end=reg_end,
        retention_days=retention_days,
        n_total=n_total,
        n_retained=n_retained,
        n_churn=n_churn,
        retention_rate=retention_rate,
        country_retention=country_retention,
        channel_retention=channel_retention,
        top_paths=top_paths,
    )
    print(f"\n[DONE] 报告已生成：{report_path}")
    print(f"{'='*60}")


# ============================================================
# 报告生成
# ============================================================

def _generate_report(
    path: str,
    game_name: str,
    reg_start: pd.Timestamp,
    reg_end: pd.Timestamp,
    retention_days: int,
    n_total: int,
    n_retained: int,
    n_churn: int,
    retention_rate: float,
    country_retention: pd.DataFrame,
    channel_retention: pd.DataFrame,
    top_paths: List[Dict],
) -> None:
    """生成 Markdown 格式的留存分析报告"""

    # 国家分群表
    country_table = "| 国家/地区 | 总用户数 | 留存用户数 | 留存率 | 备注 |\n|---------|---------|-----------|------|------|\n"
    for _, row in country_retention.head(10).iterrows():
        country_table += f"| {row['segment']} | {row['n_total']} | {row['n_retained']} | {row['retention_rate']}% | {row.get('note', '')} |\n"

    # 渠道分群表
    channel_table = "| 渠道 | 总用户数 | 留存用户数 | 留存率 | 备注 |\n|------|---------|-----------|------|------|\n"
    for _, row in channel_retention.head(10).iterrows():
        channel_table += f"| {row['segment']} | {row['n_total']} | {row['n_retained']} | {row['retention_rate']}% | {row.get('note', '')} |\n"

    # 行为路径表
    path_table = "| 排名 | 行为路径（最后5步，时间正序） | 用户数 | 占流失用户比例 |\n|------|------------------------------|--------|---------------|\n"
    for p in top_paths:
        path_table += f"| {p['rank']} | {p['path']} | {p['count']} | {p['pct']}% |\n"

    content = f"""# {game_name} 用户留存分析报告

> 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}

---

## 一、数据口径

| 维度 | 说明 |
|------|------|
| 注册窗口 | {reg_start.date()} ~ {reg_end.date()} |
| 留存定义 | D+{retention_days}（注册后第 {retention_days} 天有活跃记录） |
| 总用户数 | **{n_total}** |
| 留存用户数 | **{n_retained}**（{retention_rate}%） |
| 流失用户数 | **{n_churn}**（{100 - retention_rate:.2f}%） |

---

## 二、D+{retention_days} 留存率 · 国家分群

{country_table}

> 注：样本量 < 30 的分组标注"谨慎解读"

---

## 三、D+{retention_days} 留存率 · 渠道分群

{channel_table}

---

## 四、流失用户典型行为路径（最后5步）

{path_table}

---

## 五、分析说明

- **流失定义**：注册后第 {retention_days} 天（D+{retention_days}）无任何事件记录。
- **行为路径**：取每个流失用户时间最新的 5 条事件，按时间正序排列。
- **生存者偏倚**：本报告中广告接触量统计均使用密度（广告事件占总事件比例），而非绝对数量，以控制游戏时长的影响。

---

*本报告由 game_retention_framework v1.0 自动生成*
"""

    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)


# ============================================================
# 命令行入口
# ============================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='游戏留存分析框架')
    parser.add_argument('--config', required=True, help='游戏配置文件路径（YAML）')
    parser.add_argument('--data',   required=True, help='日志数据文件路径（CSV）')
    parser.add_argument('--output', default='output', help='报告输出目录（默认 output/）')
    parser.add_argument('--skip-sanity-check', action='store_true',
                        help='跳过数据质量校验（不推荐）')
    args = parser.parse_args()

    run(
        config_path=args.config,
        data_path=args.data,
        output_dir=args.output,
        skip_sanity_check=args.skip_sanity_check,
    )
