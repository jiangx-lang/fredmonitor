#!/usr/bin/env python3
"""
事实表渲染脚本

为每个FRED序列生成FRED风格的事实表Markdown文件。
"""

import os
import json
import pathlib
import logging
from typing import Dict, Any, Optional

import pandas as pd
from jinja2 import Template

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 基础路径
BASE = os.getenv("BASE_DIR", os.getcwd())


def load_template() -> Template:
    """加载Jinja2模板"""
    template_file = os.path.join(BASE, "templates", "fact_sheet.md.j2")
    try:
        with open(template_file, "r", encoding="utf-8") as f:
            return Template(f.read())
    except Exception as e:
        logger.error(f"加载模板失败: {e}")
        raise


def compute_trend_6m(feats: pd.DataFrame) -> Optional[float]:
    """计算6个月趋势斜率"""
    try:
        # 获取最近6个非NA数据点
        tail = feats.dropna(subset=["value"]).tail(6)
        if len(tail) < 2:
            return None
        
        # 计算线性趋势斜率
        x = pd.RangeIndex(len(tail))
        y = tail["value"].reset_index(drop=True)
        
        if x.var() == 0:
            return None
        
        slope = y.cov(x) / x.var()
        return float(slope)
    except Exception as e:
        logger.debug(f"计算6个月趋势失败: {e}")
        return None


def get_attachments_list(series_dir: pathlib.Path) -> list:
    """获取附件列表"""
    attachments_dir = series_dir / "notes" / "attachments"
    if not attachments_dir.exists():
        return []
    
    attachments = []
    for file_path in attachments_dir.iterdir():
        if file_path.is_file():
            attachments.append(file_path.name)
    
    return sorted(attachments)


def render_fact_sheet(series_id: str, template: Template) -> None:
    """渲染单个序列的事实表"""
    series_dir = pathlib.Path(BASE) / "data" / "fred" / "series" / series_id
    
    # 检查必要文件
    meta_file = series_dir / "meta.json"
    features_file = series_dir / "features.parquet"
    
    if not meta_file.exists() or not features_file.exists():
        logger.warning(f"跳过序列 {series_id}: 缺少必要文件")
        return
    
    try:
        # 加载元数据
        with open(meta_file, "r", encoding="utf-8") as f:
            meta = json.load(f)
        
        # 加载特征数据
        feats = pd.read_parquet(features_file)
        feats = feats.dropna(subset=["value"]).sort_values("date")
        
        if feats.empty:
            logger.warning(f"跳过序列 {series_id}: 无有效数据")
            return
        
        # 获取最新数据
        latest = feats.iloc[-1]
        
        # 计算特征值
        yoy = latest.get("yoy", None)
        mom = latest.get("mom", None)
        
        # 计算6个月趋势
        trend_6m = compute_trend_6m(feats)
        
        # 读取自定义笔记
        custom_notes_file = series_dir / "notes" / "custom_notes.md"
        custom_notes = ""
        if custom_notes_file.exists():
            custom_notes = custom_notes_file.read_text(encoding="utf-8").strip()
        
        # 获取附件列表
        attachments = get_attachments_list(series_dir)
        
        # 渲染模板
        md_content = template.render(
            title=meta.get("title", series_id),
            series_id=series_id,
            alias=meta.get("alias", series_id),
            latest_value=f"{latest['value']:.4g}",
            latest_period=str(pd.to_datetime(latest['date']).date()),
            last_updated=meta.get("last_updated", "N/A"),
            next_release="N/A",  # FRED通常不提供下次发布时间
            units=meta.get("units", ""),
            frequency=meta.get("frequency", ""),
            seasonal_adjustment=meta.get("seasonal_adjustment", ""),
            observation_start=meta.get("observation_start", ""),
            observation_end=meta.get("observation_end", ""),
            yoy=(None if pd.isna(yoy) else f"{yoy:.3g}%") if 'yoy' in feats.columns else None,
            mom=(None if pd.isna(mom) else f"{mom:.3g}%") if 'mom' in feats.columns else None,
            trend_6m=None if trend_6m is None else f"{trend_6m:.4g}",
            official_notes=meta.get("notes", "").strip(),
            source_name="U.S. Bureau of Labor Statistics",  # 可以根据序列调整
            release_name=meta.get("title", ""),
            custom_notes_block=custom_notes or "_(no manual notes)_",
            attachments_list="\n".join([f"- {a}" for a in attachments]) or "_(no attachments)_",
        )
        
        # 保存事实表
        fact_sheet_file = series_dir / "fact_sheet.md"
        fact_sheet_file.write_text(md_content, encoding="utf-8")
        
        logger.info(f"✓ {series_id} 事实表已更新")
        
    except Exception as e:
        logger.error(f"渲染事实表失败 {series_id}: {e}")


def main():
    """主函数"""
    logger.info("开始渲染事实表")
    
    # 加载模板
    template = load_template()
    
    # 获取所有序列目录
    series_root = pathlib.Path(BASE) / "data" / "fred" / "series"
    if not series_root.exists():
        logger.error(f"序列目录不存在: {series_root}")
        return
    
    # 处理每个序列
    series_dirs = [d for d in series_root.iterdir() if d.is_dir()]
    logger.info(f"找到 {len(series_dirs)} 个序列目录")
    
    for series_dir in series_dirs:
        series_id = series_dir.name
        render_fact_sheet(series_id, template)
    
    logger.info("事实表渲染完成")


if __name__ == "__main__":
    main()
