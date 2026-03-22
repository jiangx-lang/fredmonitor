#!/usr/bin/env python3
"""
基于FRED HTTP API的数据同步脚本

直接使用FRED官方REST API，避免第三方库的不稳定性。
"""

import os
import json
import pathlib
import yaml
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.fred_http import (
    series_info, 
    series_observations, 
    get_next_release_date, 
    polite_sleep
)
from scripts.duckdb_io import upsert_parquet

# 设置日志
# 确保日志目录存在
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "fred")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "sync.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
# 确保从项目根目录加载环境变量
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "macrolab.env")
load_dotenv(env_path)

CATALOG_FILE = os.path.join(BASE, "config", "catalog_fred.yaml")
CRISIS_INDICATORS_FILE = os.path.join(BASE, "config", "crisis_indicators.yaml")
DERIVED_DEPS_FILE = pathlib.Path(BASE) / "config" / "derived_fred_deps.yaml"
SERIES_ROOT = pathlib.Path(BASE) / "data" / "fred" / "series"
COVERAGE_CSV = pathlib.Path(BASE) / "data" / "crisis_fred_sync_coverage.csv"

# 派生指标依赖：优先从 config/derived_fred_deps.yaml 读取（一级配置）；失败则用下方 fallback
_DERIVED_FRED_DEPS_FALLBACK = {
    "CP_MINUS_DTB3": ["CPN3M", "DTB3"],
    "SOFR20DMA_MINUS_DTB3": ["SOFR", "DTB3"],
    "CORPDEBT_GDP_PCT": ["NCBDBIQ027S", "GDP"],
    "RESERVES_ASSETS_PCT": ["TOTRESNS", "WALCL"],
    "RESERVES_DEPOSITS_PCT": ["TOTRESNS", "TOTALSA"],
    "UST30Y_UST2Y_RSI": ["DGS30", "DGS2"],
    "HY_OAS_MOMENTUM_RATIO": ["BAMLH0A0HYM2"],
    "SP500_DGS10_CORR60D": ["SP500", "DGS10"],
    "NET_LIQUIDITY": ["WALCL", "WTREGEN", "RRPONTSYD"],
    "VIX_TERM_STRUCTURE": ["VIXCLS", "VIX3M"],
    "HY_IG_RATIO": ["BAMLHYH0A0HYM2TRIV", "BAMLCC0A0CMTRIV"],
    "GLOBAL_LIQUIDITY_USD": ["WALCL", "DEXUSEU", "DEXJPUS"],
    "CREDIT_CARD_DELINQUENCY": ["DRCCLACBS"],
    "US_JPY_10Y_SPREAD": ["DGS10", "IRLTLT01JPM156N"],
    "MOVE_PROXY": ["VIXCLS", "DGS10"],
    "SOFR_MINUS_IORB": ["SOFR", "IORB"],
    "RRP_DRAIN_RATE": ["RRPONTSYD"],
    "T5YIE_REALTIME": ["DFII5", "T5YIE"],
}


def _load_derived_fred_deps() -> Dict[str, List[str]]:
    """从 config/derived_fred_deps.yaml 加载派生→FRED 依赖；缺失或异常时用 fallback。"""
    if DERIVED_DEPS_FILE.exists():
        try:
            with open(DERIVED_DEPS_FILE, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            raw = data.get("derived_indicators") or {}
            return {k: (v if isinstance(v, list) else list(v)) for k, v in raw.items()}
        except Exception as e:
            logger.warning(f"读取 derived_fred_deps.yaml 失败，使用 fallback: {e}")
    return _DERIVED_FRED_DEPS_FALLBACK.copy()


# 模块加载时解析一次；调用方使用 DERIVED_FRED_DEPS
DERIVED_FRED_DEPS = _load_derived_fred_deps()

NON_FRED_INDICATOR_IDS = {
    "HYG_LQD_RATIO", "DXY_CHANGE", "KRE_SPY_RATIO", "XLF_SPY_RATIO",
    "BTC_QQQ_RATIO", "CROSS_ASSET_CORR_STRESS",
    "JPY_VOL_20D", "JPY_VOL_1M", "US_JPY_10Y_1W_CHG", "NKY_SPX_CORR_20D", "JGB_ETF_8282",
}


def ensure_series_dir(series_id: str) -> pathlib.Path:
    """创建序列目录结构"""
    p = SERIES_ROOT / series_id
    (p / "notes" / "attachments").mkdir(parents=True, exist_ok=True)
    
    # 创建空的custom_notes.md文件（如果不存在）
    custom_notes_file = p / "notes" / "custom_notes.md"
    if not custom_notes_file.exists():
        custom_notes_file.write_text("", encoding="utf-8")
    
    return p


def safe_write_json(path: pathlib.Path, obj: dict):
    """原子写入JSON文件"""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2), 
        encoding="utf-8"
    )
    tmp.replace(path)


def fetch_metadata(series_id: str) -> Dict[str, Any]:
    """
    获取序列元数据，包括下一次发布日期
    
    Args:
        series_id: FRED序列ID
        
    Returns:
        包含元数据的字典
    """
    logger.info(f"获取序列元数据: {series_id}")
    
    # 获取基本元数据
    meta_response = series_info(series_id)
    series_list = meta_response.get("seriess", [])
    
    if not series_list:
        raise ValueError(f"未找到序列 {series_id}")
    
    meta = series_list[0]
    
    # 添加同步时间戳
    meta["sync_timestamp"] = datetime.now().isoformat()
    
    return meta


def fetch_observations(series_id: str, force_full: bool = False) -> pd.DataFrame:
    """
    获取序列观测数据（支持增量更新；force_full 时强制全量拉取，用于报告前刷新）。
    
    Args:
        series_id: FRED序列ID
        force_full: 为 True 时忽略本地缓存，始终从 FRED 全量拉取（避免修订值/滞后未更新）
        
    Returns:
        包含观测数据的DataFrame，列仅含 date, value
    """
    logger.info(f"获取序列观测数据: {series_id}" + (" [强制全量]" if force_full else ""))
    
    folder = SERIES_ROOT / series_id
    local_file = folder / "raw.csv"
    
    if not force_full and local_file.exists():
        try:
            # 读取本地数据（可能为 date,value 或 date,series_id,value，统一为 date + value）
            local_df = pd.read_csv(local_file, parse_dates=["date"])
            if "value" not in local_df.columns and len(local_df.columns) >= 2:
                local_df = local_df.rename(columns={local_df.columns[1]: "value"})
            if not local_df.empty:
                # 获取最新日期
                latest_date = local_df["date"].max()
                logger.info(f"本地数据最新日期: {latest_date}")
                
                # 检查是否需要更新（超过1天）
                days_since_update = (datetime.now().date() - latest_date.date()).days
                if days_since_update <= 1:
                    logger.info(f"✓ {series_id}: 数据新鲜，跳过下载")
                    return local_df
                else:
                    logger.info(f"数据过期 {days_since_update} 天，尝试增量更新")
                    # 尝试增量更新（只获取最新数据）
                    try:
                        # 获取从最新日期开始的数据（必须用 observation_start，FRED API 不认 start_date）
                        start_date = latest_date.strftime("%Y-%m-%d")
                        obs_response = series_observations(series_id, observation_start=start_date)
                        new_observations = obs_response.get("observations", [])
                        
                        if new_observations:
                            # 处理新数据（先不 dropna，避免 FRED 对同一天修订值被丢掉）
                            new_df = pd.DataFrame(new_observations)
                            new_df["value"] = pd.to_numeric(new_df["value"], errors="coerce")
                            new_df["date"] = pd.to_datetime(new_df["date"]).dt.date.astype("datetime64[ns]")
                            new_df = new_df[["date", "value"]]
                            # 合并时保留「后出现的」行，使 FRED 对同一日期的修订（如 Q4 从缺失改为 2.94）覆盖本地
                            local_flat = local_df[["date", "value"]] if "value" in local_df.columns else local_df[["date", local_df.columns[1]]].rename(columns={local_df.columns[1]: "value"})
                            combined_df = pd.concat([local_flat, new_df], ignore_index=True).drop_duplicates(subset=["date"], keep="last").sort_values("date")
                            # 若 FRED 返回同日期但值为 NaN（尚未修订），用本地值填回，再丢弃仍无值的行
                            local_by_date = local_flat.set_index("date")["value"]
                            combined_df["value"] = combined_df["value"].fillna(combined_df["date"].map(lambda d: local_by_date.get(pd.Timestamp(d).date() if hasattr(d, "date") else d, float("nan"))))
                            combined_df = combined_df.dropna(subset=["value"])
                            logger.info(f"✓ {series_id}: 增量更新完成，新增 {len(new_df)} 条数据")
                            return combined_df
                        else:
                            logger.info(f"✓ {series_id}: 无新数据，使用本地数据")
                            return local_df
                    except Exception as e:
                        logger.warning(f"增量更新失败: {e}，使用全量下载")
        except Exception as e:
            logger.warning(f"读取本地数据失败: {e}")
    
    # 获取全历史数据
    obs_response = series_observations(series_id)
    observations = obs_response.get("observations", [])
    
    if not observations:
        logger.warning(f"序列 {series_id} 无观测数据")
        return pd.DataFrame(columns=["date", "value"])
    
    # 转换为DataFrame，统一为 date + value 两列
    df = pd.DataFrame(observations)
    if "value" not in df.columns and len(df.columns) >= 2:
        df = df.rename(columns={df.columns[1]: "value"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype("datetime64[ns]")
    df = df[["date", "value"]].dropna(subset=["value"])
    logger.info(f"序列 {series_id} 获取到 {len(df)} 条有效数据")
    return df


def calculate_features(df: pd.DataFrame, calc_config: Optional[Dict[str, Any]]) -> pd.DataFrame:
    """
    计算衍生特征
    
    Args:
        df: 原始数据DataFrame
        calc_config: 计算配置
        
    Returns:
        包含特征的DataFrame
    """
    if not calc_config:
        return df
    
    out = df.sort_values("date").copy()
    
    for name, rule in calc_config.items():
        try:
            if rule.get("op") == "pct_change":
                shift = int(rule.get("shift", 1))
                scale = float(rule.get("scale", 1.0))
                
                # 计算百分比变化
                out[name] = out["value"].pct_change(shift) * scale
                
                logger.debug(f"计算特征 {name}: shift={shift}, scale={scale}")
            elif rule.get("op") == "none" or rule.get("op") == "level":
                # 不做变换，直接使用原始值
                out[name] = out["value"]
                logger.debug(f"计算特征 {name}: 使用原始值")
            elif rule.get("op") == "divide":
                # 除法操作：除以另一个序列
                by_series = rule.get("by")
                scale = float(rule.get("scale", 1.0))
                
                if by_series:
                    # 需要获取另一个序列的数据
                    try:
                        # 这里需要实现获取其他序列数据的逻辑
                        # 暂时使用简化版本
                        logger.warning(f"除法操作 {name} 需要实现跨序列计算")
                        out[name] = out["value"] * scale  # 临时处理
                    except Exception as e:
                        logger.error(f"除法计算失败 {name}: {e}")
                        out[name] = out["value"] * scale  # 临时处理
                else:
                    logger.warning(f"除法操作 {name} 缺少by参数")
                    out[name] = out["value"] * scale  # 临时处理
                
                logger.debug(f"计算特征 {name}: divide by {by_series}, scale={scale}")
            else:
                logger.warning(f"不支持的特征计算操作: {rule.get('op')}")
                
        except Exception as e:
            logger.error(f"计算特征失败 {name}: {e}")
    
    return out


def sync_series(series_config: Dict[str, Any], force_full: bool = False) -> None:
    """
    同步单个序列
    
    Args:
        series_config: 序列配置
        force_full: 为 True 时强制全量拉取观测数据（不读本地、不增量），用于报告前刷新
    """
    series_id = series_config["id"]
    alias = series_config.get("alias", series_id)
    calc_config = series_config.get("calc")
    freshness_days = series_config.get("freshness_days", 45)
    
    logger.info(f"开始同步序列: {series_id} ({alias})")
    
    try:
        # 创建目录
        folder = ensure_series_dir(series_id)
        
        # 1) 获取基本元数据并立即落盘（避免因下次发布日期失败丢失）
        metadata = fetch_metadata(series_id)
        metadata["alias"] = alias
        metadata["freshness_days"] = freshness_days
        metadata["next_release"] = "N/A"  # 默认值
        
        # 原子写入基本元数据
        safe_write_json(folder / "meta.json", metadata)
        
        # 2) 尝试获取下次发布日期（不阻塞主流程）
        try:
            next_release = get_next_release_date(series_id)
            if next_release != "N/A":
                metadata["next_release"] = next_release
                safe_write_json(folder / "meta.json", metadata)
                logger.debug(f"序列 {series_id} 下次发布日期: {next_release}")
        except Exception as e:
            logger.debug(f"序列 {series_id} 下次发布日期查询失败: {e}")
        
        # 3) 获取观测数据（报告前模式强制全量，确保滞后/修订值全部更新）
        df = fetch_observations(series_id, force_full=force_full)
        
        if df.empty:
            logger.warning(f"序列 {series_id} 无有效数据，创建空文件")
            # 创建空的CSV和Parquet文件
            empty_df = pd.DataFrame(columns=["date", "value"])
            empty_df.to_csv(folder / "raw.csv", index=False, encoding="utf-8")
            empty_df.to_parquet(folder / "features.parquet", index=False)
            logger.info(f"✓ {series_id}: 0 条数据（空序列）")
            return
        
        # 保存原始数据
        raw_file = folder / "raw.csv"
        df.to_csv(raw_file, index=False, encoding="utf-8")
        
        # 计算特征
        features_df = calculate_features(df, calc_config)
        
        # 保存特征数据
        features_file = folder / "features.parquet"
        features_df.to_parquet(features_file, index=False)
        
        # 导入DuckDB
        try:
            upsert_parquet(series_id, str(features_file))
            logger.debug(f"✓ {series_id} 已导入DuckDB")
        except Exception as e:
            logger.warning(f"✗ {series_id} DuckDB导入失败: {e}")
        
        # 礼貌性延迟
        polite_sleep()
        
        logger.info(f"✓ {series_id}: {len(df)} 条数据, 最新日期: {df['date'].max().date()}")
        
    except Exception as e:
        logger.error(f"✗ 同步序列失败 {series_id}: {e}")


def check_series_freshness(series_id: str, freshness_days: int = 7) -> bool:
    """
    检查序列数据是否新鲜；返回 True 表示「需要更新」，会入队并实际发请求。

    逻辑：基于本地 raw.csv 最后一行日期计算 days_old；若 days_old > freshness_days
    则返回 True（需要更新）。因此长期滞后的序列不会被错误跳过：越 stale 越会入队并拉取。
    """
    try:
        raw_file = SERIES_ROOT / series_id / "raw.csv"
        if not raw_file.exists():
            logger.info(f"📊 {series_id}: 数据文件不存在，需要下载")
            return True

        df = pd.read_csv(raw_file)
        if df.empty:
            logger.info(f"📊 {series_id}: 数据文件为空，需要下载")
            return True

        df["date"] = pd.to_datetime(df["date"])
        latest_date = df["date"].max()
        days_old = (datetime.now() - latest_date).days

        if days_old > freshness_days:
            logger.info(f"📊 {series_id}: 数据过期 {days_old} 天，需要更新")
            return True
        logger.debug(f"📊 {series_id}: 数据新鲜 ({days_old} 天前)")
        return False

    except Exception as e:
        logger.warning(f"📊 {series_id}: 检查新鲜度失败: {e}")
        return True


def get_crisis_required_fred_series() -> List[str]:
    """
    从 crisis_indicators.yaml 与派生依赖推导 crisis 监控实际需要的 FRED 原生序列，
    作为同步入队范围（方案 A：配置驱动，不再依赖狭窄硬编码列表）。
    """
    try:
        with open(CRISIS_INDICATORS_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"读取 crisis_indicators 失败，回退到扩大版 needed 列表: {e}")
        return _fallback_needed_series()
    indicators = data.get("indicators") or []
    derived_ids = set(DERIVED_FRED_DEPS.keys())
    ids_from_config = set()
    for ind in indicators:
        sid = (ind.get("id") or ind.get("series_id") or "").strip().upper()
        if sid and sid not in NON_FRED_INDICATOR_IDS:
            ids_from_config.add(sid)
    for _derived, deps in DERIVED_FRED_DEPS.items():
        for s in deps:
            ids_from_config.add(s.upper())
    fred_native = set()
    for sid in ids_from_config:
        if sid in derived_ids or sid in NON_FRED_INDICATOR_IDS:
            continue
        fred_native.add(sid)
    for _derived, deps in DERIVED_FRED_DEPS.items():
        for s in deps:
            fred_native.add(s.upper())
    return list(fred_native)


def _fallback_needed_series() -> List[str]:
    """过渡回退：crisis 配置不可用时使用扩大版 needed 列表（含原 get_daily_factors_needed + 漏同步 7 序列）。"""
    daily_factors = {
        "VIXCLS", "T10Y2Y", "BAMLH0A0HYM2", "DTWEXBGS", "BAMLHE00EHYIEY",
        "HOUST", "NFCI", "TEDRATE", "UMCSENT",
    }
    yoy_indicators = [
        "PAYEMS", "INDPRO", "GDP", "NEWORDER", "CSUSHPINSA",
        "TOTALSA", "TOTLL", "MANEMP", "WALCL", "DTWEXBGS",
        "PERMIT", "TOTRESNS",
    ]
    special = ["NCBDBIQ027S"]
    event_x_radar = ["DCOILBRENTEU", "T5YIE", "STLFSI4", "CPIENGSL", "GOLDAMGBD228NLBM", "GOLDPMGBD228NLBM", "DRTSCILM"]
    extra_crisis = ["DTB3", "DGS10", "T10Y3M", "SOFR", "MORTGAGE30US", "BAA10YM", "CPN3M"]
    needed = set()
    needed.update(daily_factors)
    needed.update(yoy_indicators)
    needed.update(special)
    needed.update(event_x_radar)
    needed.update(extra_crisis)
    return list(needed)


def write_sync_coverage_audit(catalog_ids: set, needed_set: set) -> None:
    """写入同步覆盖率审计 CSV：crisis 依赖 / 在 catalog / 入队情况 / status。"""
    import csv
    sync_queue_ids = catalog_ids & needed_set
    all_required = sorted(needed_set)
    rows = []
    for sid in all_required:
        in_cat = sid in catalog_ids
        in_queue = sid in sync_queue_ids
        if not in_cat:
            status = "MISSING_IN_CATALOG"
        elif not in_queue:
            status = "MISSING_IN_SYNC_QUEUE"
        else:
            status = "OK"
        rows.append({
            "series_id": sid,
            "required_by_crisis": "true",
            "present_in_catalog": str(in_cat).lower(),
            "present_in_sync_queue": str(in_queue).lower(),
            "status": status,
        })
    try:
        COVERAGE_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(COVERAGE_CSV, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["series_id", "required_by_crisis", "present_in_catalog", "present_in_sync_queue", "status"])
            w.writeheader()
            w.writerows(rows)
        logger.info(f"📋 同步覆盖率审计已写入: {COVERAGE_CSV} (入队 {len(sync_queue_ids)}/{len(needed_set)})")
    except Exception as e:
        logger.warning(f"写入覆盖率审计失败: {e}")


def main():
    """主函数 - 智能同步。支持 --before-report：生成报告前调用时强制拉取全部所需序列并重算派生，不因新鲜度跳过。"""
    import argparse
    parser = argparse.ArgumentParser(description="FRED 数据同步")
    parser.add_argument(
        "--before-report",
        action="store_true",
        help="报告前模式：不按新鲜度跳过，拉取所有 crisis 依赖的序列后再重算派生，避免报告遗漏数据",
    )
    args = parser.parse_args()
    before_report = getattr(args, "before_report", False)

    logger.info("🚀 开始智能FRED数据同步 (HTTP API)" + (" [报告前模式: 先全量拉取再计算]" if before_report else ""))
    
    # 确保目录存在
    SERIES_ROOT.mkdir(parents=True, exist_ok=True)
    
    # 加载配置
    try:
        with open(CATALOG_FILE, "r", encoding="utf-8") as f:
            catalog = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"加载配置文件失败: {e}")
        return
    
    series_list = catalog.get("series", [])
    if not series_list:
        logger.warning("未找到需要同步的序列")
        return
    
    # 同步范围 = catalog 中且被 crisis 实际依赖的 FRED 序列（配置驱动，见 docs/FRED_SYNC_SCOPE_FIX.md）
    needed_series = get_crisis_required_fred_series()
    needed_set = set(needed_series)
    logger.info(f"📊 Crisis 依赖的 FRED 序列: {len(needed_series)} 个")
    
    # 过滤：在 catalog 且被 crisis 依赖；报告前模式不按新鲜度跳过，其余按新鲜度决定是否拉取
    filtered_series = []
    for series_config in series_list:
        series_id = series_config.get("id")
        if series_id not in needed_set:
            continue
        if before_report:
            filtered_series.append(series_config)
        else:
            freshness_days = series_config.get("freshness_days", 7)
            if check_series_freshness(series_id, freshness_days):
                filtered_series.append(series_config)
            else:
                logger.info(f"✅ {series_id}: 数据已是最新，跳过")
    
    if not filtered_series:
        logger.info("🎉 所有数据都是最新的，无需同步" + ("（报告前模式仍会执行派生指标重算）" if before_report else ""))
    else:
        logger.info(f"📥 需要同步 {len(filtered_series)} 个序列" + ("（强制全量拉取）" if before_report else ""))
        # 并行同步序列（最多2个并发，避免DuckDB文件锁定）；报告前模式强制全量拉取
        success_count = 0
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_series = {
                executor.submit(sync_series, series_config, before_report): series_config
                for series_config in filtered_series
            }
            for future in as_completed(future_to_series):
                series_config = future_to_series[future]
                series_id = series_config.get("id", "unknown")
                try:
                    future.result()
                    success_count += 1
                    logger.info(f"✅ 序列 {series_id} 同步完成")
                except Exception as e:
                    logger.error(f"❌ 序列 {series_id} 同步失败: {e}")
        logger.info(f"📊 FRED数据同步完成: {success_count}/{len(filtered_series)} 成功")
    
    # 同步覆盖率审计（见 docs/FRED_SYNC_SCOPE_FIX.md）
    catalog_ids = {s.get("id") for s in series_list if s.get("id")}
    write_sync_coverage_audit(catalog_ids, needed_set)
    
    # 始终执行派生指标计算：先下载（上一步）再处理数据，避免报告使用陈旧的派生结果
    logger.info("🔧 开始计算合成指标（先下载后计算，保证报告数据一致）...")
    calculate_derived_series()
    
    logger.info("🎉 智能同步完成！")


def calculate_derived_series():
    """计算合成指标"""
    import pandas as pd
    import os
    
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "fred", "series")
    
    try:
        # 1. CP_MINUS_DTB3 = CPN3M - DTB3
        logger.info("计算 CP_MINUS_DTB3 (商业票据-3个月国债利差)...")
        cp_data = pd.read_csv(f"{data_dir}/CPN3M/raw.csv", index_col=0, parse_dates=True)
        tb_data = pd.read_csv(f"{data_dir}/DTB3/raw.csv", index_col=0, parse_dates=True)
        # CPN3M raw 可能含多列：优先 value，否则第一列
        cp_values = cp_data["value"] if "value" in cp_data.columns else cp_data.iloc[:, 0]
        cp_values = pd.to_numeric(cp_values, errors="coerce")
        tb_values = tb_data.iloc[:, 0]  # 取第一列数据
        
        cp_aligned = cp_values.reindex_like(tb_values).fillna(method="ffill")
        cp_minus_tb = cp_aligned - tb_values
        cp_minus_tb = cp_minus_tb.dropna()
        
        # 保存到data/series目录
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "series")
        os.makedirs(output_dir, exist_ok=True)
        # 确保列名为 'value'
        cp_minus_tb_df = cp_minus_tb.to_frame('value')
        cp_minus_tb_df.to_csv(f"{output_dir}/CP_MINUS_DTB3.csv")
        logger.info(f"✓ CP_MINUS_DTB3 计算完成，数据点: {len(cp_minus_tb)}")
        
    except Exception as e:
        logger.error(f"✗ CP_MINUS_DTB3 计算失败: {e}")
    
    try:
        # 2. SOFR20DMA_MINUS_DTB3 = SOFR(20日均值) - DTB3
        logger.info("计算 SOFR20DMA_MINUS_DTB3 (SOFR20日均值-3个月国债利差)...")
        sofr_data = pd.read_csv(f"{data_dir}/SOFR/raw.csv", index_col=0, parse_dates=True)
        tb_data = pd.read_csv(f"{data_dir}/DTB3/raw.csv", index_col=0, parse_dates=True)
        
        # 计算SOFR 20日均值
        sofr_values = sofr_data.iloc[:, 0]  # 取第一列数据
        sofr_20dma = sofr_values.rolling(window=20, min_periods=1).mean()
        
        # 对齐数据并计算利差
        tb_values = tb_data.iloc[:, 0]  # 取第一列数据
        tb_aligned = tb_values.reindex_like(sofr_20dma).fillna(method="ffill")
        sofr_minus_tb = sofr_20dma - tb_aligned
        sofr_minus_tb = sofr_minus_tb.dropna()
        
        # 确保列名为 'value'
        sofr_minus_tb_df = sofr_minus_tb.to_frame('value')
        sofr_minus_tb_df.to_csv(f"{output_dir}/SOFR20DMA_MINUS_DTB3.csv")
        logger.info(f"✓ SOFR20DMA_MINUS_DTB3 计算完成，数据点: {len(sofr_minus_tb)}")
        
    except Exception as e:
        logger.error(f"✗ SOFR20DMA_MINUS_DTB3 计算失败: {e}")
    
    try:
        # 3. CORPDEBT_GDP_PCT = NCBDBIQ027S / GDP * 100
        logger.info("计算 CORPDEBT_GDP_PCT (企业债/GDP)...")
        corp_debt_data = pd.read_csv(f"{data_dir}/NCBDBIQ027S/raw.csv", index_col=0, parse_dates=True)
        gdp_data = pd.read_csv(f"{data_dir}/GDP/raw.csv", index_col=0, parse_dates=True)
        
        # 单位转换：企业债从Millions转为Billions
        corp_debt_values = corp_debt_data.iloc[:, 0]  # 取第一列数据
        corp_debt_billions = corp_debt_values / 1000
        
        # 对齐数据并计算比率
        gdp_values = gdp_data.iloc[:, 0]  # 取第一列数据
        corp_debt_aligned = corp_debt_billions.reindex_like(gdp_values).fillna(method="ffill")
        corp_debt_gdp_ratio = (corp_debt_aligned / gdp_values) * 100
        corp_debt_gdp_ratio = corp_debt_gdp_ratio.dropna()
        
        # 确保列名为 'value'
        corp_debt_gdp_ratio_df = corp_debt_gdp_ratio.to_frame('value')
        corp_debt_gdp_ratio_df.to_csv(f"{output_dir}/CORPDEBT_GDP_PCT.csv")
        logger.info(f"✓ CORPDEBT_GDP_PCT 计算完成，数据点: {len(corp_debt_gdp_ratio)}")
        
    except Exception as e:
        logger.error(f"✗ CORPDEBT_GDP_PCT 计算失败: {e}")
    
    try:
        # 4. RESERVES_ASSETS_PCT = TOTRESNS / WALCL * 100
        logger.info("计算 RESERVES_ASSETS_PCT (准备金/资产%)...")
        reserves_data = pd.read_csv(f"{data_dir}/TOTRESNS/raw.csv", index_col=0, parse_dates=True)
        assets_data = pd.read_csv(f"{data_dir}/WALCL/raw.csv", index_col=0, parse_dates=True)
        
        # 对齐数据并计算比率
        reserves_values = reserves_data.iloc[:, 0]  # 取第一列数据
        assets_values = assets_data.iloc[:, 0]  # 取第一列数据
        reserves_aligned = reserves_values.reindex_like(assets_values).fillna(method="ffill")
        reserves_assets_ratio = (reserves_aligned / assets_values) * 100
        reserves_assets_ratio = reserves_assets_ratio.dropna()
        
        # 确保列名为 'value'
        reserves_assets_ratio_df = reserves_assets_ratio.to_frame('value')
        reserves_assets_ratio_df.to_csv(f"{output_dir}/RESERVES_ASSETS_PCT.csv")
        logger.info(f"✓ RESERVES_ASSETS_PCT 计算完成，数据点: {len(reserves_assets_ratio)}")
        
    except Exception as e:
        logger.error(f"✗ RESERVES_ASSETS_PCT 计算失败: {e}")
    
    try:
        # 5. RESERVES_DEPOSITS_PCT = TOTRESNS / DEPOSITS * 100
        logger.info("计算 RESERVES_DEPOSITS_PCT (准备金/存款%)...")
        reserves_data = pd.read_csv(f"{data_dir}/TOTRESNS/raw.csv", index_col=0, parse_dates=True)
        
        # 尝试不同的存款指标
        deposits_series = ["DPSACBW027SBOG", "TOTALSA", "TOTALSL"]
        deposits_data = None
        
        for dep_series in deposits_series:
            try:
                deposits_data = pd.read_csv(f"{data_dir}/{dep_series}/raw.csv", index_col=0, parse_dates=True)
                logger.info(f"使用存款指标: {dep_series}")
                break
            except:
                continue
        
        if deposits_data is not None:
            # 对齐数据并计算比率
            reserves_values = reserves_data.iloc[:, 0]  # 取第一列数据
            deposits_values = deposits_data.iloc[:, 0]  # 取第一列数据
            reserves_aligned = reserves_values.reindex_like(deposits_values).fillna(method="ffill")
            reserves_deposits_ratio = (reserves_aligned / deposits_values) * 100
            reserves_deposits_ratio = reserves_deposits_ratio.dropna()
            
            # 确保列名为 'value'
            reserves_deposits_ratio_df = reserves_deposits_ratio.to_frame('value')
            reserves_deposits_ratio_df.to_csv(f"{output_dir}/RESERVES_DEPOSITS_PCT.csv")
            logger.info(f"✓ RESERVES_DEPOSITS_PCT 计算完成，数据点: {len(reserves_deposits_ratio)}")
        else:
            logger.warning("⚠️ 未找到合适的存款指标，跳过 RESERVES_DEPOSITS_PCT 计算")
        
    except Exception as e:
        logger.error(f"✗ RESERVES_DEPOSITS_PCT 计算失败: {e}")
    
    logger.info("合成指标计算完成")


if __name__ == "__main__":
    main()
