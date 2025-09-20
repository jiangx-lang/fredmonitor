#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FRED 危机预警监控系统（稳态版）
- 修复：函数作用域/定义顺序，确保 to_monthly/transform_series 全局唯一且可见
- 兼容：pandas 月末频率 ('ME'/'M') 动态选择
- 清洗：全链路数值化（CSV读取、API获取、变换前）
- 兜底：NAPM 自动搜索替代系列；T10Y2Y/T10Y3M/TEDRATE 自动回算
- 统计：危机窗口不足样本时使用全样本兜底，避免 NaN 传播
- 输出：CSV + Markdown + JSON
"""

import os
import sys
import json
import yaml
import math
import warnings
import pathlib
import base64
import re
import subprocess
from datetime import datetime
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# ==== HARDENED SHIM: make sure to_monthly/transform_series always exist ====
import numpy as _np
import pandas as _pd

def _month_end_code() -> str:
    try:
        _pd.date_range("2000-01-31", periods=2, freq="ME")
        return "ME"
    except Exception:
        return "M"

_FRED_FREQ_ME = _month_end_code()

def _as_float_series__shim(s: _pd.Series) -> _pd.Series:
    s = _pd.to_numeric(s, errors="coerce").replace([_np.inf, -_np.inf], _np.nan)
    return s.astype("float64")

if "to_monthly" not in globals():
    def to_monthly(s: _pd.Series, how: str = "last") -> _pd.Series:
        """改进的月度聚合，支持不同聚合方法"""
        s = s.copy()
        s.index = _pd.to_datetime(s.index)
        s = s[~s.index.duplicated(keep="last")].sort_index()
        s = _as_float_series__shim(s)
        
        if how == "mean":
            return s.resample(_FRED_FREQ_ME).mean().astype("float64")
        elif how == "median":
            return s.resample(_FRED_FREQ_ME).median().astype("float64")
        else:  # last
            return s.resample(_FRED_FREQ_ME).last().astype("float64")

if "transform_series" not in globals():
    def transform_series(s: _pd.Series, method: str, agg: str = "last") -> _pd.Series:
        method = (method or "level").lower()
        s = to_monthly(s, agg)
        if method in ("level", ""):
            return s.dropna()
        if method == "diff":
            return _as_float_series__shim(s.diff()).dropna()
        if method == "mom_pct":
            return _as_float_series__shim(s.pct_change(1, fill_method=None) * 100.0).dropna()
        if method == "yoy_pct":
            return _as_float_series__shim(s.pct_change(12, fill_method=None) * 100.0).dropna()
        if method == "log_yoy":
            log_s = _as_float_series__shim(_np.log(s))
            return _as_float_series__shim(log_s.pct_change(12, fill_method=None) * 100.0).dropna()
        return s.dropna()
# ========================================================================

# ----------------------------- 抑制无关噪声 -----------------------------
warnings.filterwarnings("ignore", category=FutureWarning, message=".*will be removed in a future version.*")
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning, message=".*non-nanosecond datetime.*")

# ----------------------------- 工程路径/环境 -----------------------------
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

# 安全加载 .env 文件
try:
    # 尝试加载环境变量文件
    env_files = [BASE / "macrolab.env", BASE / ".env"]
    loaded = False
    for env_file in env_files:
        if env_file.exists():
            try:
                load_dotenv(env_file, encoding='utf-8')
                loaded = True
                print(f"✅ 环境变量加载成功: {env_file.name}")
                break
            except UnicodeDecodeError:
                try:
                    load_dotenv(env_file, encoding='gbk')
                    loaded = True
                    print(f"✅ 环境变量加载成功: {env_file.name}")
                    break
                except:
                    continue
    
    if not loaded:
        print("⚠️ 未找到环境变量文件，将使用系统环境变量")
except Exception as e:
    print(f"⚠️ 加载环境变量失败: {e}，将使用系统环境变量")

# ----------------------------- 依赖的自有模块 ----------------------------
from scripts.fred_http import series_observations, series_search
from scripts.clean_utils import parse_numeric_series
from scripts.viz import save_indicator_plot, create_long_chart, create_detailed_long_chart

# ----------------------------- 频率 & 数值化工具 -------------------------
def _month_end_code() -> str:
    """兼容不同 pandas 版本：优先 'ME'，不支持退回 'M'。"""
    try:
        pd.date_range("2000-01-31", periods=2, freq="ME")
        return "ME"
    except Exception:
        return "M"

FREQ_ME = _month_end_code()

def _as_float_series(s: pd.Series) -> pd.Series:
    """将序列安全转换为 float64，并清理 inf/NaN。"""
    s = pd.to_numeric(s, errors="coerce")
    s = s.replace([np.inf, -np.inf], np.nan)
    return s.astype("float64")

# ----------------------------- 配置 & 读写工具 --------------------------
def load_yaml_config(path: pathlib.Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _load_csv_standard(p: pathlib.Path) -> Optional[pd.Series]:
    """标准 CSV 读取：支持多列名，统一日期/数值清洗。"""
    try:
        df = pd.read_csv(p)
        cols = {c.lower(): c for c in df.columns}
        date_col = cols.get("date") or df.columns[0]
        val_col = cols.get("value") or ("value" if "value" in df.columns else df.columns[-1])

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col]).sort_values(date_col)

        vals = parse_numeric_series(df[val_col]).replace([np.inf, -np.inf], np.nan)
        s = pd.Series(vals.values, index=df[date_col], dtype="float64").dropna()
        s = s[~s.index.duplicated(keep="last")].sort_index()
        return s
    except Exception as e:
        print(f"❌ 读取失败 {p}: {e}")
        return None

def load_local_series_data(series_id: str) -> Optional[pd.Series]:
    """兼容两种本地结构，优先新路径 data/fred/series/<ID>/raw.csv。"""
    # A 新结构
    p = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
    if p.exists():
        s = _load_csv_standard(p)
        if s is not None:
            return s

    # B 旧结构
    root = BASE / "data" / "fred" / "categories"
    if root.exists():
        for cat in root.iterdir():
            fp = cat / "series" / series_id / "raw.csv"
            if fp.exists():
                s = _load_csv_standard(fp)
                if s is not None:
                    return s
    return None

def fetch_series_from_api(series_id: str) -> Optional[pd.Series]:
    """FRED HTTP API 获取观测值并清洗。"""
    try:
        data = series_observations(series_id, limit=100000)
        arr = (data or {}).get("observations", [])
        if not arr:
            return None
        df = pd.DataFrame(arr)[["date", "value"]].copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
        df["value"] = parse_numeric_series(df["value"]).replace([np.inf, -np.inf], np.nan)
        df = df.dropna(subset=["value"])
        s = pd.Series(df["value"].values, index=df["date"], dtype="float64")
        s = s[~s.index.duplicated(keep="last")].sort_index()
        return s
    except Exception as e:
        print(f"❌ API获取失败 {series_id}: {e}")
        return None

# ----------------------------- 获取序列（含兜底） ------------------------
def _is_bad(s: Optional[pd.Series]) -> bool:
    return (s is None) or s.empty or (s.dropna().empty)

def get_series_data(series_id: str) -> Optional[pd.Series]:
    """获取序列：本地→API→自动搜索替代→关键利差/ TED 回算。"""
    sid = series_id.upper()

    # --- 虚拟序列：先拦截并计算 ---
    if sid == "CP_MINUS_DTB3":
        cp = get_series_data("CPN3M")   # 3M AA 金融商业票据（年化％）
        tb = get_series_data("DTB3")    # 3M T-Bill（％）
        if not _is_bad(cp) and not _is_bad(tb):
            return (cp - tb)
        # 否则继续走本地/API/搜索兜底

    if sid == "SOFR20DMA_MINUS_DTB3":
        sf = get_series_data("SOFR")    # SOFR（％）
        tb = get_series_data("DTB3")    # 3M T-Bill（％）
        if not _is_bad(sf) and not _is_bad(tb):
            return (sf.rolling(20, min_periods=5).mean() - tb)

    # ---- 派生序列：企业债/GDP（%）----
    if sid in {"CORPDEBT_GDP_PCT", "NCBDBIQ027S_DIV_GDP_PCT"}:
        corp = load_local_series_data("NCBDBIQ027S")
        if corp is None:
            corp = fetch_series_from_api("NCBDBIQ027S")
        
        gdp = load_local_series_data("GDP")
        if gdp is None:
            gdp = fetch_series_from_api("GDP")
            
        if corp is None or gdp is None or corp.empty or gdp.empty:
            return None

        # 统一到季度末，用 last 聚合（两者本来就是季频，但稳妥起见）
        corp_q = corp.resample("Q").last().astype("float64")          # 百万美元
        gdp_q  = (gdp * 1000.0).resample("Q").last().astype("float64")# 十亿美元 -> 百万美元

        # 确保两个序列有相同的索引
        common_index = corp_q.index.intersection(gdp_q.index)
        if len(common_index) == 0:
            return None
            
        corp_aligned = corp_q.loc[common_index]
        gdp_aligned = gdp_q.loc[common_index]

        ratio = (corp_aligned / gdp_aligned) * 100.0
        ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
        return ratio

    # ---- 派生：准备金/存款（%）----
    if sid in {"RESERVES_DEPOSITS_PCT", "TOTRESNS_DIV_DEPOSITS_PCT"}:
        # TOTRESNS：银行总准备金（十亿美元，月或周）
        reserves = load_local_series_data("TOTRESNS")
        if reserves is None:
            reserves = fetch_series_from_api("TOTRESNS")
        # DPSACBW027SBOG：Total Deposits, All Commercial Banks（常用 H.8 系列）
        deposits = load_local_series_data("DPSACBW027SBOG")
        if deposits is None:
            deposits = fetch_series_from_api("DPSACBW027SBOG")
        if _is_bad(reserves) or _is_bad(deposits):
            return None

        # 统一到月频，用月均更稳（两者可能是周度/半周度）
        r_m = pd.to_numeric(reserves, errors="coerce").resample("M").mean()
        d_m = pd.to_numeric(deposits, errors="coerce").resample("M").mean()

        ratio = (r_m / d_m) * 100.0    # 单位同为"十亿美元"时可直接相除
        ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
        return ratio

    # ---- 派生：准备金/总资产（%）----
    if sid in {"RESERVES_ASSETS_PCT", "TOTRESNS_DIV_ASSETS_PCT"}:
        reserves = load_local_series_data("TOTRESNS")
        if reserves is None:
            reserves = fetch_series_from_api("TOTRESNS")
        assets = load_local_series_data("TLAACBW027SBOG")
        if assets is None:
            assets = fetch_series_from_api("TLAACBW027SBOG")  # Total Assets, All Commercial Banks
        if _is_bad(reserves) or _is_bad(assets):
            return None

        r_m = pd.to_numeric(reserves, errors="coerce").resample("M").mean()
        a_m = pd.to_numeric(assets,   errors="coerce").resample("M").mean()

        ratio = (r_m / a_m) * 100.0
        ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
        return ratio

    # 1) 本地
    s = load_local_series_data(sid)
    if _is_bad(s):
        print(f"⚠️ 本地无数据/坏数据，尝试API获取 {sid}")
        s = fetch_series_from_api(sid)

    # 2) 自动搜索替代（如 NAPM）
    if _is_bad(s):
        try:
            keywords = [sid, "ISM Manufacturing PMI", "Manufacturing PMI", "ISM PMI", "NAPM"]
            for kw in keywords:
                res = series_search(kw, limit=5)
                cand = (res or {}).get("seriess", [])
                for it in cand:
                    alt_id = it.get("id")
                    if not alt_id or alt_id == sid:
                        continue
                    s_alt = fetch_series_from_api(alt_id)
                    if not _is_bad(s_alt):
                        print(f"🔎 用替代系列 {alt_id} 代替 {sid}（keyword='{kw}'）")
                        s, sid = s_alt, alt_id
                        break
                if not _is_bad(s):
                    break
        except Exception as e:
            print(f"🔎 搜索替代系列失败：{e}")

    # 3) 关键利差/ TED 的回算
    try:
        if _is_bad(s):
            if sid.upper() == "T10Y2Y":
                s10 = get_series_data("DGS10")
                s2  = get_series_data("DGS2")
                if not _is_bad(s10) and not _is_bad(s2):
                    s = (s10.asfreq("D").interpolate() - s2.asfreq("D").interpolate())
                    print("🔁 用 DGS10 - DGS2 回算 T10Y2Y")
            elif sid.upper() == "T10Y3M":
                s10 = get_series_data("DGS10")
                tb3 = get_series_data("DTB3")
                if not _is_bad(s10) and not _is_bad(tb3):
                    s = (s10.asfreq("D").interpolate() - tb3.asfreq("D").interpolate())
                    print("🔁 用 DGS10 - DTB3 回算 T10Y3M")
            elif sid.upper() == "TEDRATE":
                sofr = get_series_data("SOFR")
                tb3  = get_series_data("DTB3")
                if not _is_bad(sofr) and not _is_bad(tb3):
                    s = (sofr.rolling(20, min_periods=5).mean() - tb3)
                    print("🔁 用 SOFR(20日均) - DTB3 回算 TEDRATE 替代")
    except RecursionError:
        pass

    return s if not _is_bad(s) else None

# ----------------------------- 危机统计/评分 -----------------------------
def slice_crisis_window(s: pd.Series, start: str, end: str) -> pd.Series:
    """对齐月末；若为空则±1个月宽容。"""
    try:
        start_m = pd.to_datetime(start).to_period("M").to_timestamp("M")
        end_m   = pd.to_datetime(end).to_period("M").to_timestamp("M")
        win = s.loc[start_m:end_m].dropna()
        if not win.empty:
            return win
        return s.loc[start_m - pd.offsets.MonthEnd(1): end_m + pd.offsets.MonthEnd(1)].dropna()
    except Exception:
        return pd.Series(dtype=float)

def _merge_intervals(crises):
    """将危机区间做并集合并，避免重叠导致重复计样本。"""
    ivals = []
    for c in crises:
        s = pd.to_datetime(c["start"]).to_period("M").to_timestamp("M")
        e = pd.to_datetime(c["end"]).to_period("M").to_timestamp("M")
        ivals.append((min(s, e), max(s, e)))
    ivals.sort(key=lambda x: x[0])
    merged = []
    for s, e in ivals:
        if not merged or s > merged[-1][1]:
            merged.append([s, e])
        else:
            merged[-1][1] = max(merged[-1][1], e)
    return [(a, b) for a, b in merged]

def _pack(vals: np.ndarray) -> Dict[str, float]:
    return {
        "median": float(np.nanmedian(vals)),
        "p25": float(np.nanpercentile(vals, 25)),
        "p65": float(np.nanpercentile(vals, 65)),
        "p75": float(np.nanpercentile(vals, 75)),
        "p90": float(np.nanpercentile(vals, 90)),
        "mean": float(np.nanmean(vals)),
        "std": float(np.nanstd(vals, ddof=1)),
        "n": int(vals.size),
    }

def compute_distributions(ts: pd.Series, crises: List[dict], series_id: str = None) -> Dict[str, Dict[str, float]]:
    """返回 {'crisis': {...}, 'noncrisis': {...}, 'all': {...}} 三套分布统计。"""
    ts = ts.dropna().astype(float)
    if ts.empty:
        nanpack = {k: np.nan for k in ["median","p25","p75","p90","mean","std"]}
        nanpack["n"] = 0
        return {"crisis": nanpack, "noncrisis": nanpack, "all": nanpack}

    merged = _merge_intervals(crises)
    # 危机样本
    cvals = []
    for s, e in merged:
        cvals.extend(ts.loc[s:e].values.tolist())
    carr = np.asarray(cvals, dtype=float)
    carr = carr[~np.isnan(carr)]

    # 对T10Y3M/T10Y2Y特殊处理：危机样本<6时扩展窗口
    if series_id in ["T10Y3M", "T10Y2Y"] and carr.size < 6:
        print(f"  ⚠️ {series_id} 危机样本不足({carr.size})，扩展窗口...")
        # 扩展危机窗口到±1-2个季度
        extended_cvals = []
        for s, e in merged:
            extended_start = s - pd.DateOffset(months=6)
            extended_end = e + pd.DateOffset(months=6)
            extended_cvals.extend(ts.loc[extended_start:extended_end].values.tolist())
        carr = np.asarray(extended_cvals, dtype=float)
        carr = carr[~np.isnan(carr)]

    # 非危机样本 = 全样本 - 危机样本索引
    mask = pd.Series(False, index=ts.index)
    for s, e in merged:
        mask.loc[s:e] = True
    non_ts = ts.loc[~mask]

    out = {
        "crisis": _pack(carr) if carr.size > 0 else _pack(ts.values),
        "noncrisis": _pack(non_ts.values) if non_ts.size > 0 else _pack(ts.values),
        "all": _pack(ts.values),
    }
    return out

def resolve_baseline(ts: pd.Series, crises: List[dict], policy: str, *, recent_years: int = 15, series_id: str = None) -> dict:
    """
    统一的基准策略解析器（支持新配置的基准类型）
    输入：已transform并dropna的月频序列ts，及危机窗口crises
    输出：一个dict，至少含 {"ref": 基准值, "policy": 策略名, "notes": 说明字符串}
    """
    # 利率类指标使用近代样本（1990年后），避免70-80年代高利率稀释
    if series_id in ["FEDFUNDS", "DTB3", "DGS10", "MORTGAGE30US", "SOFR"] and policy.startswith("noncrisis"):
        # 使用1990年后的数据计算非危机期基准
        ts_modern = ts[ts.index >= '1990-01-01']
        if not ts_modern.empty:
            dist_modern = compute_distributions(ts_modern, crises)
            if policy == "noncrisis_p25":
                return {"ref": dist_modern["noncrisis"]["p25"], "policy": policy, "notes": "非危机期P25（1990年后）"}
            elif policy == "noncrisis_p65":
                return {"ref": dist_modern["noncrisis"]["p65"], "policy": policy, "notes": "非危机期P65（1990年后）"}
            elif policy == "noncrisis_p75":
                return {"ref": dist_modern["noncrisis"]["p75"], "policy": policy, "notes": "非危机期P75（1990年后）"}
            elif policy == "noncrisis_p90":
                return {"ref": dist_modern["noncrisis"]["p90"], "policy": policy, "notes": "非危机期P90（1990年后）"}
            elif policy == "noncrisis_median":
                return {"ref": dist_modern["noncrisis"]["median"], "policy": policy, "notes": "非危机期中位数（1990年后）"}
    
    # 使用compute_distributions获取分布统计
    dist = compute_distributions(ts, crises)
    
    # 支持新的基准类型
    if policy == "crisis_median":
        return {"ref": dist["crisis"]["median"], "policy": policy, "notes": "危机期中位数"}
    
    if policy == "crisis_p25":
        return {"ref": dist["crisis"]["p25"], "policy": policy, "notes": "危机期P25"}
    
    if policy == "crisis_p75":
        return {"ref": dist["crisis"]["p75"], "policy": policy, "notes": "危机期P75"}
    
    if policy == "noncrisis_p25":
        return {"ref": dist["noncrisis"]["p25"], "policy": policy, "notes": "非危机期P25"}
    
    if policy == "noncrisis_p65":
        return {"ref": dist["noncrisis"]["p65"], "policy": policy, "notes": "非危机期P65"}
    
    if policy == "noncrisis_p75":
        return {"ref": dist["noncrisis"]["p75"], "policy": policy, "notes": "非危机期P75"}
    
    if policy == "noncrisis_p90":
        return {"ref": dist["noncrisis"]["p90"], "policy": policy, "notes": "非危机期P90"}
    
    if policy == "noncrisis_median":
        return {"ref": dist["noncrisis"]["median"], "policy": policy, "notes": "非危机期中位数"}
    
    if policy == "all_p75":
        return {"ref": dist["all"]["p75"], "policy": policy, "notes": "全样本P75"}
    
    if policy == "all_p90":
        return {"ref": dist["all"]["p90"], "policy": policy, "notes": "全样本P90"}
    
    # 兜底
    return {"ref": dist["noncrisis"]["median"], "policy": "fallback_noncrisis_median", "notes": "非危机中位数（兜底）"}

def get_benchmark_by_policy(ts: pd.Series, crises: List[dict], policy: str) -> float:
    """
    policy 可取：
      'crisis_median'/'crisis_p25'/'crisis_p75'/'crisis_mean'
      'noncrisis_p75'/'noncrisis_p90'
      'all_p75'/'all_p90'
    若目标分位缺失，依次退化为：noncrisis -> all -> crisis -> np.nan
    """
    dist = compute_distributions(ts, crises)
    m = policy.lower()

    mapping = {
        "crisis_median": ("crisis", "median"),
        "crisis_p25": ("crisis", "p25"),
        "crisis_p75": ("crisis", "p75"),
        "crisis_mean": ("crisis", "mean"),
        "noncrisis_p75": ("noncrisis", "p75"),
        "noncrisis_p90": ("noncrisis", "p90"),
        "all_p75": ("all", "p75"),
        "all_p90": ("all", "p90"),
    }
    if m not in mapping:
        # 兼容旧配置
        m = "crisis_median"

    grp, key = mapping[m]
    val = dist.get(grp, {}).get(key, np.nan)

    # 兜底：避免 None/NaN
    if np.isnan(val):
        for g in ["noncrisis", "all", "crisis"]:
            v = dist.get(g, {}).get(key, np.nan)
            if not np.isnan(v):
                return float(v)
        return np.nan
    return float(val)

def calculate_crisis_stats(s: pd.Series, crises: List[dict]) -> Dict[str, float]:
    """危机统计；窗口样本 <6 用全样本兜底，避免 NaN 传播。"""
    def _pack(vals: np.ndarray) -> Dict[str, float]:
        return {
            "crisis_median": float(np.nanmedian(vals)),
            "crisis_p25":    float(np.nanpercentile(vals, 25)),
            "crisis_p75":    float(np.nanpercentile(vals, 75)),
            "crisis_mean":   float(np.nanmean(vals)),
            "crisis_std":    float(np.nanstd(vals, ddof=1)),
        }

    vals: List[float] = []
    for c in crises:
        win = slice_crisis_window(s, c["start"], c["end"])
        if not win.empty:
            vals.extend(win.values.tolist())

    arr = np.asarray(vals, dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size >= 6:
        return _pack(arr)

    allv = s.dropna().values.astype(float)
    if allv.size == 0:
        return {k: np.nan for k in ["crisis_median","crisis_p25","crisis_p75","crisis_mean","crisis_std"]}
    return _pack(allv)

def calculate_post2000_noncrisis_stats(s: pd.Series, crises: List[dict], post_qe_start: str = None) -> Dict[str, float]:
    """计算2000年后非危机期统计，避免70-80年代超高利率污染基准，支持QE后样本"""
    def _pack(vals: np.ndarray) -> Dict[str, float]:
        return {
            "crisis_median": float(np.nanmedian(vals)),
            "crisis_p25":    float(np.nanpercentile(vals, 25)),
            "crisis_p75":    float(np.nanpercentile(vals, 75)),
            "crisis_mean":   float(np.nanmean(vals)),
            "crisis_std":    float(np.nanstd(vals, ddof=1)),
        }

    # 获取2000年后的数据
    s_2000 = s[s.index >= '2000-01-01']
    if s_2000.empty:
        return {k: np.nan for k in ["crisis_median","crisis_p25","crisis_p75","crisis_mean","crisis_std"]}
    
    # 如果指定了QE后起始日期，则只使用QE后的数据
    if post_qe_start:
        s_2000 = s_2000[s_2000.index >= post_qe_start]
        if s_2000.empty:
            return {k: np.nan for k in ["crisis_median","crisis_p25","crisis_p75","crisis_mean","crisis_std"]}
    
    # 排除危机期数据
    crisis_dates = set()
    for c in crises:
        crisis_start = pd.to_datetime(c["start"])
        crisis_end = pd.to_datetime(c["end"])
        # 只考虑2000年后的危机期
        if crisis_start >= pd.to_datetime('2000-01-01'):
            crisis_mask = (s_2000.index >= crisis_start) & (s_2000.index <= crisis_end)
            crisis_dates.update(s_2000[crisis_mask].index)
    
    # 非危机期数据
    non_crisis_data = s_2000[~s_2000.index.isin(crisis_dates)]
    
    if non_crisis_data.empty:
        # 如果没有非危机期数据，使用2000年后全样本
        non_crisis_data = s_2000
    
    vals = non_crisis_data.dropna().values.astype(float)
    if vals.size == 0:
        return {k: np.nan for k in ["crisis_median","crisis_p25","crisis_p75","crisis_mean","crisis_std"]}
    
    return _pack(vals)

def calculate_zscore(value: float, mean: float, std: float) -> float:
    if std is None or std == 0 or np.isnan(std) or np.isnan(mean):
        return np.nan
    return (value - mean) / std

# 过期阈值常量
FRESHNESS_DAYS_MONTHLY = 60
FRESHNESS_DAYS_QUARTERLY = 120


def is_stale(last_obs: pd.Timestamp, freshness_days: int) -> bool:
    """检查数据是否过期 - 使用期差而非天数避免月内误判"""
    today = pd.Timestamp.today().normalize()
    
    # 根据freshness_days判断数据频率
    if freshness_days <= 30:  # 日频/周频/月频数据
        # 使用月份差：超过1个月为过期
        today_period = today.to_period('M')
        last_period = last_obs.to_period('M')
        period_diff = today_period - last_period
        return period_diff.n > 1
    elif freshness_days <= 120:  # 季频数据
        # 使用季度差：超过1个季度为过期
        today_period = today.to_period('Q')
        last_period = last_obs.to_period('Q')
        period_diff = today_period - last_period
        return period_diff.n > 1
    else:
        # 兜底：使用天数差
        return (today - last_obs) > pd.Timedelta(days=freshness_days)

def fmt_value(val: float, transform: str) -> str:
    """
    格式化数值显示，YoY/MoM数据统一加%符号
    """
    if np.isnan(val):
        return "N/A"
    
    if transform in {"yoy_pct", "mom_pct"}:
        return f"{val:.2f}%"
    else:
        return f"{val:.3f}"

def score_curve(x_pp: float) -> float:
    """
    收益率曲线专用评分函数（倒挂要重罚）
    x_pp: 百分点，例如-0.25表示-25bp
    返回: 0-100分
    """
    if np.isnan(x_pp):
        return 50.0
    
    if x_pp >= 0:
        # 未倒挂：40~60之间，越靠近0越危险
        # 参考带宽：0~+100bp
        return 40 + 20 * max(0, 1 - min(x_pp/1.0, 1))  # 0bp≈60分，+100bp≈40分
    else:
        # 已倒挂：60~100之间，-100bp及以下封顶
        return 60 + 40 * min(abs(x_pp)/1.0, 1)

def score_point(value: float, baseline: dict, higher_is_risk: bool, ts: pd.Series = None, crises: List[dict] = None, series_id: str = None) -> float:
    """
    统一的评分函数（按照新配置的公式）
    记号：
      B = 基准值（按 compare_to 取值）
      d = +1 (higher_is_risk=true)；d = -1 (higher_is_risk=false)
      IQR = noncrisis_p75 - noncrisis_p25（非危机样本四分位差）
      dev = d * (current - B)
      norm = clip( dev / max(IQR, eps), -iqr_clip, +iqr_clip )
      score_single = clip( 50 + 10 * norm, 0, 100 )
    """
    if np.isnan(value) or np.isnan(baseline["ref"]):
        return 50.0
    
    # 收益率曲线使用专用评分函数
    if series_id in ["T10Y3M", "T10Y2Y"]:
        return score_curve(value)
    
    policy = baseline["policy"]
    ref = baseline["ref"]
    
    # 计算IQR（非危机样本四分位差）
    eps = 1.0e-6
    iqr_clip = 5.0
    
    if ts is not None and crises is not None:
        # 计算非危机样本的IQR
        dist = compute_distributions(ts, crises)
        noncrisis_p25 = dist["noncrisis"]["p25"]
        noncrisis_p75 = dist["noncrisis"]["p75"]
        IQR = max(noncrisis_p75 - noncrisis_p25, eps)
    else:
        # 兜底：使用基准值的10%作为IQR
        IQR = max(abs(ref) * 0.1, eps)
    
    # 计算偏离度
    if higher_is_risk:
        dev = value - ref  # 越高越危险：当前值-基准值
    else:
        dev = ref - value  # 越低越危险：基准值-当前值
    
    # 归一化
    norm = max(-iqr_clip, min(iqr_clip, dev / IQR))
    
    # 计算最终分数
    score = max(0, min(100, 50 + 10 * norm))
    
    # NFCI 硬阈值规则：若 current >= 0，在原评分基础上 +5 分
    if series_id == "NFCI" and value >= 0.0:
        score = min(100, score + 5)
    
    return score

def score_from_deviation(current: float, bench: float, crisis_vals: np.ndarray, 
                        mode: str = "iqr_tanh", higher_is_risk: bool = True, 
                        double_tail: bool = False) -> float:
    """改进的评分方法：IQR归一化 + tanh平滑，避免饱和"""
    if np.isnan(current) or np.isnan(bench) or crisis_vals.size == 0:
        return 50.0
    
    # 双尾风险：过高=紧张，过低=自满（如信用利差）
    if double_tail:
        # 计算与中位数的绝对偏离
        median_val = np.nanmedian(crisis_vals)
        abs_dev = abs(current - median_val)
        
        # 使用IQR作为尺度
        p25, p75 = np.nanpercentile(crisis_vals, [25, 75])
        scale = max(p75 - p25, 1e-9)
        
        z = abs_dev / scale
        
        if "tanh" in mode:
            return float(50 + 45 * np.tanh(z))
        return float(max(0, min(100, 50 + 10 * z)))
    
    # 单尾风险：传统逻辑
    sign = 1.0 if higher_is_risk else -1.0
    dev = sign * (current - bench)
    
    if mode.startswith("iqr"):
        p25, p75 = np.nanpercentile(crisis_vals, [25, 75])
        scale = max(p75 - p25, 1e-9)
    elif mode.startswith("std"):
        scale = max(np.nanstd(crisis_vals, ddof=1), 1e-9)
    else:  # linear
        scale = 1.0
    
    z = dev / scale
    
    if "tanh" in mode:
        return float(50 + 45 * np.tanh(z))
    return float(max(0, min(100, 50 + 10 * z)))


# ----------------------------- 报告输出 -------------------------------
def calculate_group_scores(results: List[dict], groups_config: dict) -> dict:
    """计算分组风险评分（支持新的global_weight配置）"""
    successful = [r for r in results if r["status"] == "success"]
    
    group_scores = {}
    group_details = {}
    
    # 权重归一化
    total_weight = sum(group_info["weight"] for group_info in groups_config.values())
    if abs(total_weight - 1.0) > 1e-9:
        print(f"⚠️ 权重未归一化，总权重={total_weight:.3f}，正在归一化...")
        for group_info in groups_config.values():
            group_info["weight"] = group_info["weight"] / total_weight
    
    # 调试输出
    print("\n🔍 分组评分调试信息:")
    print("=" * 60)
    
    for group_id, group_info in groups_config.items():
        group_name = group_info.get("name", group_info.get("title", group_id))  # 兼容name和title字段
        group_weight = group_info["weight"]  # 这里应该是小数，如0.25而不是25
        
        # 找到属于该组的指标（只包含参与计分的指标）
        group_indicators = [r for r in successful if r.get("group") == group_id and r.get("include_in_score", True)]
        
        if group_indicators:
            # 计算组内加权平均分（支持global_weight）
            scores = [r["risk_score"] for r in group_indicators]
            
            # 检查是否有global_weight字段
            has_global_weights = any(r.get("global_weight") is not None for r in group_indicators)
            
            if has_global_weights:
                # 使用global_weight进行加权平均，应用过期数据降权
                weights = []
                freshness_weights = []
                for r in group_indicators:
                    base_weight = r.get("global_weight", 0.0)
                    is_stale = r.get("stale", False)
                    freshness_weight = 0.9 if is_stale else 1.0
                    effective_weight = base_weight * freshness_weight
                    weights.append(effective_weight)
                    freshness_weights.append(freshness_weight)
                
                total_weight = sum(weights)
                if total_weight > 0:
                    group_avg = sum(s * w for s, w in zip(scores, weights)) / total_weight
                else:
                    group_avg = np.mean(scores)
            else:
                # 传统方法：组内等权平均，应用过期数据降权
                freshness_weights = [0.9 if r.get("stale", False) else 1.0 for r in group_indicators]
                weighted_scores = [s * fw for s, fw in zip(scores, freshness_weights)]
                total_freshness_weight = sum(freshness_weights)
                if total_freshness_weight > 0:
                    group_avg = sum(weighted_scores) / total_freshness_weight
                else:
                    group_avg = np.mean(scores)
            
            # 红线规则：若组内≥1核心指标分数≥80，组分数至少抬到60
            high_risk_count = sum(1 for s in scores if s >= 80)
            if high_risk_count >= 1:
                group_avg = max(group_avg, 60)
            
            group_scores[group_id] = group_avg
            group_details[group_id] = {
                "name": group_name,
                "weight": group_weight,
                "score": group_avg,
                "count": len(group_indicators),
                "total_count": len([r for r in successful if r.get("group") == group_id]),  # 包括过期指标
                "contribution": group_avg * group_weight,
                "indicators": group_indicators
            }
            
            # 调试输出
            contribution = group_avg * group_weight
            print(f"  {group_name}: 组内均分={group_avg:.1f}, 权重={group_weight:.2f}, 贡献={contribution:.1f}")
            print(f"    指标: {[r['indicator'] for r in group_indicators]}")
        else:
            group_scores[group_id] = 50.0  # 默认中性分
            group_details[group_id] = {
                "name": group_name,
                "weight": group_weight,
                "score": 50.0,
                "count": 0,
                "indicators": []
            }
            print(f"  {group_name}: 无有效指标，使用默认分50.0")
    
    # 权重校验 - 必须合计=1.0
    total_weight = sum(groups_config[g]["weight"] for g in groups_config)
    if abs(total_weight - 1.0) > 1e-9:
        print(f"❌ 权重校验失败: 总权重={total_weight:.6f}, 必须=1.0")
        print("🔧 正在归一化权重...")
        for g in groups_config:
            groups_config[g]["weight"] = groups_config[g]["weight"] / total_weight
        print(f"✅ 权重归一化完成: 新总权重={sum(groups_config[g]['weight'] for g in groups_config):.6f}")
    else:
        print(f"✅ 权重校验通过: 总权重={total_weight:.6f}")
    
    # 计算加权总分（支持global_weight）
    # 检查是否有global_weight字段
    has_global_weights = any(r.get("global_weight") is not None for r in successful)
    
    if has_global_weights:
        # 使用global_weight直接计算总分，应用过期数据降权
        total_score = 0.0
        total_weight = 0.0
        
        for r in successful:
            if r.get("include_in_score", True):
                base_weight = r.get("global_weight", 0.0)
                is_stale = r.get("stale", False)
                freshness_weight = 0.9 if is_stale else 1.0
                effective_weight = base_weight * freshness_weight
                
                total_score += r["risk_score"] * effective_weight
                total_weight += effective_weight
        
        if total_weight > 0:
            total_score = total_score / total_weight
        else:
            total_score = 50.0  # 兜底
    else:
        # 传统方法：分组权重计算
        total_score = sum(group_scores[g] * groups_config[g]["weight"] for g in group_scores)
    
    # 总分红线规则：若全体核心指标中≥2个≥80，总分下限不低于40
    all_scores = [r["risk_score"] for r in successful if not r.get("stale", False)]
    high_risk_count = sum(1 for s in all_scores if s >= 80)
    if high_risk_count >= 2:
        total_score = max(total_score, 40)
    
    # 高风险占比惩罚：占比>30% → 总分+5；>50% → 总分+10
    high_risk_ratio = high_risk_count / len(all_scores) if all_scores else 0
    if high_risk_ratio > 0.5:
        total_score += 10
    elif high_risk_ratio > 0.3:
        total_score += 5
    
    # 总分上限100
    total_score = min(total_score, 100)
    
    # 调试输出总分
    print(f"\n📊 总分计算: {total_score:.1f} (高风险指标: {high_risk_count}/{len(all_scores)}, 占比: {high_risk_ratio:.1%})")
    print("=" * 60)
    
    return {
        "total_score": total_score,
        "group_scores": group_scores,
        "group_details": group_details
    }

def generate_markdown_report(results: List[dict], crises: List[dict], output_file: pathlib.Path, groups_config: dict = None):
    lines: List[str] = []
    lines.append("# 🚨 宏观金融危机监察报告")
    lines.append("")
    lines.append(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    lines.append("")
    lines.append("## 📋 报告说明")
    lines.append("")
    lines.append("本报告基于FRED宏观指标，将当前值与历史危机期间基准值比较，以评估风险。")
    lines.append("")
    lines.append("【数据由人采集和处理，请批判看待这些数据，欢迎email jiangx@gmail.com 任何问题讨论】")
    lines.append("")
    lines.append("风险评分范围 0-100：50 为中性，越高越危险（除非指标设定为'越低越危险'）。")
    lines.append("")
    lines.append("采用分组加权评分：先计算各组平均分，再按权重合成总分。")
    lines.append("")
    lines.append("总分 = ∑(分组平均分 × 分组权重)，分组权重归一处理后合成。")
    lines.append("")
    lines.append("过期数据处理：月频数据>60天、季频数据>120天标记⚠️，过期数据权重×0.9。")
    lines.append("")
    lines.append("颜色分段：0–39 🔵 极低，40–59 🟢 低，60–79 🟡 中，80–100 🔴 高；50 为中性。")
    lines.append("")

    successful = [r for r in results if r["status"] == "success"]
    skipped = [r for r in results if r["status"] == "skipped"]
    
    if successful and groups_config:
        # 使用分组打分
        scoring_result = calculate_group_scores(results, groups_config)
        total_score = scoring_result["total_score"]
        group_details = scoring_result["group_details"]
        
        lines.append("## 🎯 总体风险概览")
        lines.append("")
        lines.append(f"- **加权风险总分**: {total_score:.1f}/100")
        lines.append("")
        # 统计所有启用的指标（包括成功、失败、跳过的）
        total_enabled = len([r for r in results if r.get("status") in ["success", "error", "skipped"]])
        lines.append(f"- **成功监控指标**: {len(successful)}/{total_enabled}")
        lines.append("")
        if skipped:
            lines.append(f"- **跳过指标数量**: {len(skipped)}")
            lines.append("")
        errors = [r for r in results if r["status"] == "error"]
        if errors:
            lines.append(f"- **失败指标数量**: {len(errors)}")
            lines.append("")
        
        # 分组详情
        lines.append("### 📊 分组风险评分")
        lines.append("")
        for group_id, details in group_details.items():
            if details["count"] > 0:
                lines.append(f"- **{details['name']}**: {details['score']:.1f}/100 (权重: {details['weight']:.0%}, 指标数: {details['count']})")
                lines.append("")
        
        # 总体风险等级
        if   total_score >= 80: overall = "🔴 高风险"
        elif total_score >= 60: overall = "🟡 中风险"
        elif total_score >= 40: overall = "🟢 低风险"
        else:                   overall = "🔵 极低风险"
        lines.append(f"**总体风险等级**: {overall}")
        lines.append("")
        
    elif successful:
        # 传统平均分方法（向后兼容）
        avg = np.mean([r["risk_score"] for r in successful])
        hi = len([r for r in successful if r["risk_score"] >= 80])
        mid = len([r for r in successful if 60 <= r["risk_score"] < 80])

        lines.append("## 🎯 总体风险概览")
        lines.append("")
        lines.append(f"- **平均风险评分**: {avg:.1f}/100")
        lines.append(f"- **高风险指标数量**: {hi}")
        lines.append(f"- **中风险指标数量**: {mid}")
        lines.append(f"- **成功监控指标**: {len(successful)}/{len(results)}")
        if skipped:
            lines.append(f"- **跳过指标数量**: {len(skipped)}")
        lines.append("")
        if   avg >= 80: overall = "🔴 高风险"
        elif avg >= 60: overall = "🟡 中风险"
        elif avg >= 40: overall = "🟢 低风险"
        else:           overall = "🔵 极低风险"
        lines.append(f"**总体风险等级**: {overall}")
        lines.append("")

    def _dump_block(title: str, arr: List[dict]):
        if arr:
            lines.append(title)
            lines.append("")
            for r in arr:
                stale_marker = " ⚠️(过期)" if r.get('stale', False) else ""
                lines.append(f"### {r['indicator']} ({r['series_id']}){stale_marker}")
                lines.append(f"- **当前值**: {r.get('current_value')} {r.get('unit','')}")
                lines.append(f"- **基准值**: {r.get('benchmark_value')} ({r.get('baseline_policy', 'N/A')})")
                lines.append(f"- **风险评分**: {r.get('risk_score')}/100  {r.get('risk_level')}")
                lines.append(f"- **偏离度**: {r.get('deviation')}")
                lines.append(f"- **历史Z分数**: {r.get('zscore')}")
                # 添加方向说明
                direction_text = "该指标越高越危险" if r.get('higher_is_risk', True) else "该指标越低越危险"
                lines.append(f"- **方向说明**: {direction_text}")
                if r.get("explanation"):
                    lines.append(f"- **解释**: {r['explanation']}")
                if r.get('figure'):
                    lines.append(f"![{r['series_id']}]({r['figure']})")
                lines.append("")

    _dump_block("## 🔴 高风险指标", [r for r in successful if r["risk_score"] >= 80])
    _dump_block("## 🟡 中风险指标", [r for r in successful if 60 <= r["risk_score"] < 80])
    _dump_block("## 🟢 低风险指标",  [r for r in successful if r["risk_score"] < 60])

    # 跳过指标
    if skipped:
        lines.append("## ⏭️ 跳过指标")
        lines.append("")
        for r in skipped:
            lines.append(f"- **{r['indicator']}** ({r['series_id']}): {r['error_message']}")
        lines.append("")

    # 错误指标
    errors = [r for r in results if r["status"] == "error"]
    if errors:
        lines.append("## ❌ 数据获取失败")
        lines.append("")
        for r in errors:
            lines.append(f"- **{r['indicator']}** ({r['series_id']}): {r['error_message']}")
        lines.append("")

    lines.append("## 📅 历史危机期间参考")
    lines.append("")
    for c in crises:
        lines.append(f"- **{c['name']}** ({c['code']}): {c['start']} 至 {c['end']}")
        if c.get("description"):
            lines.append(f"  - {c['description']}")

    lines.append("")
    lines.append("## ⚠️ 免责声明")
    lines.append("本报告仅供参考，不构成投资建议。历史数据不保证未来表现。")

    # 添加指标配置表
    lines.append("")
    lines.append("## 📋 指标配置表")
    lines.append("")
    lines.append("| 指标名称 | 分组 | 基准分位 | 基准理由 | 变换方法 | 计分方式 |")
    lines.append("|---------|------|----------|----------|----------|----------|")
    
    # 获取指标配置
    indicators_cfg = load_yaml_config(BASE / "config" / "crisis_indicators.yaml")
    indicators = indicators_cfg["indicators"]
    
    for ind in indicators:
        if ind.get("enabled", True):
            name = ind["name"]
            group = ind.get("group", "uncategorized")
            compare_to = ind.get("compare_to", "crisis_median")
            baseline_reason = ind.get("baseline_reason", "未说明")
            transform = ind.get("transform", "level")
            include_in_score = ind.get("include_in_score", True)
            score_method = "按组平均" if include_in_score else "不计分"
            
            lines.append(f"| {name} | {group} | {compare_to} | {baseline_reason} | {transform} | {score_method} |")
    
    lines.append("")
    lines.append("### 📖 基准分位解释")
    lines.append("")
    lines.append("| 基准分位 | 含义 | 使用场景 |")
    lines.append("|---------|------|----------|")
    lines.append("| **noncrisis_p25** | 非危机期间25%分位数（较低值） | 收益率曲线倒挂、消费者信心等'越低越危险'指标 |")
    lines.append("| **noncrisis_p75** | 非危机期间75%分位数（较高值） | 利率水平、信用利差等'越高越危险'指标 |")
    lines.append("| **noncrisis_p90** | 非危机期间90%分位数（很高值） | VIX波动率等需要更灵敏捕捉抬升的指标 |")
    lines.append("| **crisis_median** | 历史危机期间中位数 | 信用利差、金融状况指数等危机敏感指标 |")
    lines.append("| **crisis_p25** | 历史危机期间25%分位数 | 实体经济指标（就业、GDP等）'越低越危险' |")
    lines.append("| **noncrisis_median** | 非危机期间中位数 | 美元指数等中性指标 |")
    lines.append("")
    lines.append("**说明**：")
    lines.append("- **p25/p75/p90**：表示历史数据中25%/75%/90%的观测值低于该水平")
    lines.append("- **noncrisis**：排除历史危机期间的数据，反映'正常'经济环境")
    lines.append("- **crisis**：仅使用历史危机期间的数据，反映'危险'水平")
    lines.append("")
    
    lines.append("## 📅 危机窗口定义")
    lines.append("")
    lines.append("本报告使用的历史危机期间包括：")
    for crisis in crises:
        lines.append(f"- **{crisis['name']}**: {crisis['start']} 至 {crisis['end']}")
    
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*本报告基于FRED数据，仅供参考，不构成投资建议*")

    output_file.write_text("\n".join(lines), encoding="utf-8")

def _read_png_as_base64(p: pathlib.Path) -> str | None:
    """读取PNG文件并转换为base64字符串"""
    try:
        with open(p, "rb") as f:
            return base64.b64encode(f.read()).decode("ascii")
    except Exception:
        return None

_IMG_MD_RE = re.compile(r"!\[[^\]]*\]\((?P<path>[^)]+\.png)\)")

def render_html_report(md_text: str, report_title: str, report_dir: pathlib.Path) -> str:
    """
    把 Markdown 中的相对 PNG 链接替换为 base64 <img>，输出完整 HTML 字符串。
    仅处理形如 ![](figures/xxx.png) 的相对路径。
    """
    def repl(m: re.Match) -> str:
        rel = m.group("path")
        img_path = (report_dir / rel).resolve()
        b64 = _read_png_as_base64(img_path)
        if not b64:
            # 读不到就保留原始相对链接（降级）
            return f'<img alt="" src="{rel}"/>'
        return f'<img alt="" src="data:image/png;base64,{b64}"/>'

    body = _IMG_MD_RE.sub(repl, md_text)

    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{report_title}</title>
<style>
  body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;line-height:1.6;padding:16px;max-width:940px;margin:auto;}}
  img{{max-width:100%;height:auto;display:block;margin:8px 0;}}
  code,pre{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;}}
  h1,h2,h3{{line-height:1.25}}
  .kpi{{display:flex;gap:12px;flex-wrap:wrap}}
  .kpi div{{padding:8px 12px;border-radius:10px;background:#f5f5f7}}
</style>
</head>
<body>
<article>
{body}
</article>
</body>
</html>"""
    return html

def export_html(md_path: pathlib.Path, html_path: pathlib.Path, embed_images: bool = False):
    """
    将Markdown报告导出为移动端友好的HTML
    """
    try:
        import markdown
        
        # 读取Markdown内容
        md_content = md_path.read_text(encoding="utf-8")
        
        # 转换为HTML
        html_content = markdown.markdown(md_content, extensions=['tables', 'fenced_code'])
        
        # 移动端友好的HTML模板
        html_template = f"""<!doctype html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>宏观金融危机监察报告</title>
    <style>
        body{{max-width:900px;margin:0 auto;padding:16px;line-height:1.6;font-family:-apple-system,Segoe UI,Roboto,PingFang SC,Helvetica,Arial}}
        img{{max-width:100%;height:auto;display:block;margin:8px 0}}
        table{{width:100%;border-collapse:collapse}}
        th,td{{border:1px solid #eee;padding:6px 8px}}
        .muted{{color:#888;font-size:0.9em}}
        h1,h2,h3{{line-height:1.25;margin:1.2em 0 .6em}}
        .kpi{{display:flex;gap:12px;flex-wrap:wrap}}
        .kpi div{{padding:8px 12px;border-radius:10px;background:#f5f5f7}}
    </style>
</head>
<body>
    <article>
        {html_content}
    </article>
</body>
</html>"""
        
        # 如果要求嵌入图片，将相对路径转换为base64
        if embed_images:
            import base64
            import re
            
            def replace_img_src(match):
                src = match.group(1)
                if src.startswith('data:') or src.startswith('http'):
                    return match.group(0)
                
                img_path = md_path.parent / src
                if img_path.exists():
                    try:
                        img_data = img_path.read_bytes()
                        b64_data = base64.b64encode(img_data).decode('ascii')
                        mime_type = 'image/png' if src.endswith('.png') else 'image/jpeg'
                        return f'<img src="data:{mime_type};base64,{b64_data}"'
                    except Exception:
                        return match.group(0)
                return match.group(0)
            
            html_content = re.sub(r'<img\s+src="([^"]+)"', replace_img_src, html_content)
        
        # 写入HTML文件
        html_path.write_text(html_template, encoding="utf-8")
        print(f"✅ HTML报告已生成: {html_path}")
        
    except Exception as e:
        print(f"❌ HTML导出失败: {e}")

def generate_outputs(results: List[dict], crises: List[dict], timestamp: str, groups_config: dict = None, data_cache: dict = None):
    out_dir = BASE / "outputs" / "crisis_monitor"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = timestamp

    # 计算分组评分并添加调试信息
    if groups_config:
        scoring_result = calculate_group_scores(results, groups_config)
        total_score = scoring_result["total_score"]
        group_details = scoring_result["group_details"]
        
        print(f"\n🔍 分组评分调试信息:")
        print("=" * 60)
        for group_id, details in group_details.items():
            if details["count"] > 0:
                stale_count = details["total_count"] - details["count"]
                stale_info = f", 过期: {stale_count}" if stale_count > 0 else ""
                print(f"  {details['name']}: 组内均分={details['score']:.1f}, 权重={details['weight']:.2f}, 贡献={details['contribution']:.1f}")
                print(f"    指标: {[r['indicator'] for r in results if r.get('group') == group_id and r.get('status') == 'success']}")
        
        # 权重校验
        total_weight = sum(groups_config[g]["weight"] for g in groups_config)
        if abs(total_weight - 1.0) > 1e-9:
            print(f"⚠️ 权重未归一化，总权重={total_weight:.3f}，正在归一化...")
            for g in groups_config:
                groups_config[g]["weight"] = groups_config[g]["weight"] / total_weight
            print(f"✅ 权重归一化完成: 新总权重={sum(groups_config[g]['weight'] for g in groups_config):.6f}")
        else:
            print(f"✅ 权重校验通过: 总权重={total_weight:.6f}")
        
        print(f"\n📊 总分计算: {total_score:.1f}")
        print("=" * 60)

    # 1) 写 Markdown
    md_path = out_dir / f"crisis_report_{ts}.md"
    generate_markdown_report(results, crises, md_path, groups_config)

    # 2) 生成移动端友好的HTML
    html_path = out_dir / f"crisis_report_{ts}.html"
    export_html(md_path, html_path, embed_images=False)  # 使用相对路径，手机浏览器可直接显示
    
    # 3) 生成"最新"副本（手机上只点 latest.html 就行）
    latest_md = out_dir / "latest.md"
    latest_html = out_dir / "latest.html"
    latest_md.write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")
    export_html(latest_md, latest_html, embed_images=False)

    # 4) JSON（保留原有功能）
    json_path = out_dir / f"crisis_report_{ts}.json"
    json_path.write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    # 5) 生成长图版本（使用Playwright）
    long_image_path = out_dir / f"crisis_report_long_{ts}.png"
    try:
        import asyncio
        from playwright.async_api import async_playwright
        
        async def generate_long_image():
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # 设置视口大小
                await page.set_viewport_size({"width": 1080, "height": 1200})
                
                # 加载HTML文件
                await page.goto(f"file://{html_path.absolute()}")
                
                # 等待页面加载完成
                await page.wait_for_load_state("networkidle")
                
                # 获取页面高度
                page_height = await page.evaluate("document.body.scrollHeight")
                
                # 设置视口高度为页面高度
                await page.set_viewport_size({"width": 1080, "height": page_height})
                
                # 截取全页面
                await page.screenshot(
                    path=str(long_image_path),
                    full_page=True
                )
                
                await browser.close()
        
        # 运行异步函数
        asyncio.run(generate_long_image())
        
        if long_image_path.exists():
            file_size = long_image_path.stat().st_size / 1024  # KB
            print(f"✅ 长图已生成: {long_image_path} ({file_size:.1f} KB)")
        else:
            print(f"⚠️ 长图生成失败")
            
    except Exception as e:
        print(f"⚠️ 长图生成失败: {e}")

    print("\n📄 报告已生成：")
    print(f"  📝 MD : {md_path}")
    print(f"  🌐 HTML: {html_path}（移动端友好）")
    print(f"  👉 快捷打开: {latest_html}")
    print(f"  📋 JSON: {json_path}")
    print(f"  📱 长图: {long_image_path}")

# ----------------------------- 主流程 -------------------------------
def generate_crisis_report():
    print("🚨 FRED 危机预警监控系统启动...")
    print("=" * 80)

    indicators_cfg = load_yaml_config(BASE / "config" / "crisis_indicators.yaml")
    crises_cfg = load_yaml_config(BASE / "config" / "crisis_periods.yaml")
    indicators = indicators_cfg["indicators"]
    crises = crises_cfg["crises"]
    
    # 支持新的配置结构
    if "weights" in indicators_cfg and "groups" in indicators_cfg["weights"]:
        groups_config = indicators_cfg["weights"]["groups"]
    else:
        groups_config = indicators_cfg.get("groups", {})  # 向后兼容

    print(f"📊 指标数: {len(indicators)}")
    print(f"📅 危机段: {len(crises)}")

    # 生成时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results: List[dict] = []
    cache: Dict[str, pd.Series] = {}

    for i, ind in enumerate(indicators, 1):
        name         = ind["name"]
        series_id    = ind["series_id"]
        transform    = ind.get("transform", "level")
        higher_risk  = bool(ind.get("higher_is_risk", True))
        compare_to   = ind.get("compare_to", "crisis_median")
        unit_hint    = ind.get("unit_hint", "")
        explainer    = ind.get("plain_explainer", "")
        enabled      = ind.get("enabled", True)
        group        = ind.get("group", "uncategorized")
        global_weight = ind.get("global_weight", 0.0)
        include_in_score = ind.get("include_in_score", True)

        # 跳过未启用的指标
        if not enabled:
            print(f"\r[{i}/{len(indicators)}] ⏭️ 跳过 {name} ({series_id}) - 已禁用")
            continue

        # 显示进度
        progress = f"[{i}/{len(indicators)}]"
        print(f"\r{progress} 🔄 处理 {name} ({series_id})...", end="", flush=True)

        try:
            # 拿数据（含本地/远程/兜底）
            if series_id not in cache:
                s = get_series_data(series_id)
                if _is_bad(s):
                    # 检查是否为可选指标
                    if ind.get("optional", False):
                        results.append({
                            "indicator": name, "series_id": series_id, "status": "skipped",
                            "error_message": "可选指标：API连接失败/本地无数据，已跳过", 
                            "current_value": np.nan, "benchmark_value": np.nan, 
                            "deviation": np.nan, "risk_score": 50.0, "zscore": np.nan, 
                            "unit": unit_hint, "explanation": explainer
                        })
                        print(f"  ⏭️ 可选指标已跳过: {name}")
                        continue
                    else:
                        results.append({
                            "indicator": name, "series_id": series_id, "status": "error",
                            "error_message": "无法获取数据", "current_value": np.nan,
                            "benchmark_value": np.nan, "deviation": np.nan,
                            "risk_score": 50.0, "zscore": np.nan, "unit": unit_hint,
                            "explanation": explainer
                        })
                        continue
                cache[series_id] = s
            else:
                s = cache[series_id]

            # 变换（支持聚合方法）
            agg_method = ind.get("agg", "last")
            ts = transform_series(s, transform, agg_method).dropna()
            if ts.empty:
                results.append({
                    "indicator": name, "series_id": series_id, "status": "error",
                    "error_message": "变换后无数据", "current_value": np.nan,
                    "benchmark_value": np.nan, "deviation": np.nan,
                    "risk_score": 50.0, "zscore": np.nan, "unit": unit_hint,
                    "explanation": explainer
                })
                continue

            # 当前值
            last_date = ts.index[-1]
            current_value = float(ts.iloc[-1])
            
            # 使用新的配置系统
            include_in_score = ind.get("include_in_score", True)
            freshness_days = ind.get("freshness_days", 60)  # 默认60天
            baseline_policy = compare_to  # 使用compare_to字段
            
            # 检测数据是否过期 - 使用配置的freshness_days
            is_stale_data = is_stale(last_date, freshness_days)
            
            # 是否参与计分：过期数据不计分，或显式设置不计分
            effective_include_in_score = include_in_score and not is_stale_data
            
            # 使用新的基准策略解析器
            baseline_info = resolve_baseline(ts, crises, compare_to, series_id=series_id)
            benchmark_value = baseline_info["ref"]
            
            # 打印危机窗口合并信息（仅第一次）
            if i == 1:
                merged = _merge_intervals(crises)
                print(f"\n📊 Merged crisis windows: {len(crises)} → {len(merged)}")
            
            # 调试输出危机基准（特别关注UMCSENT等指标）
            if series_id in ["UMCSENT", "T10Y3M", "SOFR20DMA_MINUS_DTB3"]:
                print(f"\n  🔍 {series_id} 危机基准调试:")
                print(f"    当前值: {current_value:.2f}")
                print(f"    基准值: {benchmark_value:.2f}")
                print(f"    基准类型: {compare_to}")
                print(f"    方向: {'高为险' if higher_risk else '低为险'}")

            # 生成图表
            fig_dir = BASE / "outputs" / "crisis_monitor" / "figures"
            fig_path = fig_dir / f"{series_id}_latest.png"
            try:
                title_for_plot = f"{name} ({series_id})"
                # 生成crisis_stats用于图表
                dist = compute_distributions(ts, crises, series_id)
                crisis_stats = {
                    "crisis_median": dist["crisis"]["median"],
                    "crisis_p25": dist["crisis"]["p25"],
                    "crisis_p75": dist["crisis"]["p75"],
                    "crisis_mean": dist["crisis"]["mean"]
                }
                
                save_indicator_plot(
                    ts=ts, 
                    title=title_for_plot,
                    unit=unit_hint or "",
                    crises=crises,
                    crisis_stats=crisis_stats,
                    out_path=fig_path
                )
                # 使用相对路径而不是base64嵌入
                # 计算相对于Markdown文件的相对路径
                md_file = BASE / "outputs" / "crisis_monitor" / f"crisis_report_{timestamp}.md"
                rel_path = fig_path.relative_to(md_file.parent)
                figure_rel = str(rel_path).replace('\\', '/')
                print(f"  📊 图片路径: {figure_rel}")
            except Exception as e:
                print(f"\r{progress} ⚠️ 图表生成失败: {e}")
                figure_rel = None

            # 历史均值/波动
            hist_mean = float(np.nanmean(ts.values))
            hist_std  = float(np.nanstd(ts.values, ddof=1))
            z = calculate_zscore(current_value, hist_mean, hist_std)

            # 计算偏离度（用于展示）
            raw_deviation = current_value - benchmark_value if not np.isnan(benchmark_value) else np.nan
            
            # 使用新的统一评分系统
            risk_score = score_point(current_value, baseline_info, higher_risk, ts, crises, series_id)

            # 风险等级
            if   risk_score >= 80: level = "🔴 高风险"
            elif risk_score >= 60: level = "🟡 中风险"
            elif risk_score >= 40: level = "🟢 低风险"
            else:                  level = "🔵 极低风险"
            
            # 格式化显示值
            current_display = fmt_value(current_value, transform)
            benchmark_display = fmt_value(benchmark_value, transform) if not np.isnan(benchmark_value) else "N/A"
            
            # 显示处理结果
            print(f"\r{progress} ✅ {name}: {current_display} | {level} ({risk_score:.1f}/100)")

            results.append({
                "indicator": name, "series_id": series_id, "status": "success",
                "group": group,
                "global_weight": global_weight,
                "include_in_score": effective_include_in_score,
                "include_reason": "过期不计分" if is_stale_data else ("显式不计分" if not include_in_score else "正常计分"),
                "last_observation": last_date.strftime("%Y-%m-%d"),
                "current_value": round(current_value, 4),
                "current_display": current_display,  # 添加格式化显示值
                "baseline_policy": compare_to,  # 使用配置中的compare_to
                "baseline_notes": baseline_info["notes"],
                "benchmark_value": round(benchmark_value, 4) if not np.isnan(benchmark_value) else None,
                "benchmark_display": benchmark_display,  # 添加格式化显示值
                "benchmark_type": compare_to,  # 统一使用配置中的基准类型
                "deviation": round(raw_deviation, 4) if not np.isnan(raw_deviation) else None,
                "risk_score": round(risk_score, 1), "risk_level": level,
                "zscore": round(z, 2) if not np.isnan(z) else None,
                "unit": unit_hint, "higher_is_risk": higher_risk,
                "explanation": explainer, "data_points": len(ts),
                "crisis_periods_used": len([c for c in crises if not slice_crisis_window(ts, c["start"], c["end"]).empty]),
                "figure": figure_rel,
                "stale": bool(is_stale_data),
                "freshness_days": freshness_days,
                "transform": transform  # 添加变换类型
            })

            print(f"  ✅ 当前值: {current_display}")
            print(f"  📊 基准值: {benchmark_display} ({compare_to})")
            print(f"  📈 风险评分: {risk_score:.1f} ({level})")
            print(f"  📋 计分状态: {'✅' if effective_include_in_score else '❌'} {results[-1]['include_reason']}")

        except Exception as e:
            print(f"\r{progress} ❌ 处理失败: {str(e)[:50]}...")
            results.append({
                "indicator": name, "series_id": series_id, "status": "error",
                "error_message": str(e), "current_value": np.nan,
                "benchmark_value": np.nan, "deviation": np.nan,
                "risk_score": 50.0, "zscore": np.nan, "unit": unit_hint,
                "explanation": explainer
            })

    # 统计处理结果
    successful = [r for r in results if r["status"] == "success"]
    errors = [r for r in results if r["status"] == "error"]
    skipped = [r for r in results if r["status"] == "skipped"]
    total_enabled = len(successful) + len(errors) + len(skipped)
    
    print(f"\n🎉 危机预警报告生成完成！")
    print(f"   ✅ 成功处理: {len(successful)} 个指标")
    print(f"   📊 总计指标: {total_enabled} 个")
    if errors:
        print(f"   ❌ 处理失败: {len(errors)} 个")
    if skipped:
        print(f"   ⏭️ 跳过指标: {len(skipped)} 个")

    generate_outputs(results, crises, timestamp, groups_config, cache)
    return results
            "explanation": explainer
        }

        try:
            # 拿数据（含本地/远程/兜底）
            if series_id not in cache:
                s = get_series_data(series_id)
                if _is_bad(s):
                    # 检查是否为可选指标
                    if ind.get("optional", False):
                    return {
                            "indicator": name, "series_id": series_id, "status": "skipped",
                            "error_message": "可选指标：API连接失败/本地无数据，已跳过", 
                            "current_value": np.nan, "benchmark_value": np.nan, 
                            "deviation": np.nan, "risk_score": 50.0, "zscore": np.nan, 
                            "unit": unit_hint, "explanation": explainer
                    }
                    else:
                    return {
                            "indicator": name, "series_id": series_id, "status": "error",
                            "error_message": "无法获取数据", "current_value": np.nan,
                            "benchmark_value": np.nan, "deviation": np.nan,
                            "risk_score": 50.0, "zscore": np.nan, "unit": unit_hint,
                            "explanation": explainer
                    }
                cache[series_id] = s
            else:
                s = cache[series_id]

        # 变换（支持聚合方法）
        agg_method = ind.get("agg", "last")
        ts = transform_series(s, transform, agg_method).dropna()
        if ts.empty:
            return {
                "indicator": name, "series_id": series_id, "status": "error",
                "error_message": "变换后无数据", "current_value": np.nan,
                "benchmark_value": np.nan, "deviation": np.nan,
                "risk_score": 50.0, "zscore": np.nan, "unit": unit_hint,
                "explanation": explainer
            }

        # 当前值
        last_date = ts.index[-1]
        current_value = float(ts.iloc[-1])
        
        # 使用新的配置系统
        include_in_score = ind.get("include_in_score", True)
        freshness_days = ind.get("freshness_days", 60)  # 默认60天
        baseline_policy = compare_to  # 使用compare_to字段
        
        # 检测数据是否过期 - 使用配置的freshness_days
        is_stale_data = is_stale(last_date, freshness_days)
        
        # 是否参与计分：过期数据不计分，或显式设置不计分
        effective_include_in_score = include_in_score and not is_stale_data
        
        # 使用新的基准策略解析器
        baseline_info = resolve_baseline(ts, crises, compare_to, series_id=series_id)
        benchmark_value = baseline_info["ref"]
        
        # 调试输出危机基准（特别关注UMCSENT等指标）
        if series_id in ["UMCSENT", "T10Y3M", "SOFR20DMA_MINUS_DTB3"]:
            print(f"\n  🔍 {series_id} 危机基准调试:")
            print(f"    当前值: {current_value:.2f}")
            print(f"    基准值: {benchmark_value:.2f}")
            print(f"    基准类型: {compare_to}")
            print(f"    方向: {'高为险' if higher_risk else '低为险'}")

        # 生成图表
        fig_dir = BASE / "outputs" / "crisis_monitor" / "figures"
        fig_path = fig_dir / f"{series_id}_latest.png"
        try:
            title_for_plot = f"{name} ({series_id})"
            # 生成crisis_stats用于图表
            dist = compute_distributions(ts, crises, series_id)
            crisis_stats = {
                "crisis_median": dist["crisis"]["median"],
                "crisis_p25": dist["crisis"]["p25"],
                "crisis_p75": dist["crisis"]["p75"],
                "crisis_mean": dist["crisis"]["mean"]
            }
            
            save_indicator_plot(
                ts=ts, 
                title=title_for_plot,
                unit=unit_hint or "",
                crises=crises,
                crisis_stats=crisis_stats,
                out_path=fig_path
            )
            # 使用相对路径而不是base64嵌入
            # 计算相对于Markdown文件的相对路径
            md_file = BASE / "outputs" / "crisis_monitor" / f"crisis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            rel_path = fig_path.relative_to(md_file.parent)
            figure_rel = str(rel_path).replace('\\', '/')
        except Exception as e:
            figure_rel = None

        # 历史均值/波动
        hist_mean = float(np.nanmean(ts.values))
        hist_std  = float(np.nanstd(ts.values, ddof=1))
        z = calculate_zscore(current_value, hist_mean, hist_std)

        # 计算偏离度（用于展示）
        raw_deviation = current_value - benchmark_value if not np.isnan(benchmark_value) else np.nan
        
        # 使用新的统一评分系统
        risk_score = score_point(current_value, baseline_info, higher_risk, ts, crises, series_id)

        # 风险等级
        if   risk_score >= 80: level = "🔴 高风险"
        elif risk_score >= 60: level = "🟡 中风险"
        elif risk_score >= 40: level = "🟢 低风险"
        else:                  level = "🔵 极低风险"
        
        # 格式化显示值
        current_display = fmt_value(current_value, transform)
        benchmark_display = fmt_value(benchmark_value, transform) if not np.isnan(benchmark_value) else "N/A"
        
        return {
            "indicator": name, "series_id": series_id, "status": "success",
            "group": group,
            "global_weight": global_weight,
            "include_in_score": effective_include_in_score,
            "include_reason": "过期不计分" if is_stale_data else ("显式不计分" if not include_in_score else "正常计分"),
            "last_observation": last_date.strftime("%Y-%m-%d"),
            "current_value": round(current_value, 4),
            "current_display": current_display,
            "baseline_policy": compare_to,
            "baseline_notes": baseline_info["notes"],
            "benchmark_value": round(benchmark_value, 4) if not np.isnan(benchmark_value) else None,
            "benchmark_display": benchmark_display,
            "benchmark_type": compare_to,
            "deviation": round(raw_deviation, 4) if not np.isnan(raw_deviation) else None,
            "risk_score": round(risk_score, 1), "risk_level": level,
            "zscore": round(z, 2) if not np.isnan(z) else None,
            "unit": unit_hint, "higher_is_risk": higher_risk,
            "explanation": explainer, "data_points": len(ts),
            "crisis_periods_used": len([c for c in crises if not slice_crisis_window(ts, c["start"], c["end"]).empty]),
            "figure": figure_rel,
            "stale": bool(is_stale_data),
            "freshness_days": freshness_days,
            "transform": transform
        }

    except Exception as e:
        return {
            "indicator": name, "series_id": series_id, "status": "error",
            "error_message": str(e), "current_value": np.nan,
            "benchmark_value": np.nan, "deviation": np.nan,
            "risk_score": 50.0, "zscore": np.nan, "unit": unit_hint,
            "explanation": explainer
        }

# ----------------------------- 主流程 -------------------------------
def generate_crisis_report():
    print("🚨 FRED 危机预警监控系统启动...")
    print("=" * 80)

    indicators_cfg = load_yaml_config(BASE / "config" / "crisis_indicators.yaml")
    crises_cfg = load_yaml_config(BASE / "config" / "crisis_periods.yaml")
    indicators = indicators_cfg["indicators"]
    crises = crises_cfg["crises"]
    
    # 支持新的配置结构
    if "weights" in indicators_cfg and "groups" in indicators_cfg["weights"]:
        groups_config = indicators_cfg["weights"]["groups"]
    else:
        groups_config = indicators_cfg.get("groups", {})  # 向后兼容

    print(f"📊 指标数: {len(indicators)}")
    print(f"📅 危机段: {len(crises)}")

    # 生成时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    results: List[dict] = []
    cache: Dict[str, pd.Series] = {}

    # 打印危机窗口合并信息（仅第一次）
    merged = _merge_intervals(crises)
    print(f"\n📊 Merged crisis windows: {len(crises)} → {len(merged)}")

    # 使用并发处理指标（最多4个并发）
    print(f"\n🚀 开始并发处理 {len(indicators)} 个指标...")
    success_count = 0
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 提交所有任务
        future_to_indicator = {
            executor.submit(process_single_indicator, ind, i, len(indicators), cache, crises): (ind, i)
            for i, ind in enumerate(indicators, 1)
        }
        
        # 处理完成的任务
        for future in as_completed(future_to_indicator):
            ind, i = future_to_indicator[future]
            series_id = ind.get("series_id", "unknown")
            name = ind.get("name", "unknown")
            
            try:
                result = future.result()  # 获取结果，如果有异常会抛出
                results.append(result)
                
                if result["status"] == "success":
                    success_count += 1
                    print(f"\r[{i}/{len(indicators)}] ✅ {name}: {result['current_display']} | {result['risk_level']} ({result['risk_score']:.1f}/100)")
                    print(f"  ✅ 当前值: {result['current_display']}")
                    print(f"  📊 基准值: {result['benchmark_display']} ({result['baseline_policy']})")
                    print(f"  📈 风险评分: {result['risk_score']:.1f} ({result['risk_level']})")
                    print(f"  📋 计分状态: {'✅' if result['include_in_score'] else '❌'} {result['include_reason']}")
                elif result["status"] == "skipped":
                    print(f"\r[{i}/{len(indicators)}] ⏭️ 跳过 {name} ({series_id}) - {result['error_message']}")
                else:  # error
                    print(f"\r[{i}/{len(indicators)}] ❌ 处理失败 {name} ({series_id}): {result['error_message'][:50]}...")
                    
            except Exception as e:
                print(f"\r[{i}/{len(indicators)}] ❌ 处理失败 {name} ({series_id}): {str(e)[:50]}...")
                results.append({
                    "indicator": name, "series_id": series_id, "status": "error",
                    "error_message": str(e), "current_value": np.nan,
                    "benchmark_value": np.nan, "deviation": np.nan,
                    "risk_score": 50.0, "zscore": np.nan, "unit": "",
                    "explanation": ""
                })

    # 按原始顺序排序结果
    results.sort(key=lambda x: indicators.index(next(ind for ind in indicators if ind["series_id"] == x["series_id"])))

    # 统计处理结果
    successful = [r for r in results if r["status"] == "success"]
    errors = [r for r in results if r["status"] == "error"]
    skipped = [r for r in results if r["status"] == "skipped"]
    total_enabled = len(successful) + len(errors) + len(skipped)
    
    print(f"\n🎉 危机预警报告生成完成！")
    print(f"   ✅ 成功处理: {len(successful)} 个指标")
    print(f"   📊 总计指标: {total_enabled} 个")
    if errors:
        print(f"   ❌ 处理失败: {len(errors)} 个")
    if skipped:
        print(f"   ⏭️ 跳过指标: {len(skipped)} 个")

    generate_outputs(results, crises, timestamp, groups_config, cache)
    return results
            ts = transform_series(s, transform, agg_method).dropna()
            if ts.empty:
                results.append({
                    "indicator": name, "series_id": series_id, "status": "error",
                    "error_message": "变换后无数据", "current_value": np.nan,
                    "benchmark_value": np.nan, "deviation": np.nan,
                    "risk_score": 50.0, "zscore": np.nan, "unit": unit_hint,
                    "explanation": explainer
                })
                continue

            # 当前值
            last_date = ts.index[-1]
            current_value = float(ts.iloc[-1])

            # 使用新的配置系统
            include_in_score = ind.get("include_in_score", True)
            freshness_days = ind.get("freshness_days", 60)  # 默认60天
            baseline_policy = compare_to  # 使用compare_to字段
            
            # 检测数据是否过期 - 使用配置的freshness_days
            is_stale_data = is_stale(last_date, freshness_days)
            
            # 是否参与计分：过期数据不计分，或显式设置不计分
            effective_include_in_score = include_in_score and not is_stale_data
            
            # 使用新的基准策略解析器
            baseline_info = resolve_baseline(ts, crises, compare_to, series_id=series_id)
            benchmark_value = baseline_info["ref"]
            
            # 打印危机窗口合并信息（仅第一次）
            if i == 1:
                merged = _merge_intervals(crises)
                print(f"\n📊 Merged crisis windows: {len(crises)} → {len(merged)}")
            
            # 调试输出危机基准（特别关注UMCSENT等指标）
            if series_id in ["UMCSENT", "T10Y3M", "SOFR20DMA_MINUS_DTB3"]:
                print(f"\n  🔍 {series_id} 危机基准调试:")
                print(f"    当前值: {current_value:.2f}")
                print(f"    基准值: {benchmark_value:.2f}")
                print(f"    基准类型: {compare_to}")
                print(f"    方向: {'高为险' if higher_risk else '低为险'}")

            # 生成图表
            fig_dir = BASE / "outputs" / "crisis_monitor" / "figures"
            fig_path = fig_dir / f"{series_id}_latest.png"
            try:
                title_for_plot = f"{name} ({series_id})"
                # 生成crisis_stats用于图表
                dist = compute_distributions(ts, crises, series_id)
                crisis_stats = {
                    "crisis_median": dist["crisis"]["median"],
                    "crisis_p25": dist["crisis"]["p25"],
                    "crisis_p75": dist["crisis"]["p75"],
                    "crisis_mean": dist["crisis"]["mean"]
                }
                
                save_indicator_plot(
                    ts=ts, 
                    title=title_for_plot,
                    unit=unit_hint or "",
                    crises=crises,
                    crisis_stats=crisis_stats,
                    out_path=fig_path
                )
                # 使用相对路径而不是base64嵌入
                # 计算相对于Markdown文件的相对路径
                md_file = BASE / "outputs" / "crisis_monitor" / f"crisis_report_{timestamp}.md"
                rel_path = fig_path.relative_to(md_file.parent)
                figure_rel = str(rel_path).replace('\\', '/')
                print(f"  📊 图片路径: {figure_rel}")
            except Exception as e:
                print(f"\r{progress} ⚠️ 图表生成失败: {e}")
                figure_rel = None

            # 历史均值/波动
            hist_mean = float(np.nanmean(ts.values))
            hist_std  = float(np.nanstd(ts.values, ddof=1))
            z = calculate_zscore(current_value, hist_mean, hist_std)

            # 计算偏离度（用于展示）
            raw_deviation = current_value - benchmark_value if not np.isnan(benchmark_value) else np.nan
            
            # 使用新的统一评分系统
            risk_score = score_point(current_value, baseline_info, higher_risk, ts, crises, series_id)

            # 风险等级
            if   risk_score >= 80: level = "🔴 高风险"
            elif risk_score >= 60: level = "🟡 中风险"
            elif risk_score >= 40: level = "🟢 低风险"
            else:                  level = "🔵 极低风险"
            
            # 格式化显示值
            current_display = fmt_value(current_value, transform)
            benchmark_display = fmt_value(benchmark_value, transform) if not np.isnan(benchmark_value) else "N/A"
            
            # 显示处理结果
            print(f"\r{progress} ✅ {name}: {current_display} | {level} ({risk_score:.1f}/100)")

            results.append({
                "indicator": name, "series_id": series_id, "status": "success",
                "group": group,
                "global_weight": global_weight,
                "include_in_score": effective_include_in_score,
                "include_reason": "过期不计分" if is_stale_data else ("显式不计分" if not include_in_score else "正常计分"),
                "last_observation": last_date.strftime("%Y-%m-%d"),
                "current_value": round(current_value, 4),
                "current_display": current_display,  # 添加格式化显示值
                "baseline_policy": compare_to,  # 使用配置中的compare_to
                "baseline_notes": baseline_info["notes"],
                "benchmark_value": round(benchmark_value, 4) if not np.isnan(benchmark_value) else None,
                "benchmark_display": benchmark_display,  # 添加格式化显示值
                "benchmark_type": compare_to,  # 统一使用配置中的基准类型
                "deviation": round(raw_deviation, 4) if not np.isnan(raw_deviation) else None,
                "risk_score": round(risk_score, 1), "risk_level": level,
                "zscore": round(z, 2) if not np.isnan(z) else None,
                "unit": unit_hint, "higher_is_risk": higher_risk,
                "explanation": explainer, "data_points": len(ts),
                "crisis_periods_used": len([c for c in crises if not slice_crisis_window(ts, c["start"], c["end"]).empty]),
                "figure": figure_rel,
                "stale": bool(is_stale_data),
                "freshness_days": freshness_days,
                "transform": transform  # 添加变换类型
            })

            print(f"  ✅ 当前值: {current_display}")
            print(f"  📊 基准值: {benchmark_display} ({compare_to})")
            print(f"  📈 风险评分: {risk_score:.1f} ({level})")
            print(f"  📋 计分状态: {'✅' if effective_include_in_score else '❌'} {results[-1]['include_reason']}")

        except Exception as e:
            print(f"\r{progress} ❌ 处理失败: {str(e)[:50]}...")
            results.append({
                "indicator": name, "series_id": series_id, "status": "error",
                "error_message": str(e), "current_value": np.nan,
                "benchmark_value": np.nan, "deviation": np.nan,
                "risk_score": 50.0, "zscore": np.nan, "unit": unit_hint,
                "explanation": explainer
            })

    # 统计处理结果
    successful = [r for r in results if r["status"] == "success"]
    errors = [r for r in results if r["status"] == "error"]
    skipped = [r for r in results if r["status"] == "skipped"]
    total_enabled = len(successful) + len(errors) + len(skipped)
    
    print(f"\n🎉 危机预警报告生成完成！")
    print(f"   ✅ 成功处理: {len(successful)} 个指标")
    print(f"   📊 总计指标: {total_enabled} 个")
    if errors:
        print(f"   ❌ 处理失败: {len(errors)} 个")
    if skipped:
        print(f"   ⏭️ 跳过指标: {len(skipped)} 个")
    
    generate_outputs(results, crises, timestamp, groups_config, cache)
    return results

def generate_pdf_report(results: List[dict], crises: List[dict], pdf_path: pathlib.Path, groups_config: dict = None):
    """
    生成PDF报告
    """
    try:
        # 先生成HTML内容
        md_path = pdf_path.parent / f"temp_report_{pdf_path.stem}.md"
        generate_markdown_report(results, crises, md_path, groups_config)
        
        # 读取markdown内容
        md_text = md_path.read_text(encoding="utf-8")
        
        # 转换为HTML
        html_text = render_html_report(md_text, report_title="宏观金融危机监察报告", report_dir=pdf_path.parent)
        
        # 使用wkhtmltopdf转换为PDF
        html_temp_path = pdf_path.parent / f"temp_report_{pdf_path.stem}.html"
        html_temp_path.write_text(html_text, encoding="utf-8")
        
        # 调用wkhtmltopdf
        cmd = [
            "wkhtmltopdf",
            "--page-size", "A4",
            "--margin-top", "20mm",
            "--margin-bottom", "20mm", 
            "--margin-left", "15mm",
            "--margin-right", "15mm",
            "--encoding", "UTF-8",
            "--enable-local-file-access",
            str(html_temp_path),
            str(pdf_path)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ PDF报告已生成: {pdf_path}")
            # 清理临时文件
            html_temp_path.unlink(missing_ok=True)
            md_path.unlink(missing_ok=True)
        else:
            print(f"❌ PDF生成失败: {result.stderr}")
            # 如果wkhtmltopdf不可用，尝试使用weasyprint
            try_generate_pdf_with_weasyprint(html_text, pdf_path)
            
    except FileNotFoundError:
        print("⚠️ wkhtmltopdf未安装，尝试使用weasyprint...")
        try_generate_pdf_with_weasyprint(html_text, pdf_path)
    except Exception as e:
        print(f"❌ PDF生成失败: {e}")

def try_generate_pdf_with_weasyprint(html_text: str, pdf_path: pathlib.Path):
    """
    使用weasyprint生成PDF（备用方案）
    """
    try:
        from weasyprint import HTML, CSS
        from weasyprint.text.fonts import FontConfiguration
        
        font_config = FontConfiguration()
        html_doc = HTML(string=html_text)
        css = CSS(string='''
            @page { size: A4; margin: 20mm; }
            body { font-family: "Microsoft YaHei", "SimHei", sans-serif; }
            h1, h2, h3 { color: #333; }
            .risk-high { color: #d32f2f; }
            .risk-medium { color: #f57c00; }
            .risk-low { color: #388e3c; }
        ''', font_config=font_config)
        
        html_doc.write_pdf(pdf_path, stylesheets=[css], font_config=font_config)
        print(f"✅ PDF报告已生成(weasyprint): {pdf_path}")
        
    except ImportError:
        print("❌ weasyprint未安装，无法生成PDF")
        print("💡 请安装: pip install weasyprint 或 wkhtmltopdf")
    except Exception as e:
        print(f"❌ PDF生成失败(weasyprint): {e}")

def convert_pdf_to_long_image(pdf_path: pathlib.Path, output_path: pathlib.Path):
    """
    使用ImageMagick将PDF转换为长图
    """
    try:
        # 检查ImageMagick是否可用
        result = subprocess.run(["magick", "-version"], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ ImageMagick未安装，无法生成长图")
            print("💡 请安装ImageMagick: https://imagemagick.org/script/download.php")
            return
        
        # 使用ImageMagick转换PDF为长图
        cmd = [
            "magick",
            "-density", "160",  # 提高分辨率
            str(pdf_path),
            "-quality", "85",   # 压缩质量
            "-background", "white",
            "-alpha", "remove",
            str(output_path)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ 长图已生成: {output_path}")
        else:
            print(f"❌ 长图生成失败: {result.stderr}")
            
    except FileNotFoundError:
        print("❌ ImageMagick未找到，请确保已安装并添加到PATH")
    except Exception as e:
        print(f"❌ 长图生成失败: {e}")

def calculate_group_scores(results: List[dict], groups_config: dict) -> dict:
    """
    计算分组评分和总分，确保过期数据权重真正生效
    """
    # 过滤成功的指标
    successful = [r for r in results if r.get("status") == "success"]
    
    if not successful:
        return {"total_score": 50.0, "group_details": {}}
    
    group_scores = {}
    group_details = {}
    
    # 按分组计算
    for group_id, group_info in groups_config.items():
        group_indicators = [r for r in successful if r.get("group") == group_id]
        
        if group_indicators:
            # 计算组内加权平均分（支持global_weight和过期权重）
            scores = []
            weights = []
            freshness_weights = []
            
            for r in group_indicators:
                score = r["risk_score"]
                global_weight = r.get("global_weight", 0.0)
                include_in_score = r.get("include_in_score", True)
                is_stale = r.get("stale", False)
                
                # 过期数据权重衰减
                freshness_weight = 0.9 if is_stale else 1.0
                
                if include_in_score and global_weight > 0:
                    scores.append(score)
                    weights.append(global_weight)
                    freshness_weights.append(freshness_weight)
            
            if scores:
                # 应用过期权重
                effective_weights = [w * fw for w, fw in zip(weights, freshness_weights)]
                total_weight = sum(effective_weights)
                
                if total_weight > 0:
                    group_avg = sum(s * w for s, w in zip(scores, effective_weights)) / total_weight
                else:
                    group_avg = np.mean(scores)
            else:
                group_avg = 50.0  # 兜底
            
            group_scores[group_id] = group_avg
            
            # 记录分组详情
            group_weight = group_info.get("weight", 0.0)
            group_name = group_info.get("title", group_id)
            
            group_details[group_id] = {
                "name": group_name,
                "score": group_avg,
                "weight": group_weight,
                "count": len([r for r in group_indicators if r.get("include_in_score", True)]),
                "total_count": len(group_indicators),
                "contribution": group_avg * group_weight
            }
    
    # 计算总分（使用global_weight直接计算）
    has_global_weights = any(r.get("global_weight") is not None for r in successful)
    
    if has_global_weights:
        # 使用global_weight直接计算总分
        total_score = 0.0
        total_weight = 0.0
        
        for r in successful:
            if r.get("include_in_score", True):
                global_weight = r.get("global_weight", 0.0)
                is_stale = r.get("stale", False)
                freshness_weight = 0.9 if is_stale else 1.0
                
                effective_weight = global_weight * freshness_weight
                total_score += r["risk_score"] * effective_weight
                total_weight += effective_weight
        
        if total_weight > 0:
            total_score = total_score / total_weight
        else:
            total_score = 50.0  # 兜底
    else:
        # 传统方法：分组权重计算（确保权重归一化）
        group_weights = [groups_config[g]["weight"] for g in group_scores]
        total_group_weight = sum(group_weights)
        
        if total_group_weight > 0:
            # 归一化权重
            normalized_weights = [w / total_group_weight for w in group_weights]
            total_score = sum(group_scores[g] * normalized_weights[i] for i, g in enumerate(group_scores))
        else:
            total_score = 50.0  # 兜底
    
    return {
        "total_score": total_score,
        "group_scores": group_scores,
        "group_details": group_details
    }

def create_mobile_friendly_long_image(results: List[dict], crises: List[dict], output_path: pathlib.Path, groups_config: dict = None):
    """
    创建手机友好的长图版本 - 基于HTML报告内容
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.patches as patches
        from matplotlib.patches import FancyBboxPatch
        
        # 过滤成功的指标
        successful_results = [r for r in results if r.get("status") == "success"]
        
        if not successful_results:
            print("❌ 没有成功的指标数据，无法生成长图")
            return
        
        # 计算分组信息
        if groups_config:
            scoring_result = calculate_group_scores(results, groups_config)
            total_score = scoring_result["total_score"]
            group_details = scoring_result["group_details"]
        else:
            total_score = np.mean([r["risk_score"] for r in successful_results])
            group_details = {}
        
        # 创建长图 - 模拟HTML报告的布局
        fig_width = 14  # 更宽的布局
        fig_height = 8 + len(successful_results) * 0.4  # 根据指标数量调整高度
        
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))
        fig.patch.set_facecolor('white')
        ax.set_facecolor('white')
        
        y_pos = fig_height - 0.5
        
        # 标题
        ax.text(fig_width/2, y_pos, '🚨 宏观金融危机监察报告', 
               fontsize=24, fontweight='bold', ha='center', va='top')
        
        # 生成时间
        timestamp = datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')
        ax.text(fig_width/2, y_pos - 0.4, f'**生成时间**: {timestamp}', 
               fontsize=12, ha='center', va='top', color='#666')
        
        y_pos -= 1.0
        
        # 报告说明
        ax.text(0.5, y_pos, '## 📋 报告说明', fontsize=16, fontweight='bold', va='top')
        y_pos -= 0.3
        ax.text(0.5, y_pos, '本报告基于FRED宏观指标，将当前值与历史危机期间基准值比较，以评估风险。', 
               fontsize=11, va='top', wrap=True)
        y_pos -= 0.25
        ax.text(0.5, y_pos, '风险评分范围 0-100：50 为中性，越高越危险（除非指标设定为\'越低越危险\'）。', 
               fontsize=11, va='top', wrap=True)
        y_pos -= 0.25
        ax.text(0.5, y_pos, '采用分组加权评分：先计算各组平均分，再按权重合成总分。', 
               fontsize=11, va='top', wrap=True)
        
        y_pos -= 0.8
        
        # 总体风险概览
        ax.text(0.5, y_pos, '## 🎯 总体风险概览', fontsize=16, fontweight='bold', va='top')
        y_pos -= 0.3
        
        # 风险等级颜色
        if total_score >= 80:
            risk_color = '#d32f2f'
            risk_level = '🔴 高风险'
        elif total_score >= 60:
            risk_color = '#f57c00'
            risk_level = '🟡 中风险'
        elif total_score >= 40:
            risk_color = '#fbc02d'
            risk_level = '🟢 低风险'
        else:
            risk_color = '#388e3c'
            risk_level = '🔵 极低风险'
        
        ax.text(0.5, y_pos, f'- **加权风险总分**: {total_score:.1f}/100', fontsize=12, va='top')
        y_pos -= 0.25
        ax.text(0.5, y_pos, f'- **成功监控指标**: {len(successful_results)}/{len(results)}', fontsize=12, va='top')
        y_pos -= 0.25
        ax.text(0.5, y_pos, f'**总体风险等级**: {risk_level}', fontsize=14, fontweight='bold', color=risk_color, va='top')
        
        y_pos -= 0.8
        
        # 分组风险评分
        ax.text(0.5, y_pos, '### 📊 分组风险评分', fontsize=14, fontweight='bold', va='top')
        y_pos -= 0.3
        
        if group_details:
            for group_id, details in group_details.items():
                if details["count"] > 0:
                    stale_count = details["total_count"] - details["count"]
                    stale_info = f", 过期: {stale_count}" if stale_count > 0 else ""
                    ax.text(0.5, y_pos, f'- **{details["name"]}**: {details["score"]:.1f}/100 (权重: {details["weight"]:.0%}, 有效: {details["count"]}{stale_info}, 贡献: {details["contribution"]:.1f})', 
                           fontsize=11, va='top')
                    y_pos -= 0.2
        
        y_pos -= 0.5
        
        # 高风险指标
        high_risk_indicators = [r for r in successful_results if r["risk_score"] >= 80]
        if high_risk_indicators:
            ax.text(0.5, y_pos, '## 🔴 高风险指标', fontsize=16, fontweight='bold', va='top')
            y_pos -= 0.3
            
            for result in high_risk_indicators:
                indicator_name = result["indicator"]
                current_value = result["current_value"]
                risk_score = result["risk_score"]
                unit = result.get("unit", "")
                benchmark_value = result.get("benchmark_value", 0)
                compare_to = result.get("compare_to", "crisis_median")
                
                ax.text(0.5, y_pos, f'### {indicator_name}', fontsize=13, fontweight='bold', va='top')
                y_pos -= 0.25
                ax.text(0.5, y_pos, f'- **当前值**: {current_value:.4f} {unit}', fontsize=11, va='top')
                y_pos -= 0.2
                ax.text(0.5, y_pos, f'- **基准值**: {benchmark_value:.4f} ({compare_to})', fontsize=11, va='top')
                y_pos -= 0.2
                ax.text(0.5, y_pos, f'- **风险评分**: {risk_score:.1f}/100 (🔴 高风险)', fontsize=11, va='top')
                y_pos -= 0.2
                ax.text(0.5, y_pos, f'- **偏离度**: {current_value - benchmark_value:.4f}', fontsize=11, va='top')
                y_pos -= 0.2
                ax.text(0.5, y_pos, f'- **历史Z分数**: {result.get("zscore", 0):.2f}', fontsize=11, va='top')
                y_pos -= 0.2
                ax.text(0.5, y_pos, f'- **说明**: {result.get("explanation", "")}', fontsize=10, va='top', style='italic')
                y_pos -= 0.4
        
        # 设置坐标轴
        ax.set_xlim(0, fig_width)
        ax.set_ylim(0, fig_height)
        ax.axis('off')
        
        # 添加免责声明
        disclaimer = "数据真实性无法保证，所有公开数据是别人想给你看见的数据，处理方法不同结果不一，我处理数据的方法很有可能是错误的，请谨慎批判阅读"
        ax.text(fig_width/2, 0.3, disclaimer, 
               fontsize=9, ha='center', va='bottom', color='#666', 
               style='italic', wrap=True)
        
        plt.tight_layout()
        fig.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        print(f"✅ 手机友好长图已生成: {output_path}")
        
    except ImportError:
        print("❌ matplotlib未安装，无法生成长图")
        print("💡 请安装: pip install matplotlib")
    except Exception as e:
        print(f"❌ 长图生成失败: {e}")

# ----------------------------- 入口 -------------------------------
def run_data_sync():
    """运行数据同步程序"""
    try:
        print("🔄 开始数据同步...")
        print("⏳ 这可能需要1-3分钟，请耐心等待...")
        print("💡 提示：您可以离开去做其他事情，程序会自动完成")
        print("=" * 60)
        
        # 运行数据同步脚本
        import subprocess
        import threading
        import time
        
        # 创建进度指示器
        def progress_indicator():
            symbols = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
            i = 0
            while True:
                print(f"\r{symbols[i % len(symbols)]} 数据同步中... ({i//10 + 1}秒)", end="", flush=True)
                time.sleep(0.1)
                i += 1
        
        # 启动进度指示器
        progress_thread = threading.Thread(target=progress_indicator, daemon=True)
        progress_thread.start()
        
        # 运行同步
        result = subprocess.run([
            sys.executable, 
            str(BASE / "scripts" / "sync_fred_http.py")
        ], capture_output=True, text=True, cwd=str(BASE))
        
        # 停止进度指示器
        print(f"\r✅ 数据同步完成! ({result.returncode == 0 and '成功' or '有问题'})")
        
        if result.returncode == 0:
            print("📊 数据已更新到最新状态")
        else:
            print("⚠️ 数据同步遇到问题，但继续运行:")
            print(result.stderr)
            
    except Exception as e:
        print(f"\r❌ 数据同步失败: {e}")
        print("⚠️ 继续运行，使用现有数据")

if __name__ == "__main__":
    # 先运行数据下载
    run_data_sync()
    
    print("\n" + "=" * 80)
    print("🚨 开始生成危机预警报告...")
    print("=" * 80)
    
    # 生成报告
    res = generate_crisis_report()
    ok = len([r for r in res if r.get("status") == "success"])
    skipped = len([r for r in res if r.get("status") == "skipped"])
    total = len(res)
    print(f"\n🎉 危机预警报告生成完成！")
    print(f"   ✅ 成功处理: {ok} 个指标")
    if skipped > 0:
        print(f"   ⏭️ 跳过指标: {skipped} 个指标")
    print(f"   📊 总计指标: {total} 个")