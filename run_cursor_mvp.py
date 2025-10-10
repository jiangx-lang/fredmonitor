# run_cursor_mvp.py
import json, math
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import yaml

ROOT = Path(__file__).resolve().parent
CFG_IND = ROOT/"config/indicators.yaml"
CFG_SCORE = ROOT/"config/scoring.yaml"
CFG_CRISIS = ROOT/"config/crisis_periods.yaml"
SERIES_DIR = ROOT/"data/series"
FIG_DIR = ROOT/"figures"
OUT_JSON = ROOT/"outputs/latest.json"
OUT_MD   = ROOT/"outputs/latest.md"

def yload(p): 
    with open(p, "r", encoding="utf-8") as f: 
        return yaml.safe_load(f)

def load_series(sid:str)->pd.Series:
    fp = SERIES_DIR/f"{sid}.csv"
    if not fp.exists(): return None
    df = pd.read_csv(fp)
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    s = pd.Series(df["value"].values, index=df["date"])
    s = s.replace([np.inf,-np.inf], np.nan).dropna()
    return s

def infer_freq(meta)->str:
    return (meta.get("freq") or "M").upper()

def yoy_pct(s:pd.Series, freq:str)->pd.Series:
    lag = {"M":12, "Q":4, "W":52, "D":252}.get(freq, 12)
    return s.pct_change(lag)*100

def apply_transform(s:pd.Series, meta)->pd.Series:
    t = meta.get("transform","level")
    freq = infer_freq(meta)
    if t=="yoy_pct":
        out = yoy_pct(s, freq)
    elif t=="diff":
        out = s.diff()
    else:
        out = s.copy()
    return out.dropna()

def build_masks(idx:pd.DatetimeIndex, crisis_cfg)->pd.Series:
    # 返回一个布尔序列：True=危机期
    mask = pd.Series(False, index=idx)
    for item in crisis_cfg.get("periods", []):
        st = pd.to_datetime(item["start"]); en = pd.to_datetime(item["end"])
        mask.loc[(mask.index>=st)&(mask.index<=en)] = True
    return mask

def qtl(a:pd.Series, q:float)->float:
    return float(np.nanquantile(a.values, q))

def pick_subset(s:pd.Series, mask:pd.Series, key:str)->pd.Series:
    # key: crisis_* / noncrisis_*
    if key.startswith("crisis_"):
        return s[mask.reindex(s.index, fill_value=False)]
    if key.startswith("noncrisis_"):
        return s[~mask.reindex(s.index, fill_value=False)]
    return s

def compute_benchmark(s:pd.Series, mask:pd.Series, compare_to:str)->float:
    key = compare_to.lower()
    sub = pick_subset(s, mask, key)
    # 样本不足则回退全样本
    if sub.dropna().shape[0] < 24:
        sub = s
        fallback = True
    else:
        fallback = False
    if "median" in key:
        val = qtl(sub, 0.5)
    elif "p" in key:
        p = float(key.split("p")[-1])/100.0
        val = qtl(sub, p)
    else:
        val = qtl(sub, 0.5)
    return float(val)

def ecdf_percentile(s:pd.Series, x:float)->float:
    arr = s.dropna().values
    if arr.size==0: return 0.5
    rank = (arr<=x).sum()
    return rank/arr.size

def interp_anchors(p:float, anchors:list)->float:
    # anchors: [[q,score], ...] q in [0,1]
    anchors = sorted(anchors, key=lambda z: z[0])
    if p<=anchors[0][0]: return anchors[0][1]
    if p>=anchors[-1][0]: return anchors[-1][1]
    for (q0,s0),(q1,s1) in zip(anchors[:-1], anchors[1:]):
        if q0<=p<=q1:
            w = (p-q0)/(q1-q0) if q1>q0 else 0.0
            return s0 + w*(s1-s0)
    return anchors[-1][1]

def staleness_penalty(meta, last_date:pd.Timestamp, score_cfg)->float:
    if last_date is None: return 0.9
    now = pd.Timestamp(datetime.utcnow().date())
    days = (now - last_date.normalize()).days
    f = (meta.get("freq") or "M").upper()
    th = score_cfg["staleness_penalty"]
    if f=="D": base = th["daily_days"]
    elif f=="W": base = th["weekly_days"]
    elif f=="Q": base = th["quarterly_days"]
    else: base = th["monthly_days"]
    if days <= base: return 1.0
    k = math.floor((days - base)/base) + 1
    return th["factor"]**k

def color_of(x, thr):  # for MD
    if x>=thr["high"]: return "🔴"
    if x>=thr["mid"]:  return "🟡"
    if x>=thr["low"]:  return "🟢"
    return "🔵"

def explain(meta)->str:
    tr = meta.get("transform","level")
    dirn = "越高越险" if meta.get("higher_is_risk", True) else "越低越险"
    return f"口径：{tr}；方向：{dirn}；基准：{meta.get('compare_to')}。"

def main():
    ind_cfg  = yload(CFG_IND)
    score_cfg= yload(CFG_SCORE)
    crisis   = yload(CFG_CRISIS)
    anchors  = score_cfg["quantile_tail"]["anchors"]
    thr      = score_cfg["thresholds"]
    bmin,bmax= score_cfg["bounds"]

    records=[]; missing=[]
    # 先建立统一时间索引集合（按各自序列处理）
    for meta in ind_cfg["indicators"]:
        sid = meta["id"]
        role = meta.get("role","score")
        s = load_series(sid)
        if s is None:
            if role!="monitor":
                missing.append(sid)
            continue
        s_tr = apply_transform(s, meta)
        if s_tr.empty:
            missing.append(sid); continue

        mask = build_masks(s_tr.index, crisis)  # 与变换后频率对齐
        bench = compute_benchmark(s_tr, mask, meta.get("compare_to","noncrisis_median"))

        # 分位概率
        p = ecdf_percentile(s_tr, s_tr.iloc[-1])
        if not meta.get("higher_is_risk", True):
            p = 1 - p
        # 双尾可选：若 tail: both
        if meta.get("tail","single")=="both":
            p = max(p, 1-p)

        raw_score = interp_anchors(p, anchors)
        # 时效惩罚
        penalty = staleness_penalty(meta, s_tr.index[-1], score_cfg)
        score = max(bmin, min(bmax, raw_score*penalty))

        fig = str((FIG_DIR/f"{sid}_latest.png").relative_to(ROOT)) if (FIG_DIR/f"{sid}_latest.png").exists() else ""

        records.append({
            "id": sid,
            "name": meta.get("name", sid),
            "group": meta.get("group","misc"),
            "role": meta.get("role","score"),
            "current_value": float(s_tr.iloc[-1]),
            "benchmark_value": float(bench),
            "risk_score": round(float(score),2),
            "higher_is_risk": bool(meta.get("higher_is_risk", True)),
            "compare_to": meta.get("compare_to"),
            "transform": meta.get("transform","level"),
            "freq": infer_freq(meta),
            "last_date": str(pd.to_datetime(s_tr.index[-1]).date()),
            "weight": float(meta.get("weight",0.0)),
            "figure": fig,
            "explain": explain(meta)
        })

    # 归一权重（仅 role=score 且非缺失）
    df = pd.DataFrame(records)
    usable = df.query("role=='score'")
    wsum = usable["weight"].sum()
    if wsum>0:
        df.loc[usable.index, "weight_eff"] = usable["weight"]/wsum
    else:
        df["weight_eff"] = 0.0

    # 组内均值、组间加权
    grp = df.query("role=='score'").groupby("group")["risk_score"].mean().sort_values(ascending=False)
    total = (df.query("role=='score'")["risk_score"]*df.query("role=='score'")["weight_eff"]).sum()

    out = {
        "timestamp": pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "total_score": round(float(total),2),
        "risk_level": color_of(total, thr),
        "groups": grp.to_dict(),
        "missing_series": missing,
        "indicators": df.to_dict(orient="records")
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON,"w",encoding="utf-8") as f: json.dump(out,f,ensure_ascii=False,indent=2)

    # 写 MD
    lines=[]
    lines+= [f"# FRED 宏观危机预警（Cursor-MVP）  \n**生成**：{out['timestamp']}"]
    lines+= [f"\n## 总览\n- 综合分：**{total:.1f}/100** {color_of(total,thr)}\n- 缺数据指标：{', '.join(missing) if missing else '无'}\n"]
    lines+= ["## 分组分数（组内均值）"]
    for g,sc in grp.items():
        lines.append(f"- **{g}**：{sc:.1f} {color_of(sc,thr)}")
    lines+= ["\n---\n## 详细指标"]
    for r in df.sort_values("risk_score", ascending=False).itertuples():
        tag = "[监测]" if r.role!="score" else ""
        lines.append(f"\n### {r.name} {tag}\n- 当前值：{r.current_value:.4g}（{r.last_date}）"
                     f"\n- 基准：{r.benchmark_value:.4g}（{r.compare_to}）"
                     f"\n- 口径：{r.transform}；方向：{'↑险' if r.higher_is_risk else '↓险'}；权重(有效)：{(r.weight if r.role=='score' else 0):.3f}"
                     f"\n- 评分：**{r.risk_score:.1f}** {color_of(r.risk_score,thr)}"
                     f"\n- 说明：{r.explain}")
        if r.figure:
            lines.append(f"\n![{r.id}]({r.figure})")
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ 写入 {OUT_JSON}\n✅ 写入 {OUT_MD}")

if __name__=="__main__":
    main()
