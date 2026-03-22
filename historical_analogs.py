# -*- coding: utf-8 -*-
"""
历史情景对照：静态情景库与匹配逻辑。
用于报告尾部《📚 历史情景对照与普通人理解指南》，不修改任何评分逻辑。

维护约定：
- 新增情景时在 HISTORICAL_ANALOGS 补一条，并标注 analog_confidence（high/medium/low）。
- 若需非开发编辑，可将 HISTORICAL_ANALOGS 迁至 config/historical_analogs.yaml 再在此处加载。
"""
from __future__ import annotations

from typing import Any

# 静态历史情景库（参照模板，非精确回测；允许部分匹配；必须标注 analog_confidence）
HISTORICAL_ANALOGS = {
    "ANTI_FIAT_1979": {
        "label": "1979 年滞胀 / 反法币阶段",
        "trigger_signals": ["gold_regime_critical", "anti_fiat", "inflation_high"],
        "time_window_text": "数周到数月",
        "common_market_pattern": "黄金走强、长债承压、股票先估值压缩后分化",
        "escalation_condition": "若通胀预期、能源与政策约束同时持续升级",
        "plain_english": "这类环境通常不是一天内崩盘，而是资产定价开始偏向硬资产、远离长久期资产。",
        "analog_confidence": "medium",
    },
    "JAPAN_CARRY_2007": {
        "label": "2007 年日元套利回撤前夕",
        "trigger_signals": ["japan_contagion_critical", "credit_watch", "vix_elevated"],
        "time_window_text": "数周内",
        "common_market_pattern": "先出现局部去杠杆，再扩散为风险资产普遍承压",
        "escalation_condition": "若信用利差与波动率同步上行",
        "plain_english": "表面平静时，局部资金链收紧也可能先从边缘资产开始出问题。",
        "analog_confidence": "low",
    },
    "KDOLLARIZATION_2011": {
        "label": "2011 年政策冲突 / 去美元化阶段",
        "trigger_signals": ["de_dollarization_alert", "gold_regime_critical", "policy_conflict"],
        "time_window_text": "1–3 个月",
        "common_market_pattern": "黄金偏强、长债波动上升、股票经历估值压缩",
        "escalation_condition": "若政策协调失败、信用和波动率进一步共振",
        "plain_english": "这类阶段通常先表现为不确定性上升，而不是立刻全面崩盘。",
        "analog_confidence": "medium",
    },
    "KSHAPE_RECESSION_2001": {
        "label": "2001 年科技股出清 / 白领衰退",
        "trigger_signals": ["k_shaped_warning", "consumer_confidence_low", "credit_watch"],
        "time_window_text": "数月",
        "common_market_pattern": "成长股和高估值资产先受压，随后才传导至更广泛资产",
        "escalation_condition": "若就业、消费、信用持续共振走弱",
        "plain_english": "市场常先从高估值板块出问题，再慢慢扩散到实体经济。",
        "analog_confidence": "medium",
    },
    "PRIVATE_CREDIT_2018": {
        "label": "2018 年流动性收紧 / 企业融资压力",
        "trigger_signals": ["private_credit_watch", "hy_oas_rising", "vix_elevated"],
        "time_window_text": "2–8 周",
        "common_market_pattern": "先是高风险信用资产与高弹性股票承压，随后决定是否扩散",
        "escalation_condition": "若高收益利差继续走阔并伴随金融压力抬升",
        "plain_english": "这类阶段常先是边缘资产变弱，不一定立刻变成全面危机。",
        "analog_confidence": "high",
    },
    "ALL_CLEAR_2019": {
        "label": "2019 年预防性宽松 / 软着陆阶段",
        "trigger_signals": [],
        "time_window_text": "数月到更久",
        "common_market_pattern": "风险资产维持偏强，偶有回撤但不构成系统性风险",
        "escalation_condition": "若新的信用、流动性或通胀链条触发",
        "plain_english": "信号偏平稳时，市场更像正常波动，而不是危机准备阶段。",
        "analog_confidence": "high",
    },
}


def match_historical_analogs(report_signals: dict) -> list:
    """
    输入当前报告的核心信号状态，返回最多 2 个最相似的历史情景，按匹配度排序。
    每个元素: {"key": str, "label": str, "match_score": float, "exact_or_partial": "exact"|"partial", "analog": dict}
    """
    active: list[str] = []
    # 从 report_signals 提取可用的信号标签（与 HISTORICAL_ANALOGS.trigger_signals 对应）
    gold_regime = (report_signals.get("gold_regime") or "").upper()
    japan_contagion = (report_signals.get("japan_contagion") or "").upper()
    de_dollarization = (report_signals.get("de_dollarization") or "").upper()
    k_shaped = (report_signals.get("k_shaped") or "").upper()
    private_credit = (report_signals.get("private_credit") or "").upper()
    resonance = (report_signals.get("resonance") or "").upper()
    vix_elevated = report_signals.get("vix_elevated", False)

    if gold_regime == "CRITICAL":
        active.extend(["gold_regime_critical", "anti_fiat"])
    if japan_contagion == "CRITICAL":
        active.append("japan_contagion_critical")
    if de_dollarization in ("ALERT", "CRITICAL"):
        active.append("de_dollarization_alert")
    if gold_regime == "CRITICAL" and ("inflation" in str(report_signals.get("geopolitics", "")).lower() or report_signals.get("inflation_high")):
        active.append("inflation_high")
    if k_shaped in ("WARNING", "ALERT", "CRITICAL"):
        active.append("k_shaped_warning")
    if private_credit in ("WATCH", "ALERT", "CRITICAL"):
        active.append("private_credit_watch")
    if vix_elevated or (resonance and resonance != "OFF"):
        active.append("vix_elevated")
    if report_signals.get("credit_watch"):
        active.append("credit_watch")
    if report_signals.get("policy_conflict"):
        active.append("policy_conflict")
    if report_signals.get("consumer_confidence_low"):
        active.append("consumer_confidence_low")
    if report_signals.get("hy_oas_rising"):
        active.append("hy_oas_rising")

    scored: list[tuple[str, float, str, dict]] = []
    for key, analog in HISTORICAL_ANALOGS.items():
        triggers = set(analog.get("trigger_signals") or [])
        if not triggers:  # ALL_CLEAR
            if not active:
                scored.append((key, 1.0, "exact", analog))
            else:
                scored.append((key, 0.2, "partial", analog))
            continue
        overlap = len(triggers & set(active))
        total = len(triggers)
        if total == 0:
            continue
        ratio = overlap / total
        if overlap == 0:
            if not active:
                scored.append((key, 0.5, "partial", analog))
            continue
        exact = ratio >= 0.8 and overlap >= 1
        score = min(1.0, ratio + (0.2 if exact else 0))
        scored.append((key, score, "exact" if exact else "partial", analog))

    # 优先级覆盖：按 prompt 规则
    if gold_regime == "CRITICAL" and not any(s[0] in ("ANTI_FIAT_1979", "KDOLLARIZATION_2011") for s in scored):
        for key in ("ANTI_FIAT_1979", "KDOLLARIZATION_2011"):
            if key in HISTORICAL_ANALOGS:
                scored.append((key, 0.7, "partial", HISTORICAL_ANALOGS[key]))
    if japan_contagion == "CRITICAL" and not any(s[0] == "JAPAN_CARRY_2007" for s in scored):
        scored.append(("JAPAN_CARRY_2007", 0.75, "partial", HISTORICAL_ANALOGS["JAPAN_CARRY_2007"]))
    if de_dollarization in ("ALERT", "CRITICAL") and not any(s[0] == "KDOLLARIZATION_2011" for s in scored):
        scored.append(("KDOLLARIZATION_2011", 0.7, "partial", HISTORICAL_ANALOGS["KDOLLARIZATION_2011"]))
    if k_shaped in ("WARNING", "ALERT", "CRITICAL") and not any(s[0] == "KSHAPE_RECESSION_2001" for s in scored):
        scored.append(("KSHAPE_RECESSION_2001", 0.65, "partial", HISTORICAL_ANALOGS["KSHAPE_RECESSION_2001"]))
    if private_credit in ("WATCH", "ALERT", "CRITICAL") and not any(s[0] == "PRIVATE_CREDIT_2018" for s in scored):
        scored.append(("PRIVATE_CREDIT_2018", 0.7, "partial", HISTORICAL_ANALOGS["PRIVATE_CREDIT_2018"]))
    if not scored or (len(active) == 0 and not any(s[0] == "ALL_CLEAR_2019" for s in scored)):
        scored.append(("ALL_CLEAR_2019", 0.6, "partial", HISTORICAL_ANALOGS["ALL_CLEAR_2019"]))

    scored.sort(key=lambda x: -x[1])
    out = []
    for key, score, exact_or_partial, analog in scored[:4]:
        out.append({
            "key": key,
            "label": analog.get("label", key),
            "match_score": round(score, 2),
            "exact_or_partial": exact_or_partial,
            "analog": analog,
        })
    return out[:2]


def get_confidence_label(analog: dict) -> str:
    c = (analog.get("analog_confidence") or "medium").upper()
    if c == "HIGH":
        return "HIGH"
    if c == "LOW":
        return "LOW"
    return "MEDIUM"


def get_confidence_from_matches(matches: list) -> str:
    """取当前匹配列表的匹配置信度：有 exact 且 high→HIGH，否则取主情景的 analog_confidence。"""
    if not matches:
        return "LOW"
    primary = matches[0]
    a = primary.get("analog", {})
    c = (a.get("analog_confidence") or "medium").upper()
    if primary.get("exact_or_partial") == "exact" and c == "HIGH":
        return "HIGH"
    if c == "LOW":
        return "LOW"
    return "MEDIUM"
