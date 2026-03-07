#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
信号迟滞 (Signal Hysteresis / Debouncing) — 施密特触发器逻辑
- 进入危机模式：需连续 N 期（默认 3）满足触发条件
- 退出危机模式：需连续 M 期（默认 5）低于安全线
- 不对称：进入快、退出慢，避免噪音导致权重/警报频繁跳动
"""
from __future__ import annotations

import json
import pathlib
from typing import Any, Dict, List, Optional, Tuple

BASE = pathlib.Path(__file__).parent

# 视为「危机/非正常」的 verdict（需连续 N 次才确认进入）
CRITICAL_VERDICTS = frozenset({
    "ANTI_FIAT_REGIME",
    "FISCAL_DOMINANCE_ACTIVE",
    "K_SHAPED_RECESSION",
    "LIQUIDITY_STRESS",
    "JAPAN_CONTAGION_CRITICAL",
    "SOVEREIGN_LIQUIDITY_CRISIS",
})

NORMAL_VERDICT = "NORMAL"

# 默认：进入需 3 次连续触发，退出需 5 次连续 NORMAL
ENTER_CONSECUTIVE = 3
EXIT_CONSECUTIVE = 5
HISTORY_MAX_LEN = 30


def _state_file(output_dir: Optional[pathlib.Path] = None) -> pathlib.Path:
    d = output_dir or BASE / "outputs" / "crisis_monitor"
    d.mkdir(parents=True, exist_ok=True)
    return d / "regime_state.json"


def load_regime_state(output_dir: Optional[pathlib.Path] = None) -> Tuple[str, List[str]]:
    """加载持久化的当前状态与历史 verdict 列表。返回 (current_state, history)。"""
    path = _state_file(output_dir)
    if not path.exists():
        return NORMAL_VERDICT, []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        current = data.get("current_state", NORMAL_VERDICT)
        history = data.get("history", [])
        if not isinstance(history, list):
            history = []
        return current, history
    except Exception:
        return NORMAL_VERDICT, []


def save_regime_state(
    current_state: str,
    history: List[str],
    output_dir: Optional[pathlib.Path] = None,
) -> None:
    """持久化当前状态与历史。"""
    path = _state_file(output_dir)
    data = {"current_state": current_state, "history": list(history[-HISTORY_MAX_LEN:])}
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def update_regime_state(
    raw_verdict: str,
    current_state: str,
    history: List[str],
    enter_consecutive: int = ENTER_CONSECUTIVE,
    exit_consecutive: int = EXIT_CONSECUTIVE,
) -> Tuple[str, List[str], Dict[str, Any]]:
    """
    施密特触发器：根据原始 verdict 与历史，得到迟滞后的状态。
    - 进入危机：最近 enter_consecutive 次均为同一非 NORMAL verdict
    - 退出危机：当前为 NORMAL 且最近 exit_consecutive 次均为 NORMAL
    返回 (new_state, new_history, notes)。
    """
    # 将本次 raw 计入历史（按「次」计，一次运行一条）
    new_history = history + [raw_verdict]
    new_history = new_history[-HISTORY_MAX_LEN:]

    is_critical_raw = raw_verdict in CRITICAL_VERDICTS
    k_enter = enter_consecutive
    k_exit = exit_consecutive
    tail_enter = new_history[-k_enter:] if len(new_history) >= k_enter else new_history
    tail_exit = new_history[-k_exit:] if len(new_history) >= k_exit else new_history

    new_state = current_state
    notes: Dict[str, Any] = {
        "raw_verdict": raw_verdict,
        "previous_state": current_state,
        "hysteresis_applied": True,
    }

    # 当前已在某一危机状态
    if current_state in CRITICAL_VERDICTS:
        if all(v == NORMAL_VERDICT for v in tail_exit):
            new_state = NORMAL_VERDICT
            notes["transition"] = "exit_critical"
            notes["reason"] = f"连续 {k_exit} 次 NORMAL，退出危机"
        else:
            notes["transition"] = "hold"
            notes["reason"] = "仍处于危机，未满足退出条件"
    else:
        # 当前 NORMAL：检查是否满足进入条件（最近 k_enter 次均为同一非 NORMAL）
        if len(tail_enter) >= k_enter and all(v in CRITICAL_VERDICTS for v in tail_enter):
            # 取最近一次非 NORMAL 作为新状态
            new_state = tail_enter[-1]
            notes["transition"] = "enter_critical"
            notes["reason"] = f"连续 {k_enter} 次 {new_state}，进入危机"
        else:
            notes["transition"] = "hold"
            notes["reason"] = "未满足进入条件，保持 NORMAL"

    return new_state, new_history, notes


def get_stabilized_verdict(
    raw_verdict: str,
    output_dir: Optional[pathlib.Path] = None,
    enter_consecutive: int = ENTER_CONSECUTIVE,
    exit_consecutive: int = EXIT_CONSECUTIVE,
) -> Tuple[str, Dict[str, Any]]:
    """
    对外接口：读入本次 raw_verdict，加载历史、应用迟滞、写回并返回稳定化 verdict。
    返回 (stabilized_verdict, notes)。
    """
    current_state, history = load_regime_state(output_dir)
    new_state, new_history, notes = update_regime_state(
        raw_verdict, current_state, history, enter_consecutive, exit_consecutive
    )
    save_regime_state(new_state, new_history, output_dir)
    notes["stabilized_verdict"] = new_state
    return new_state, notes
