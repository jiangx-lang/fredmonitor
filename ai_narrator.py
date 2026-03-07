#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LLM 叙事生成层 (AI Narrator Layer)
- 输入：crisis_report_latest.json（分数、体制、Top Drivers）
- 支持：OpenAI / Gemini / 阿里通义千问 (DashScope)
- 调用 LLM API 生成约 200 字的定性市场评论；解释「为什么」并给出大类资产建议
- API Key 请用环境变量（如 DASHSCOPE_API_KEY）或 --key 传入，勿写入代码
"""
from __future__ import annotations

import json
import os
import pathlib
from typing import Any, Dict, Optional

BASE = pathlib.Path(__file__).parent

DEFAULT_PROMPT_PREFIX = """你是一位华尔街首席宏观策略师。根据以下危机监控系统输出，用简洁、专业的语言写一段约 200 字的「每日宏观简报」。
要求：
1. 解释当前体制/风险分数的「为什么」（例如为什么是反法币体制、熊市陡峭化等）。
2. 给出 1–2 条具体的大类资产配置建议（股票/黄金/美债/现金）。
3. 语言：中文，专业但易懂，不要罗列数字。
"""


def _load_report(json_path: pathlib.Path) -> Optional[Dict[str, Any]]:
    if not json_path.exists():
        return None
    try:
        return json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _build_prompt(data: Dict[str, Any], max_chars: int = 1200) -> str:
    """从 report JSON 抽取关键字段，拼成给 LLM 的 prompt 正文。"""
    parts = []
    if data.get("total_score") is not None:
        parts.append(f"总风险分: {data['total_score']:.1f}/100")
    if data.get("risk_level"):
        parts.append(f"风险等级: {data['risk_level']}")
    regime = data.get("regime", {})
    if isinstance(regime, dict):
        v = regime.get("verdict", regime.get("raw_verdict", ""))
        if v:
            parts.append(f"当前体制 (Regime): {v}")
        expl = regime.get("explanations", [])
        if expl:
            parts.append("体制说明: " + "; ".join(expl[:5]))
    summary = data.get("summary", {})
    if summary.get("executive_summary"):
        parts.append("执行摘要: " + str(summary["executive_summary"])[:500])
    top = summary.get("top_drivers", {}) or data.get("summary", {}).get("top_drivers", {})
    if top:
        level = top.get("top_level_drivers", [])[:3]
        change = top.get("top_change_drivers", [])[:3]
        if level:
            parts.append("主要风险驱动(水平): " + ", ".join(f"{d.get('name', d.get('series_id'))}" for d in level))
        if change:
            parts.append("主要风险驱动(变化): " + ", ".join(f"{d.get('name', d.get('series_id'))}" for d in change))
    allocation = data.get("allocation", {})
    if allocation:
        parts.append("系统建议仓位: " + json.dumps(allocation, ensure_ascii=False))
    body = "\n".join(parts)
    if len(body) > max_chars:
        body = body[: max_chars - 20] + "..."
    return body


def _call_openai(prompt: str, api_key: str, model: str = "gpt-4o-mini") -> Optional[str]:
    try:
        import openai
        client = openai.OpenAI(api_key=api_key)
        r = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a senior macro strategist. Reply in Chinese (Simplified)."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=500,
        )
        if r.choices and r.choices[0].message.content:
            return r.choices[0].message.content.strip()
    except Exception:
        pass
    return None


def _call_gemini(prompt: str, api_key: str, model: str = "gemini-1.5-flash") -> Optional[str]:
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        m = genai.GenerativeModel(model)
        r = m.generate_content(prompt)
        if r and r.text:
            return r.text.strip()
    except Exception:
        pass
    return None


def _call_tongyi(prompt: str, api_key: str, model: str = "qwen-plus") -> Optional[str]:
    """阿里通义千问 (DashScope)。Key 从 https://bailian.console.aliyun.com/#/api-key 获取。"""
    try:
        import dashscope
        dashscope.api_key = api_key
        messages = [
            {"role": "system", "content": "你是一位资深宏观策略师。请用简体中文回答。"},
            {"role": "user", "content": prompt},
        ]
        r = dashscope.Generation.call(
            model=model,
            messages=messages,
            max_tokens=500,
            result_format="message",
        )
        if r.status_code == 200 and r.output and r.output.get("choices"):
            text = r.output["choices"][0].get("message", {}).get("content", "").strip()
            if text:
                return text
    except Exception:
        pass
    return None


def generate_narrative_from_data(
    data: Dict[str, Any],
    prompt_prefix: str = DEFAULT_PROMPT_PREFIX,
    provider: str = "auto",
    api_key: Optional[str] = None,
) -> Optional[str]:
    """
    从内存中的 report 字典调用 LLM 生成定性评论（供报告流水线内调用）。
    """
    body = _build_prompt(data)
    full_prompt = prompt_prefix.strip() + "\n\n---\n\n" + body
    key = (
        api_key
        or os.environ.get("DASHSCOPE_API_KEY")
        or os.environ.get("TONGYI_API_KEY")
        or os.environ.get("OPENAI_API_KEY")
        or os.environ.get("GEMINI_API_KEY")
    )
    if not key:
        return None
    if provider == "tongyi":
        return _call_tongyi(full_prompt, key)
    if provider == "openai":
        return _call_openai(full_prompt, api_key or os.environ.get("OPENAI_API_KEY", ""))
    if provider == "gemini":
        return _call_gemini(full_prompt, api_key or os.environ.get("GEMINI_API_KEY", ""))
    if provider == "auto":
        if os.environ.get("DASHSCOPE_API_KEY") or os.environ.get("TONGYI_API_KEY"):
            return _call_tongyi(full_prompt, key)
        if os.environ.get("OPENAI_API_KEY"):
            return _call_openai(full_prompt, key)
        if os.environ.get("GEMINI_API_KEY"):
            return _call_gemini(full_prompt, key)
        if key.startswith("sk-"):
            return _call_tongyi(full_prompt, key)
        return _call_gemini(full_prompt, key)
    return None


def generate_narrative(
    json_path: Optional[pathlib.Path] = None,
    prompt_prefix: str = DEFAULT_PROMPT_PREFIX,
    provider: str = "auto",
    api_key: Optional[str] = None,
) -> Optional[str]:
    """
    读取 crisis_report JSON，调用 LLM 生成定性评论。
    - provider: "openai" | "gemini" | "tongyi" | "auto"
      auto 时优先: DASHSCOPE_API_KEY / TONGYI_API_KEY（通义千问）> OPENAI_API_KEY > GEMINI_API_KEY
    - api_key: 若未传则从环境变量读取（勿在代码中硬编码 Key）。
    返回生成的评论文本，失败返回 None。
    """
    path = json_path or BASE / "outputs" / "crisis_monitor" / "crisis_report_latest.json"
    data = _load_report(path)
    if not data:
        return None
    return generate_narrative_from_data(data, prompt_prefix=prompt_prefix, provider=provider, api_key=api_key)


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="AI Narrator: 从 crisis_report JSON 生成 LLM 市场评论")
    p.add_argument("--json", type=pathlib.Path, default=None, help="crisis_report JSON 路径")
    p.add_argument("--provider", choices=("auto", "openai", "gemini", "tongyi"), default="auto",
                    help="通义千问选 tongyi，需 DASHSCOPE_API_KEY 或 TONGYI_API_KEY")
    p.add_argument("--key", type=str, default=None,
                    help="API Key（也可用 DASHSCOPE_API_KEY / OPENAI_API_KEY / GEMINI_API_KEY）")
    args = p.parse_args()
    out = generate_narrative(json_path=args.json, provider=args.provider, api_key=args.key)
    if out:
        print(out)
    else:
        print("（未生成：请设置 DASHSCOPE_API_KEY / OPENAI_API_KEY / GEMINI_API_KEY 或传入 --key）")


if __name__ == "__main__":
    main()
