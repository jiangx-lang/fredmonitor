#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日运行并发送综合结论邮件（SendGrid）。
依赖环境变量：SENDGRID_API_KEY
"""
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
from datetime import datetime

import yaml


BASE = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

from notify.sendgrid_mail import send_alert_email

if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())


def _load_email_config() -> dict:
    cfg_path = BASE / "config" / "email_settings.yaml"
    if not cfg_path.exists():
        return {}
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}


def _load_latest_json(output_dir: pathlib.Path) -> dict | None:
    latest_json = output_dir / "crisis_report_latest.json"
    if latest_json.exists():
        try:
            return json.loads(latest_json.read_text(encoding="utf-8"))
        except Exception:
            pass
    candidates = sorted(output_dir.glob("crisis_report_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for candidate in candidates:
        try:
            return json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
    return None


def _build_email_body(report: dict, include_paths: bool) -> str:
    summary = report.get("executive_summary", {})
    text = summary.get("text") or report.get("summary", {}).get("executive_summary", {}).get("text", "")
    classification = summary.get("classification") or report.get("summary", {}).get("executive_summary", {}).get("classification", "")
    timestamp = report.get("timestamp")
    lines = []
    lines.append(f"时间: {timestamp}")
    if classification:
        lines.append(f"结论: {classification}")
    if text:
        lines.append("")
        lines.append(text)
    if include_paths:
        output_dir = BASE / "outputs" / "crisis_monitor"
        lines.append("")
        lines.append("本地报告路径:")
        lines.append(str(output_dir / "crisis_report_latest.md"))
        lines.append(str(output_dir / "crisis_report_latest.html"))
        lines.append(str(output_dir / "crisis_report_latest.json"))
    return "\n".join(lines)


def main() -> int:
    api_key = os.getenv("SENDGRID_API_KEY")
    if not api_key:
        print("ERROR: 环境变量 SENDGRID_API_KEY 未设置")
        return 1

    run_cmd = [sys.executable, str(BASE / "crisis_monitor_v2.py")]
    print("INFO: 运行日报程序...")
    subprocess.run(run_cmd, cwd=str(BASE), check=False)

    output_dir = BASE / "outputs" / "crisis_monitor"
    report = _load_latest_json(output_dir)
    if not report:
        print("ERROR: 未找到最新报告 JSON")
        return 1

    cfg = _load_email_config()
    sender = cfg.get("sender")
    recipients = cfg.get("recipients", [])
    subject_prefix = cfg.get("subject_prefix", "[Crisis Monitor]")
    include_paths = bool(cfg.get("include_paths", True))

    summary = report.get("executive_summary", {})
    classification = summary.get("classification", "")
    today = datetime.now().strftime("%Y-%m-%d")
    subject = f"{subject_prefix} {classification} {today}".strip()

    body = _build_email_body(report, include_paths)

    if not sender or not recipients:
        print("ERROR: 邮件配置缺失 sender / recipients")
        return 1

    if send_alert_email(api_key, subject, body, sender, recipients):
        print("INFO: 邮件发送完成")
        return 0
    print("ERROR: 邮件发送失败")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
