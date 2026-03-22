#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日运行并发送综合结论邮件（QQ 邮箱 SMTP）。
配置：项目根 .env 或 config.env 中 QQ_EMAIL_USER、QQ_EMAIL_PASSWORD。
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

from notify.sendgrid_mail import (
    send_alert_email,
    qq_credentials_configured,
    qq_credentials_diag,
)

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
    if not qq_credentials_configured():
        print("ERROR: QQ 邮箱未配置完整。请在以下任一文件中设置 QQ_EMAIL_USER、QQ_EMAIL_PASSWORD（授权码）：")
        print("  - 项目根 macrolab.env / .env / config.env")
        print("  - D:\\MF\\.env 或 D:\\MF\\config.env（可与基金长图邮件共用）")
        print("--- 诊断 ---")
        print(qq_credentials_diag())
        return 1

    send_only = os.getenv("CRISIS_MONITOR_SEND_ONLY", "").strip().lower() in ("1", "true", "yes")
    if not send_only:
        run_cmd = [sys.executable, str(BASE / "crisis_monitor_v2.py")]
        print("INFO: 运行日报程序（定时发送不调用阿里 API 叙事）...")
        env = os.environ.copy()
        env["CRISIS_MONITOR_SKIP_AI_NARRATOR"] = "1"
        subprocess.run(run_cmd, cwd=str(BASE), env=env, check=False)
    else:
        print("INFO: 仅发邮件模式（CRISIS_MONITOR_SEND_ONLY=1），使用已有报告与 MD 附件")

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

    # 附件：最新 MD 报告（文件不大，便于收件人阅读完整内容）
    md_report = output_dir / "crisis_report_latest.md"
    attachments = []
    if md_report.exists():
        attachments.append((str(md_report), "crisis_report_latest.md"))
    else:
        # 尝试按时间戳找最新 MD
        for p in sorted(output_dir.glob("crisis_report_*.md"), key=lambda x: x.stat().st_mtime, reverse=True):
            attachments.append((str(p), p.name))
            break

    ok = send_alert_email(
        subject, body, sender, recipients, attachments=attachments if attachments else None
    )
    if ok:
        print("INFO: 邮件发送完成（QQ SMTP）" + ("（含 MD 附件）" if attachments else ""))
        return 0
    print("ERROR: 邮件发送失败（QQ SMTP）。请检查授权码是否过期、网络与 smtp.qq.com:465。")
    print(qq_credentials_diag())
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
