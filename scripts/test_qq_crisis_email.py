#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""发送一封测试邮件，验证 QQ SMTP 与 email_settings.yaml 是否配置正确。"""
from __future__ import annotations

import pathlib
import sys

BASE = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

import yaml
from notify.sendgrid_mail import send_alert_email, qq_credentials_diag, qq_credentials_configured


def main() -> int:
    print(qq_credentials_diag())
    print()
    if not qq_credentials_configured():
        print("凭据不完整，无法发送。请配置 QQ_EMAIL_USER / QQ_EMAIL_PASSWORD。")
        return 1
    cfg_path = BASE / "config" / "email_settings.yaml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) if cfg_path.exists() else {}
    sender = cfg.get("sender", "")
    recipients = cfg.get("recipients") or []
    if not recipients:
        print("email_settings.yaml 中无 recipients")
        return 1
    prefix = cfg.get("subject_prefix", "[Crisis Monitor]")
    ok = send_alert_email(
        f"{prefix} 测试邮件",
        "这是一封危机监控 QQ SMTP 连通性测试。若收到说明定时任务发信应可恢复。\n\n"
        "若未收到：检查 QQ 授权码、垃圾箱、以及 logs/daily_report_*.log。",
        sender,
        recipients,
        attachments=None,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
