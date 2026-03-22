#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QQ 邮箱 SMTP 邮件发送（与 D:\\MF\\send_qd_image_email.py 同一套配置习惯）。
凭据查找顺序（先命中先用）：
  1. 项目根 macrolab.env、.env、config.env
  2. D:\\MF\\.env、D:\\MF\\config.env（与基金长图邮件共用授权码时可只配一处）
  3. 环境变量 QQ_EMAIL_USER、QQ_EMAIL_PASSWORD

QQ 邮箱需在网页端开启 SMTP 并使用「授权码」作为 QQ_EMAIL_PASSWORD。
"""
import logging
import os
import smtplib
import ssl
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import List, Optional, Sequence, Tuple, Union

logger = logging.getLogger(__name__)

QQ_SMTP_HOST = "smtp.qq.com"
QQ_SMTP_PORT = 465

_BASE = Path(__file__).resolve().parents[1]
_MF_BASE = Path(r"D:\MF")


def _env_file_candidates() -> List[Path]:
    """与危机监控、MF 项目一致的配置文件列表（顺序 = 优先级）。"""
    return [
        _BASE / "macrolab.env",
        _BASE / ".env",
        _BASE / "config.env",
        _MF_BASE / ".env",
        _MF_BASE / "config.env",
    ]


def _read_key_from_file(path: Path, key_name: str) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8-sig")
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() == key_name:
                return (v.strip().strip('"').strip("'").strip() or "").replace("\ufeff", "").strip()
    except Exception:
        pass
    return ""


def _get_qq_user() -> str:
    for p in _env_file_candidates():
        v = _read_key_from_file(p, "QQ_EMAIL_USER")
        if v:
            return v
    return (os.environ.get("QQ_EMAIL_USER") or "").strip()


def _get_qq_password() -> str:
    for p in _env_file_candidates():
        v = _read_key_from_file(p, "QQ_EMAIL_PASSWORD")
        if v:
            return v.replace("\ufeff", "").strip()
    return (os.environ.get("QQ_EMAIL_PASSWORD") or "").strip().replace("\ufeff", "").strip()


def qq_credentials_configured() -> bool:
    """是否已配置发件 QQ 与授权码（供日报脚本快速检查）。"""
    return bool(_get_qq_user() and _get_qq_password())


def qq_credentials_diag() -> str:
    """不含密钥的诊断信息，便于排查「一周没收到邮件」。"""
    user = _get_qq_user()
    has_pw = bool(_get_qq_password())
    tried = [str(p) for p in _env_file_candidates() if p.exists()]
    lines = [
        f"QQ_EMAIL_USER: {'已设置 (' + user + ')' if user else '未设置'}",
        f"QQ_EMAIL_PASSWORD: {'已设置' if has_pw else '未设置'}",
        f"已扫描存在的配置文件: {tried or '(无)'}",
    ]
    return "\n".join(lines)


def _mime_for_filename(filename: str) -> str:
    suf = (Path(filename).suffix or "").lower()
    return {
        "": "application/octet-stream",
        ".md": "text/markdown",
        ".txt": "text/plain",
        ".html": "text/html",
        ".json": "application/json",
    }.get(suf, "application/octet-stream")


def _send_via_qq_smtp(msg: MIMEMultipart) -> bool:
    password = _get_qq_password()
    if not password:
        logger.error("QQ_EMAIL_PASSWORD 未设置（请在 macrolab.env / .env / D:\\MF\\.env 等中配置）")
        return False
    user = _get_qq_user()
    if not user:
        logger.error("QQ_EMAIL_USER 未设置")
        return False
    ctx = ssl.create_default_context()
    for attempt in range(3):
        try:
            with smtplib.SMTP_SSL(QQ_SMTP_HOST, QQ_SMTP_PORT, context=ctx, timeout=60) as server:
                server.login(user, password)
                server.send_message(msg)
            return True
        except Exception as e:
            logger.error("邮件发送失败(第%d次): %s", attempt + 1, e)
            if attempt < 2:
                time.sleep(5)
    return False


def send_alert_email(
    subject: str,
    body_text: str,
    from_email: str,
    to_emails: List[str],
    attachments: Optional[Sequence[Union[Tuple[str, str], Tuple[Path, str]]]] = None,
) -> bool:
    """
    发送告警/日报邮件（QQ SMTP），可选附件。
    实际发件人固定为 QQ_EMAIL_USER（与 QQ 登录一致）；from_email 参数仅保留兼容。
    """
    user = _get_qq_user()
    if not user:
        logger.error("QQ_EMAIL_USER 未配置")
        return False
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = ", ".join(to_emails)
    msg["Subject"] = subject
    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    if attachments:
        n_added = 0
        for item in attachments:
            path_or_str, display_name = item
            path = Path(path_or_str) if isinstance(path_or_str, str) else path_or_str
            if not path.exists():
                logger.warning("附件不存在，跳过: %s", path)
                continue
            try:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(path.read_bytes())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", "attachment", filename=display_name)
                msg.attach(part)
                n_added += 1
            except Exception as e:
                logger.warning("添加附件失败 %s: %s", path, e)
        if n_added:
            logger.info("已添加 %d 个附件", n_added)
    if _send_via_qq_smtp(msg):
        logger.info("QQ SMTP 邮件发送成功 -> %s", to_emails)
        return True
    logger.error("QQ SMTP 邮件发送失败")
    return False
