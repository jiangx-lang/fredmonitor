#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SendGrid 邮件发送模块（复用历史实现，简化为纯文本）。
"""
import logging
from typing import List

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

logger = logging.getLogger(__name__)


def send_alert_email(api_key: str, subject: str, body_text: str, from_email: str, to_emails: List[str]) -> bool:
    try:
        message = Mail(
            from_email=from_email,
            to_emails=to_emails,
            subject=subject,
            plain_text_content=body_text,
        )
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        if response.status_code == 202:
            logger.info("✅ SendGrid 邮件发送成功")
            return True
        logger.error(f"❌ SendGrid 邮件发送失败: {response.status_code}")
        if hasattr(response, "body"):
            logger.error(f"   响应内容: {response.body}")
        return False
    except Exception as e:
        logger.error(f"❌ SendGrid 邮件发送异常: {e}")
        return False
