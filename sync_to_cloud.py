#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仅同步「危机长图」到腾讯云：本仓库 outputs/crisis_monitor/ 下 crisis_report_long* 文件。

运行目录：须为 fred_crisis_monitor 根目录（例如 D:\\fred_crisis_monitor），
本地路径固定为：./outputs/crisis_monitor/（与脚本所在目录相对）。

认证（二选一）：
  - FRED_CLOUD_SYNC_PASSWORD
  - FRED_CLOUD_SYNC_KEY_PATH（推荐）

环境变量（macrolab.env / .env 可写）：
  FRED_CLOUD_SYNC_HOST=43.161.234.75
  FRED_CLOUD_SYNC_USER=root
  FRED_CLOUD_SYNC_REMOTE_PATH=/root/fredmonitor/outputs/crisis_monitor/
  FRED_CLOUD_SYNC_KEY_PATH=C:/Users/xxx/.ssh/id_rsa
  FRED_CLOUD_SYNC_PORT=22
"""
from __future__ import annotations

import os
import pathlib
import shlex
import sys
from typing import List, Optional

try:
    import paramiko
    from scp import SCPClient
except ImportError:
    print("❌ 请先安装: pip install paramiko scp", file=sys.stderr)
    sys.exit(1)

_BASE = pathlib.Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv
    for _f in (_BASE / "macrolab.env", _BASE / ".env"):
        if _f.exists():
            try:
                load_dotenv(_f, encoding="utf-8")
            except Exception:
                load_dotenv(_f, encoding="gbk")
except Exception:
    pass

HOST = os.environ.get("FRED_CLOUD_SYNC_HOST", "43.161.234.75")
USER = os.environ.get("FRED_CLOUD_SYNC_USER", "root")
PASSWORD = os.environ.get("FRED_CLOUD_SYNC_PASSWORD", "").strip() or None
KEY_PATH = os.environ.get("FRED_CLOUD_SYNC_KEY_PATH", "").strip() or None
REMOTE_PATH = os.environ.get(
    "FRED_CLOUD_SYNC_REMOTE_PATH",
    "/root/fredmonitor/outputs/crisis_monitor/",
).rstrip("/") + "/"
try:
    PORT = int(os.environ.get("FRED_CLOUD_SYNC_PORT", "22"))
except ValueError:
    PORT = 22

LOCAL_PATH = _BASE / "outputs" / "crisis_monitor"
LONG_PREFIX = "crisis_report_long"
# 长图常见后缀；若有其它后缀可把环境变量 FRED_SYNC_LONG_EXT 设为 ".png,.jpg" 等
_DEFAULT_EXTS = {".png", ".jpg", ".jpeg", ".webp"}


def _long_extensions() -> set[str]:
    raw = os.environ.get("FRED_SYNC_LONG_EXT", "").strip()
    if not raw:
        return set(_DEFAULT_EXTS)
    out: set[str] = set()
    for part in raw.split(","):
        e = part.strip().lower()
        if not e:
            continue
        out.add(e if e.startswith(".") else f".{e}")
    return out if out else set(_DEFAULT_EXTS)


def _iter_long_files() -> List[pathlib.Path]:
    """outputs/crisis_monitor/ 下 crisis_report_long*，按修改时间新→旧。"""
    exts = _long_extensions()
    if not LOCAL_PATH.is_dir():
        return []
    out: List[pathlib.Path] = []
    for f in LOCAL_PATH.iterdir():
        if not f.is_file():
            continue
        if not f.name.lower().startswith(LONG_PREFIX.lower()):
            continue
        if f.suffix.lower() not in exts:
            continue
        out.append(f)
    out.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return out


def _load_pkey(path: str) -> Optional[paramiko.PKey]:
    expanded = pathlib.Path(path).expanduser()
    if not expanded.is_file():
        return None
    p = str(expanded)
    for key_cls in (
        paramiko.RSAKey,
        paramiko.Ed25519Key,
        paramiko.ECDSAKey,
    ):
        try:
            return key_cls.from_private_key_file(p)
        except Exception:
            continue
    return None


def _exec_out(ssh: paramiko.SSHClient, cmd: str) -> tuple[int, str, str]:
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    code = stdout.channel.recv_exit_status()
    return code, out, err


def upload_reports() -> int:
    if not PASSWORD and not KEY_PATH:
        print(
            "⚠️ 未设置 FRED_CLOUD_SYNC_PASSWORD 或 FRED_CLOUD_SYNC_KEY_PATH，跳过同步。"
        )
        return 0

    if not LOCAL_PATH.is_dir():
        print(f"❌ 本地目录不存在: {LOCAL_PATH}")
        return 1

    long_files = _iter_long_files()
    if not long_files:
        print(
            f"❌ 未找到可上传文件：{LOCAL_PATH} 下无 crisis_report_long* "
            f"（后缀需为 {sorted(_long_extensions())}，可用 FRED_SYNC_LONG_EXT 扩展）"
        )
        return 1

    pkey = _load_pkey(KEY_PATH) if KEY_PATH else None
    if KEY_PATH and pkey is None:
        print(f"❌ 无法读取私钥文件: {KEY_PATH}")
        return 1
    if not pkey and not PASSWORD:
        print("❌ 无有效认证方式（密码或私钥）")
        return 1

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        connect_kw: dict = {
            "hostname": HOST,
            "port": PORT,
            "username": USER,
            "timeout": 30,
        }
        if pkey is not None:
            connect_kw["pkey"] = pkey
        if PASSWORD:
            connect_kw["password"] = PASSWORD
        ssh.connect(**connect_kw)
    except Exception as e:
        print(f"❌ SSH 连接失败: {e}")
        return 1

    try:
        remote_base = REMOTE_PATH.rstrip("/")
        code, _, err = _exec_out(ssh, f"mkdir -p {shlex.quote(remote_base)}")
        if code != 0:
            print(f"⚠️ mkdir 远端目录: {err.strip()}")

        with SCPClient(ssh.get_transport(), socket_timeout=600) as scp:
            print(
                f"🚀 仅同步 crisis_report_long* → {USER}@{HOST}:{REMOTE_PATH} "
                f"（共 {len(long_files)} 个文件）"
            )
            for lf in long_files:
                scp.put(str(lf), REMOTE_PATH + lf.name)
                print(f"✅ {lf.name}")

        rp_q = shlex.quote(REMOTE_PATH)
        _, vlong, _ = _exec_out(
            ssh,
            f"ls -1t {rp_q}crisis_report_long* 2>/dev/null | head -1",
        )
        vlong = vlong.strip()
        if vlong:
            print(
                f"🔎 远端最新: {vlong.split('/')[-1] if '/' in vlong else vlong}"
            )
        else:
            print("⚠️ 远端 ls 未匹配到 crisis_report_long*，请 SSH 检查权限与路径")

        print("🎉 长图同步完成")
        return 0
    except Exception as e:
        print(f"❌ 同步失败: {e}")
        return 1
    finally:
        try:
            ssh.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(upload_reports())
