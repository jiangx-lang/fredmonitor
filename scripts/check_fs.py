#!/usr/bin/env python3
"""
文件系统写入检查
"""

import os
import pathlib

BASE = os.getenv("BASE_DIR", r"D:\Macro")
p = pathlib.Path(BASE)/"__writable_test__.txt"
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text("ok", encoding="utf-8")
print(f"[OK] write {p} succeeded.")
p.unlink()
print("[OK] 文件系统写入检查通过")
