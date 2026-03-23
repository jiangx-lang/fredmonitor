import json
import re
from pathlib import Path

import os
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import crisis_monitor_v2 as cm


def main() -> None:
    out_dir = Path("outputs") / "crisis_monitor"
    latest_json_path = out_dir / "crisis_report_latest.json"
    latest_md_path = out_dir / "crisis_report_latest.md"

    j = json.load(open(latest_json_path, encoding="utf-8"))
    summary = j.get("summary") or {}
    rc = j.get("risk_control") or {}

    # 1) recompute fragility (so triggers contain intensity)
    frag = cm.compute_fragility_state(j, summary)
    j["fragility"] = frag

    # 2) rebuild early warning section
    _md_summary = {**summary, **(j.get("summary") or {})}
    new_ew = cm._build_early_warning_section(_md_summary, frag)

    # 3) rebuild executive section (so contradictory sentence is fixed)
    profiles = getattr(cm, "V2_SUMMARY", {}) or {}
    exec_summary = cm.generate_executive_summary(_md_summary, profiles, risk_control=rc)
    cb_md = cm.check_circuit_breaker(j)
    exec_section = cm._build_executive_summary_section(
        exec_summary, circuit_breaker_markdown=cb_md
    )

    # 4) rebuild reader summary
    reader_summary = cm._build_reader_summary_section(j, _md_summary)

    md_text = latest_md_path.read_text(encoding="utf-8")

    # replace early warning block
    md_text = re.sub(
        r"## 🧭 早预警指数[\s\S]*?(?=\n## |\Z)",
        new_ew.rstrip(),
        md_text,
        flags=re.M,
    )

    # remove existing executive verdict block
    md_text = (
        re.sub(
            r"\n## 综合性结论（Executive Verdict）[\s\S]*?(?=\n## |\Z)",
            "\n",
            md_text,
        )
        .rstrip()
    )

    lines = md_text.splitlines()
    header_idx = next(
        (
            i
            for i, line in enumerate(lines)
            if line.startswith("# 🚨 FRED 宏观金融危机预警监控报告")
        ),
        None,
    )
    if header_idx is None:
        raise RuntimeError("report header not found")

    insert_idx = next((i for i, line in enumerate(lines) if line.startswith("**生成时间**")), None)
    if insert_idx is None:
        insert_idx = header_idx + 1
    else:
        insert_idx += 1

    while insert_idx < len(lines) and lines[insert_idx].strip() == "":
        insert_idx += 1

    # insert reader summary + exec section at top
    lines.insert(insert_idx, "")
    lines.insert(insert_idx + 1, reader_summary.rstrip())
    lines.insert(insert_idx + 2, "")
    lines.insert(insert_idx + 3, exec_section.rstrip())
    lines.insert(insert_idx + 4, "")

    md_new = "\n".join(lines).rstrip() + "\n"
    latest_md_path.write_text(md_new, encoding="utf-8")

    print("=== crisis_report_latest.md 前20行 ===")
    for i, line in enumerate(md_new.splitlines()[:20], start=1):
        print(f"L{i}: {line}")

    print("\n-- sanity checks --")
    print("has reader summary title:", "### 今天最重要的三件事" in md_new)
    print("has contradictory phrase:", "整体风险处于低位" in md_new)
    print("has fragility layer title:", "C层脆弱性传导状态（Fragility Layer）" in md_new)
    print("has intensity column:", "| 强度 |" in md_new)
    print("has KRE exclusion:", "KRE_SPY_RATIO" in md_new)


if __name__ == "__main__":
    main()

