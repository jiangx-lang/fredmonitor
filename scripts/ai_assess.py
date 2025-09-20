#!/usr/bin/env python3
"""
AI宏观分析脚本

从DuckDB数据湖读取快照，进行风险评分，并调用AI生成每日宏观状态报告。
"""

import os
import json
import datetime
from pathlib import Path
from dotenv import load_dotenv

# 添加项目根目录到Python路径
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.duckdb_io import query

# 基础路径配置
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

# 数据快照SQL查询
SNAP_SQL = """
WITH cpi_data AS (SELECT date, yoy FROM CPIAUCSL WHERE yoy IS NOT NULL ORDER BY date DESC LIMIT 1),
     core_data AS (SELECT date, yoy FROM CPILFESL WHERE yoy IS NOT NULL ORDER BY date DESC LIMIT 1),
     vix_data AS (SELECT date, value FROM VIXCLS ORDER BY date DESC LIMIT 1),
     nfci_data AS (SELECT date, value FROM NFCI ORDER BY date DESC LIMIT 1),
     y10_data AS (SELECT date, value FROM DGS10 ORDER BY date DESC LIMIT 1),
     y2_data AS (SELECT date, value FROM DGS2 ORDER BY date DESC LIMIT 1),
     hy_data AS (SELECT date, value FROM BAMLH0A0HYM2 ORDER BY date DESC LIMIT 1),
     conf_data AS (SELECT date, value FROM UMCSENT ORDER BY date DESC LIMIT 1),
     house_data AS (SELECT date, yoy FROM CSUSHPINSA WHERE yoy IS NOT NULL ORDER BY date DESC LIMIT 1),
     em_data AS (SELECT date, value FROM BAMLEMCBPIOAS ORDER BY date DESC LIMIT 1)
SELECT 
    cpi_data.date AS asof,
    cpi_data.yoy AS cpi_yoy,
    core_data.yoy AS core_yoy,
    vix_data.value AS vix,
    nfci_data.value AS nfci,
    (y10_data.value - y2_data.value) AS term_spread,
    hy_data.value AS hy_spread,
    conf_data.value AS consumer_confidence,
    house_data.yoy AS house_price_yoy,
    em_data.value AS em_spread
FROM cpi_data
LEFT JOIN core_data ON 1=1
LEFT JOIN vix_data ON 1=1
LEFT JOIN nfci_data ON 1=1
LEFT JOIN y10_data ON 1=1
LEFT JOIN y2_data ON 1=1
LEFT JOIN hy_data ON 1=1
LEFT JOIN conf_data ON 1=1
LEFT JOIN house_data ON 1=1
LEFT JOIN em_data ON 1=1;
"""


def pull_snapshot() -> dict:
    """
    从DuckDB拉取数据快照
    
    Returns:
        包含最新数据的字典
    """
    try:
        df = query(SNAP_SQL)
        if df.empty:
            return {}
        
        out = df.iloc[0].to_dict()
        
        # 转换日期为字符串
        if out.get("asof"):
            out["asof"] = str(out["asof"])
        
        return out
    except Exception as e:
        print(f"获取数据快照失败: {e}")
        return {}


def risk_score(val, low, high, reverse=False):
    """
    计算风险评分
    
    Args:
        val: 数值
        low: 低风险阈值
        high: 高风险阈值
        reverse: 是否反向评分
        
    Returns:
        风险评分 (0-100)
    """
    try:
        val = float(val)
    except (ValueError, TypeError):
        return 0.0
    
    if reverse:
        if val >= low:
            return 0.0
        if val <= high:
            return 100.0
        return 100.0 * (low - val) / (low - high)
    else:
        if val <= low:
            return 0.0
        if val >= high:
            return 100.0
        return 100.0 * (val - low) / (high - low)


def score_snapshot(snap: dict) -> dict:
    """
    对快照数据进行风险评分
    
    Args:
        snap: 数据快照
        
    Returns:
        评分结果
    """
    # 评分区间配置
    bands = {
        "CPI_yoy": (2.0, 4.0, False),      # >4% 越危险
        "CPI_core_yoy": (2.0, 4.0, False), # >4% 越危险
        "VIX": (12.0, 30.0, False),        # >30 越危险
        "NFCI": (-2.0, 1.0, False),        # >1 越危险
        "TermSpread": (0.5, 2.0, True),    # <0.5% 倒挂危险
        "HY_Spread": (3.0, 7.0, False),    # >7% 越危险
        "Consumer_Confidence": (90.0, 70.0, True),  # <70 越危险
        "House_Price_yoy": (0.0, -2.0, True),       # <-2% 越危险
        "EM_Spread": (2.0, 6.0, False),    # >6% 越危险
    }
    
    # 权重配置
    weights = {
        "CPI_yoy": 0.20,
        "CPI_core_yoy": 0.15,
        "VIX": 0.15,
        "NFCI": 0.15,
        "TermSpread": 0.10,
        "HY_Spread": 0.10,
        "Consumer_Confidence": 0.08,
        "House_Price_yoy": 0.05,
        "EM_Spread": 0.02,
    }
    
    scores = {}
    total_weight = 0.0
    weighted_sum = 0.0
    
    for indicator, (low, high, reverse) in bands.items():
        val = snap.get(indicator.lower())
        if val is not None:
            score = risk_score(val, low, high, reverse)
            scores[indicator] = score
            weight = weights.get(indicator, 0.0)
            weighted_sum += score * weight
            total_weight += weight
    
    # 计算加权总分
    total_score = weighted_sum / total_weight if total_weight > 0 else 0.0
    scores["total"] = total_score
    
    return {
        "scores": scores,
        "bands": bands,
        "weights": weights
    }


def build_prompt_payload() -> str:
    """
    构建AI提示词载荷
    
    Returns:
        JSON格式的载荷字符串
    """
    snap = pull_snapshot()
    scored = score_snapshot(snap) if snap else {}
    
    payload = {
        "snapshot": snap,
        "scored": scored,
        "timestamp": datetime.datetime.now().isoformat()
    }
    
    return json.dumps(payload, ensure_ascii=False, indent=2)


def call_ai(prompt_text: str) -> str:
    """
    调用AI生成分析报告
    
    Args:
        prompt_text: 提示词文本
        
    Returns:
        AI生成的报告或降级提示词
    """
    provider = os.getenv("AI_PROVIDER", "").lower()
    api_key = os.getenv("AI_API_KEY", "")
    model = os.getenv("AI_MODEL", "gpt-4o-mini")
    
    # 加载模板
    tpl_path = Path(BASE) / "templates" / "ai_prompt_status.md"
    try:
        template = tpl_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        template = "请分析以下宏观经济数据：\n\n{{JSON}}"
    
    prompt = template.replace("{{JSON}}", prompt_text)
    
    # 检查AI配置
    if provider != "openai" or not api_key:
        return "# (未配置AI，以下为草稿Prompt)\n\n" + prompt
    
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "你是一位资深的宏观经济研究员，擅长分析金融市场风险指标。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4,
            max_tokens=2000
        )
        
        return response.choices[0].message.content
        
    except ImportError:
        return "# (未安装OpenAI库，以下为草稿Prompt)\n\n" + prompt
    except Exception as e:
        return "# (AI调用失败，输出Prompt)\n\n" + prompt + f"\n\n<!-- error: {e} -->"


def main():
    """主函数"""
    print("开始AI宏观分析...")
    
    # 检查DuckDB连接
    try:
        from scripts.duckdb_io import test_connection
        if not test_connection():
            print("✗ DuckDB连接失败，请先运行数据同步")
            return
    except Exception as e:
        print(f"✗ DuckDB检查失败: {e}")
        return
    
    # 生成报告
    try:
        prompt_json = build_prompt_payload()
        md_content = call_ai(prompt_json)
        
        # 保存报告
        outdir = Path(BASE) / "outputs" / "macro_status"
        outdir.mkdir(parents=True, exist_ok=True)
        
        fname = outdir / f"{datetime.date.today()}.md"
        fname.write_text(md_content, encoding="utf-8")
        
        print(f"[OK] 宏观状态报告已保存: {fname}")
        
        # 检查是否有高风险预警
        try:
            snap = pull_snapshot()
            scored = score_snapshot(snap)
            total_score = scored["scores"].get("total", 0)
            
            if total_score > 60:
                alert_file = outdir / f"alerts_{datetime.date.today()}.txt"
                alert_content = f"🚨 高风险预警 - 综合风险评分: {total_score:.1f}\n"
                alert_content += f"时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                alert_content += f"详情请查看: {fname.name}\n"
                alert_file.write_text(alert_content, encoding="utf-8")
                print(f"[ALERT] 高风险预警已生成: {alert_file}")
                
        except Exception as e:
            print(f"[WARN] 预警检查失败: {e}")
        
    except Exception as e:
        print(f"[ERROR] AI分析失败: {e}")


if __name__ == "__main__":
    main()