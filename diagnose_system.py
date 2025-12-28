#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统诊断脚本
用于检查 crisis_monitor.py 及其生成的数据是否存在严重的逻辑缺陷和数据异常
"""

import pandas as pd
import json
import pathlib
import sys
import shutil
import numpy as np
import re
import os
from datetime import datetime
from typing import Optional, Dict, List

# --- 配置路径 ---
BASE_DIR = pathlib.Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "outputs" / "crisis_monitor"
LATEST_JSON = OUTPUT_DIR / "crisis_report_latest.json"
REPORT_FILE = BASE_DIR / "diagnostic_report.txt"

# --- 日志工具类 ---
class Logger:
    """同时输出到控制台和文件的日志工具，自动去除 ANSI 颜色代码"""
    
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log_file = open(filename, "w", encoding="utf-8")
        
        # ANSI 颜色代码（用于控制台）
        self.HEADER = '\033[95m'
        self.OKBLUE = '\033[94m'
        self.OKGREEN = '\033[92m'
        self.WARNING = '\033[93m'
        self.FAIL = '\033[91m'
        self.ENDC = '\033[0m'
        self.BOLD = '\033[1m'
        
        # ANSI 转义序列正则表达式
        self.ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        
        # 写入文件头
        self.log_file.write("=" * 80 + "\n")
        self.log_file.write("FRED 危机监控系统诊断报告\n")
        self.log_file.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.log_file.write("=" * 80 + "\n\n")
        self.log_file.flush()

    def _remove_ansi(self, text: str) -> str:
        """去除 ANSI 颜色代码"""
        return self.ansi_escape.sub('', text)

    def write(self, message: str, level: str = "info"):
        """
        写入日志
        
        Args:
            message: 日志消息
            level: 日志级别 (info, error, warn, pass, header, section)
        """
        # 1. 准备控制台输出（带颜色）
        console_msg = message
        
        if level == "error":
            console_msg = f"{self.FAIL}[ERROR]{self.ENDC} {message}"
        elif level == "warn":
            console_msg = f"{self.WARNING}[WARN] {self.ENDC}{message}"
        elif level == "pass":
            console_msg = f"{self.OKGREEN}[PASS] {self.ENDC}{message}"
        elif level == "header":
            console_msg = f"\n{self.HEADER}{self.BOLD}=== {message} ==={self.ENDC}"
        elif level == "section":
            console_msg = f"\n{self.OKBLUE}{self.BOLD}--- {message} ---{self.ENDC}"
        elif level == "info":
            console_msg = f"[INFO] {message}"
        
        # 2. 输出到控制台
        print(console_msg)

        # 3. 准备文件输出（去除颜色代码）
        clean_msg = self._remove_ansi(console_msg)
        
        # 写入文件
        self.log_file.write(clean_msg + "\n")
        self.log_file.flush()

    def write_dataframe(self, df, title: str = ""):
        """写入 DataFrame（格式化输出）"""
        if title:
            self.write(title, "section")
        
        # 控制台输出
        print(df.to_string())
        
        # 文件输出（去除可能的 ANSI 代码）
        df_str = df.to_string()
        self.log_file.write(df_str + "\n\n")
        self.log_file.flush()

    def write_separator(self):
        """写入分隔线"""
        separator = "-" * 80
        print(separator)
        self.log_file.write(separator + "\n")
        self.log_file.flush()

    def close(self):
        """关闭日志文件"""
        self.log_file.write("\n" + "=" * 80 + "\n")
        self.log_file.write("诊断报告结束\n")
        self.log_file.write("=" * 80 + "\n")
        self.log_file.close()

# 初始化日志
logger = Logger(REPORT_FILE)

def check_totalsa_data():
    """检查 TOTALSA (消费者信贷) 的原始数据异常"""
    logger.write("1. 检查 TOTALSA 数据源异常", "header")
    
    # 尝试多个可能的数据源路径
    data_paths = [
        DATA_DIR / "fred" / "series" / "TOTALSA" / "raw.csv",
        DATA_DIR / "series" / "TOTALSA.csv",
        DATA_DIR / "series" / "TOTALSA_YOY.csv"
    ]
    
    raw_data = None
    yoy_data = None
    data_source = None
    
    # 查找原始数据
    for path in data_paths:
        if path.exists():
            try:
                if "YOY" in path.name:
                    # 读取 YoY 数据
                    df = pd.read_csv(path)
                    if 'yoy_pct' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                        yoy_data = df.set_index('date')['yoy_pct']
                        data_source = f"YoY文件: {path.name}"
                        logger.write(f"找到 YoY 数据源: {path.name}", "info")
                else:
                    # 读取原始数据
                    df = pd.read_csv(path, index_col=0, parse_dates=True)
                    if len(df.columns) > 0:
                        raw_data = df.iloc[:, 0]
                        data_source = f"原始文件: {path.name}"
                        logger.write(f"找到原始数据源: {path.name}", "info")
                        break
            except Exception as e:
                logger.write(f"读取 {path.name} 失败: {e}", "warn")
    
    # 检查原始数据
    if raw_data is not None:
        logger.write(f"数据源: {data_source}", "info")
        logger.write(f"数据总行数: {len(raw_data)}", "info")
        
        if len(raw_data) == 0:
            logger.write("数据文件为空！", "error")
            return
        
        # 检查最后几行
        last_rows = raw_data.tail(5)
        logger.write_dataframe(last_rows.to_frame("value"), "最后5行原始数据:")
        
        last_val = raw_data.iloc[-1]
        logger.write(f"最新值: {last_val}", "info")
        
        # 检查异常值
        if pd.isna(last_val):
            logger.write("【致命异常】最新值为 NaN，这将直接导致 YoY 计算崩溃。", "error")
        elif last_val == 0:
            logger.write("【致命异常】最新值为 0，这将直接导致 YoY 计算崩溃。", "error")
        else:
            # 尝试计算 YoY（如果是月度数据，需要12个月前的数据）
            if len(raw_data) >= 13:
                prev_idx = -13
                prev_year_val = raw_data.iloc[prev_idx]
                
                logger.write(f"一年前值 (索引 {prev_idx}): {prev_year_val}", "info")
                
                if pd.isna(prev_year_val) or prev_year_val == 0:
                    logger.write("一年前数据为 NaN 或 0，无法计算 YoY。", "warn")
                else:
                    # 模拟 YoY 计算
                    yoy = (last_val / prev_year_val - 1) * 100
                    logger.write(f"模拟计算 YoY: {yoy:.4f}%", "info")
                    
                    if yoy < -5:
                        logger.write(
                            f"【异常确认】计算出的 YoY ({yoy:.2f}%) 极低，"
                            f"不符合历史常识 (消费者信贷通常为正增长)。", 
                            "error"
                        )
                    elif yoy < 0:
                        logger.write(
                            f"【警告】计算出的 YoY ({yoy:.2f}%) 为负值，需要进一步验证。", 
                            "warn"
                        )
                    else:
                        logger.write(f"YoY 计算正常: {yoy:.2f}%", "pass")
            else:
                logger.write("数据不足13行，无法计算 YoY。", "warn")
    
    # 检查 YoY 预计算数据
    if yoy_data is not None:
        logger.write_separator()
        logger.write("检查 YoY 预计算数据", "section")
        
        if len(yoy_data) == 0:
            logger.write("YoY 数据为空！", "error")
            return
        
        last_5_yoy = yoy_data.tail(5)
        logger.write_dataframe(last_5_yoy.to_frame("yoy_pct"), "最后5行 YoY 预计算数据:")
        
        latest_yoy = yoy_data.iloc[-1]
        logger.write(f"最新 YoY 值: {latest_yoy:.4f}%", "info")
        
        if pd.isna(latest_yoy):
            logger.write("【致命异常】最新 YoY 值为 NaN。", "error")
        elif latest_yoy < -5:
            logger.write(
                f"【异常确认】YoY 预计算值 ({latest_yoy:.2f}%) 异常偏低！", 
                "error"
            )
            
            # 检查趋势
            if len(yoy_data) >= 3:
                recent_trend = yoy_data.tail(3).values
                if all(pd.notna(x) and x < 0 for x in recent_trend):
                    logger.write(
                        "【趋势异常】最近3期 YoY 均为负值，可能存在数据质量问题。", 
                        "error"
                    )
        elif latest_yoy < 0:
            logger.write(
                f"【警告】YoY 预计算值为负 ({latest_yoy:.2f}%)，需要验证。", 
                "warn"
            )
        else:
            logger.write(f"YoY 预计算值正常: {latest_yoy:.2f}%", "pass")
    
    if raw_data is None and yoy_data is None:
        logger.write("未找到任何 TOTALSA 数据文件。", "error")

def check_json_logic():
    """检查 JSON 报告中的逻辑悖论和冗余"""
    logger.write("2. 检查报告结果逻辑 (JSON)", "header")
    
    if not LATEST_JSON.exists():
        # 尝试查找最新的 JSON 文件
        if OUTPUT_DIR.exists():
            json_files = list(OUTPUT_DIR.glob("crisis_report_*.json"))
            if json_files:
                latest_json = max(json_files, key=lambda x: x.stat().st_mtime)
                logger.write(f"使用最新的 JSON 文件: {latest_json.name}", "info")
                json_path = latest_json
            else:
                logger.write(f"找不到任何 JSON 报告文件，请先运行主程序生成报告。", "error")
                return
        else:
            logger.write(f"输出目录不存在: {OUTPUT_DIR}", "error")
            return
    else:
        json_path = LATEST_JSON
        logger.write(f"使用 JSON 文件: {json_path.name}", "info")
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        indicators = data.get('indicators', [])
        logger.write(f"分析了 {len(indicators)} 个指标", "info")
        
        # --- 2.1 检查冗余 ---
        logger.write_separator()
        logger.write("2.1 检查指标冗余", "section")
        
        ids = [i.get('series_id', '') for i in indicators]
        
        # 检查企业债指标重复
        if 'NCBDBIQ027S' in ids and 'CORPDEBT_GDP_PCT' in ids:
            logger.write(
                "【发现冗余】企业债同时存在新旧两个代码 (NCBDBIQ027S, CORPDEBT_GDP_PCT)。", 
                "error"
            )
            
            ind1 = next(i for i in indicators if i.get('series_id') == 'NCBDBIQ027S')
            ind2 = next(i for i in indicators if i.get('series_id') == 'CORPDEBT_GDP_PCT')
            
            val1 = ind1.get('current_value', 0)
            val2 = ind2.get('current_value', 0)
            weight1 = ind1.get('global_weight', 0)
            weight2 = ind2.get('global_weight', 0)
            
            logger.write(f"NCBDBIQ027S 当前值: {val1:.6f}, 权重: {weight1:.6f}", "info")
            logger.write(f"CORPDEBT_GDP_PCT 当前值: {val2:.6f}, 权重: {weight2:.6f}", "info")
            
            if abs(val1 - val2) < 0.0001:
                logger.write(
                    f"【严重冗余】两个指标数值完全一致 ({val1:.6f})，"
                    f"这会导致该指标权重被双倍计算。", 
                    "error"
                )
                logger.write(
                    f"总权重: {weight1 + weight2:.6f}，"
                    f"建议移除其中一个指标以避免重复计分。", 
                    "warn"
                )
            else:
                logger.write(
                    f"两个指标数值不同 (差值: {abs(val1 - val2):.6f})，"
                    f"但概念重复，需要确认是否应该合并。", 
                    "warn"
                )
        else:
            logger.write("未发现企业债指标冗余。", "pass")
        
        # 检查其他重复的 series_id
        from collections import Counter
        id_counts = Counter(ids)
        duplicates = {sid: count for sid, count in id_counts.items() if count > 1}
        
        if duplicates:
            logger.write(
                f"【发现重复】存在重复的 series_id: {duplicates}", 
                "error"
            )
        else:
            logger.write("未发现重复的 series_id。", "pass")
        
        # --- 2.2 检查 T10Y3M 评分逻辑 ---
        logger.write_separator()
        logger.write("2.2 检查 T10Y3M 评分逻辑悖论", "section")
        
        target = next((i for i in indicators if i.get('series_id') == 'T10Y3M'), None)
        
        if target:
            curr = target.get('current_value', 0)
            bench = target.get('benchmark_value', 0)
            score = target.get('risk_score', 0)
            higher_is_risk = target.get('higher_is_risk', True)
            compare_to = target.get('compare_to', '')
            last_date = target.get('last_date', '')
            
            logger.write(f"T10Y3M (10年-3个月美债利差) 详细信息:", "info")
            logger.write(f"  当前值: {curr:.6f}", "info")
            logger.write(f"  基准值: {bench:.6f}", "info")
            logger.write(f"  风险评分: {score:.2f}", "info")
            logger.write(f"  方向: {'越高越危险' if higher_is_risk else '越低越危险'}", "info")
            logger.write(f"  基准类型: {compare_to}", "info")
            logger.write(f"  最新日期: {last_date}", "info")
            
            # T10Y3M 逻辑：越低越危险 (倒挂风险)
            if not higher_is_risk:  # 越低越危险
                if curr < bench:
                    logger.write(
                        f"当前值 ({curr:.6f}) < 基准值 ({bench:.6f})，"
                        f"情况比基准更差（已跌破警戒线）", 
                        "warn"
                    )
                    if score < 50:
                        logger.write(
                            f"【逻辑悖论】指标已跌破基准线，但分数 ({score:.2f}) 却显示为低风险。"
                            f"打分算法存在严重缺陷。", 
                            "error"
                        )
                    elif score < 60:
                        logger.write(
                            f"【警告】指标已跌破基准线，但分数 ({score:.2f}) 仅略高于中性，"
                            f"可能评分不够敏感。", 
                            "warn"
                        )
                    else:
                        logger.write(f"分数 ({score:.2f}) 正确反映了风险。", "pass")
                else:
                    logger.write(
                        f"当前值 ({curr:.6f}) 优于基准值 ({bench:.6f})，低分合理。", 
                        "pass"
                    )
            else:  # 越高越危险
                if curr > bench:
                    logger.write(
                        f"当前值 ({curr:.6f}) > 基准值 ({bench:.6f})，"
                        f"情况比基准更差（已超过警戒线）", 
                        "warn"
                    )
                    if score < 50:
                        logger.write(
                            f"【逻辑悖论】指标已超过基准线，但分数 ({score:.2f}) 却显示为低风险。"
                            f"打分算法存在严重缺陷。", 
                            "error"
                        )
                    else:
                        logger.write(f"分数 ({score:.2f}) 正确反映了风险。", "pass")
                else:
                    logger.write(
                        f"当前值 ({curr:.6f}) 低于基准值 ({bench:.6f})，低分合理。", 
                        "pass"
                    )
        else:
            logger.write("未在报告中找到 T10Y3M。", "warn")
        
        # --- 2.3 检查 T10Y2Y（另一个收益率曲线指标）---
        logger.write_separator()
        logger.write("2.3 检查 T10Y2Y 评分逻辑", "section")
        
        target2 = next((i for i in indicators if i.get('series_id') == 'T10Y2Y'), None)
        
        if target2:
            curr2 = target2.get('current_value', 0)
            bench2 = target2.get('benchmark_value', 0)
            score2 = target2.get('risk_score', 0)
            higher_is_risk2 = target2.get('higher_is_risk', True)
            
            logger.write(f"T10Y2Y 当前值: {curr2:.6f}, 基准值: {bench2:.6f}, 评分: {score2:.2f}", "info")
            
            if not higher_is_risk2:
                if curr2 < bench2 and score2 < 50:
                    logger.write(
                        f"【逻辑悖论】T10Y2Y 已跌破警戒线但风险评分仅为 {score2:.2f}，存在逻辑错误！", 
                        "error"
                    )
                else:
                    logger.write("T10Y2Y 评分逻辑正常。", "pass")
        
    except json.JSONDecodeError as e:
        logger.write(f"JSON 解析失败: {e}", "error")
    except Exception as e:
        logger.write(f"检查 JSON 逻辑时出错: {e}", "error")
        import traceback
        logger.write(traceback.format_exc(), "error")

# ===== v2.0: 新增诊断功能 =====

def check_v2_indicators(logger):
    """检查v2.0新增指标配置"""
    logger.write("检查v2.0新增指标配置", "section")
    
    config_path = BASE_DIR / "config" / "crisis_indicators.yaml"
    if not config_path.exists():
        logger.write("配置文件不存在", "error")
        return False
    
    import yaml
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    indicators = config.get('indicators', [])
    
    # 检查新指标类别
    new_categories = ['liquidity', 'recession_leading', 'inflation_expectations']
    found_categories = set()
    new_indicators = ['RRPONTSYD', 'WTREGEN', 'M2SL', 'SAHMREALTIME', 'KCFSI', 'T5YIE']
    found_indicators = []
    
    for ind in indicators:
        group = ind.get('group', '')
        if group in new_categories:
            found_categories.add(group)
        
        series_id = ind.get('id') or ind.get('series_id', '')
        if series_id in new_indicators:
            found_indicators.append(series_id)
    
    # 检查新字段
    has_type = False
    has_momentum_window = False
    has_invert_momentum = False
    
    for ind in indicators:
        if 'type' in ind:
            has_type = True
        if 'momentum_window' in ind:
            has_momentum_window = True
        if 'invert_momentum' in ind:
            has_invert_momentum = True
    
    # 报告结果
    if found_categories:
        logger.write(f"✅ 找到新指标类别: {', '.join(found_categories)}", "pass")
    else:
        logger.write("⚠️ 未找到新指标类别", "warn")
    
    if found_indicators:
        logger.write(f"✅ 找到新指标: {', '.join(found_indicators)}", "pass")
    else:
        logger.write("⚠️ 未找到新指标", "warn")
    
    if has_type:
        logger.write("✅ 配置包含'type'字段", "pass")
    else:
        logger.write("⚠️ 配置缺少'type'字段", "warn")
    
    if has_momentum_window:
        logger.write("✅ 配置包含'momentum_window'字段", "pass")
    else:
        logger.write("⚠️ 配置缺少'momentum_window'字段", "warn")
    
    if has_invert_momentum:
        logger.write("✅ 配置包含'invert_momentum'字段", "pass")
    else:
        logger.write("⚠️ 配置缺少'invert_momentum'字段", "warn")
    
    return True

def check_momentum_calculations(logger):
    """检查动量计算（确保没有除零错误）"""
    logger.write("检查动量计算", "section")
    
    try:
        # 导入crisis_monitor模块
        sys.path.insert(0, str(BASE_DIR))
        from crisis_monitor import calculate_momentum_score_v2
        import pandas as pd
        import numpy as np
        
        # 测试用例1: 正常数据
        test_ts1 = pd.Series([1.0, 1.1, 1.2, 1.3, 1.4], 
                             index=pd.date_range('2024-01-01', periods=5, freq='D'))
        score1 = calculate_momentum_score_v2(test_ts1, momentum_window=3, 
                                             invert_momentum=False, higher_is_risk=True)
        if 0 <= score1 <= 100:
            logger.write(f"✅ 正常数据测试通过: score={score1:.2f}", "pass")
        else:
            logger.write(f"❌ 正常数据测试失败: score={score1:.2f} (应在0-100之间)", "error")
        
        # 测试用例2: 空数据
        test_ts2 = pd.Series(dtype=float)
        score2 = calculate_momentum_score_v2(test_ts2, momentum_window=3, 
                                             invert_momentum=False, higher_is_risk=True)
        if score2 == 0.0:
            logger.write("✅ 空数据测试通过: 返回0", "pass")
        else:
            logger.write(f"❌ 空数据测试失败: score={score2}", "error")
        
        # 测试用例3: 包含零值的数据（防止除零错误）
        test_ts3 = pd.Series([0.0, 0.1, 0.2, 0.3], 
                             index=pd.date_range('2024-01-01', periods=4, freq='D'))
        try:
            score3 = calculate_momentum_score_v2(test_ts3, momentum_window=1, 
                                                 invert_momentum=False, higher_is_risk=True)
            logger.write(f"✅ 零值数据测试通过: score={score3:.2f}", "pass")
        except ZeroDivisionError:
            logger.write("❌ 零值数据测试失败: 发生除零错误", "error")
        
        # 测试用例4: 包含NaN的数据
        test_ts4 = pd.Series([1.0, np.nan, 1.2, 1.3, 1.4], 
                             index=pd.date_range('2024-01-01', periods=5, freq='D'))
        try:
            score4 = calculate_momentum_score_v2(test_ts4, momentum_window=3, 
                                                 invert_momentum=False, higher_is_risk=True)
            logger.write(f"✅ NaN数据测试通过: score={score4:.2f}", "pass")
        except Exception as e:
            logger.write(f"❌ NaN数据测试失败: {e}", "error")
        
        return True
        
    except ImportError as e:
        logger.write(f"❌ 无法导入crisis_monitor模块: {e}", "error")
        return False
    except Exception as e:
        logger.write(f"❌ 动量计算检查失败: {e}", "error")
        return False

def check_v2_scoring_config(logger):
    """检查v2.0评分配置"""
    logger.write("检查v2.0评分配置", "section")
    
    config_path = BASE_DIR / "config" / "crisis_indicators.yaml"
    if not config_path.exists():
        logger.write("配置文件不存在", "error")
        return False
    
    import yaml
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    scoring = config.get('scoring', {})
    
    # 检查v2.0新配置项
    required_v2_configs = [
        'momentum_weight', 'level_weight', 
        'persistence_months', 'persistence_multiplier',
        'resonance_threshold', 'systemic_risk_multiplier'
    ]
    
    missing_configs = []
    for key in required_v2_configs:
        if key not in scoring:
            missing_configs.append(key)
    
    if missing_configs:
        logger.write(f"⚠️ 缺少v2.0配置项: {', '.join(missing_configs)}", "warn")
    else:
        logger.write("✅ 所有v2.0配置项都存在", "pass")
    
    # 检查权重归一化
    momentum_weight = scoring.get('momentum_weight', 0.3)
    level_weight = scoring.get('level_weight', 0.7)
    total_weight = momentum_weight + level_weight
    
    if abs(total_weight - 1.0) < 0.01:
        logger.write(f"✅ 动量/水平权重归一化正确: {momentum_weight} + {level_weight} = {total_weight:.2f}", "pass")
    else:
        logger.write(f"⚠️ 动量/水平权重未归一化: {momentum_weight} + {level_weight} = {total_weight:.2f} (应为1.0)", "warn")
    
    return True

def check_env():
    """检查环境配置"""
    logger.write("3. 环境检查", "header")
    
    # 检查 wkhtmltoimage
    logger.write_separator()
    logger.write("3.1 检查 wkhtmltoimage", "section")
    
    wk_paths = [
        shutil.which('wkhtmltoimage'),
        r'C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe',
        r'C:\Program Files (x86)\wkhtmltopdf\bin\wkhtmltoimage.exe',
    ]
    
    found = False
    for path in wk_paths:
        if path and os.path.exists(path):
            logger.write(f"wkhtmltoimage 路径: {path}", "pass")
            found = True
            break
    
    if not found:
        logger.write("未找到 wkhtmltoimage，长图生成功能将不可用。", "warn")
    
    # 检查输出目录
    logger.write_separator()
    logger.write("3.2 检查输出目录", "section")
    
    if OUTPUT_DIR.exists():
        logger.write(f"输出目录存在: {OUTPUT_DIR}", "pass")
        
        # 统计文件
        json_files = list(OUTPUT_DIR.glob("*.json"))
        md_files = list(OUTPUT_DIR.glob("*.md"))
        html_files = list(OUTPUT_DIR.glob("*.html"))
        
        logger.write(f"JSON 文件数: {len(json_files)}", "info")
        logger.write(f"Markdown 文件数: {len(md_files)}", "info")
        logger.write(f"HTML 文件数: {len(html_files)}", "info")
    else:
        logger.write(f"输出目录不存在: {OUTPUT_DIR}", "error")
    
    # 检查数据目录
    logger.write_separator()
    logger.write("3.3 检查数据目录", "section")
    
    if DATA_DIR.exists():
        logger.write(f"数据目录存在: {DATA_DIR}", "pass")
        
        fred_dir = DATA_DIR / "fred" / "series"
        if fred_dir.exists():
            series_dirs = [d for d in fred_dir.iterdir() if d.is_dir()]
            logger.write(f"FRED 数据系列数: {len(series_dirs)}", "info")
        else:
            logger.write(f"FRED 数据目录不存在: {fred_dir}", "warn")
    else:
        logger.write(f"数据目录不存在: {DATA_DIR}", "error")

def main():
    """主函数"""
    try:
        logger.write("FRED 危机监控系统诊断工具", "header")
        logger.write(f"诊断时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "info")
        logger.write_separator()
        
        check_totalsa_data()
        logger.write_separator()
        
        check_json_logic()
        logger.write_separator()
        
        # v2.0: 新增检查项
        check_v2_indicators(logger)
        logger.write_separator()
        
        check_momentum_calculations(logger)
        logger.write_separator()
        
        check_v2_scoring_config(logger)
        logger.write_separator()
        
        check_env()
        
    except Exception as e:
        logger.write(f"诊断脚本运行出错: {e}", "error")
        import traceback
        logger.write(traceback.format_exc(), "error")
    finally:
        logger.close()
        try:
            print(f"\n[完成] 诊断完成！结果已保存至: {REPORT_FILE}")
            print("请将该文件的内容发送给其他 AI 助手进行分析。")
        except UnicodeEncodeError:
            # Windows 控制台编码问题，使用 ASCII 字符
            print(f"\n[完成] 诊断完成！结果已保存至: {REPORT_FILE}")
            print("请将该文件的内容发送给其他 AI 助手进行分析。")

if __name__ == "__main__":
    main()
