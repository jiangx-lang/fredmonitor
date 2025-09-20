"""
TED利差因子 - 完整实现示例

基于SOFR和3个月国债收益率计算TED利差，展示多数据源因子的实现模式。
"""

import pandas as pd
from typing import Dict, Any, Optional, Tuple
import logging
from datetime import datetime, timedelta

from .base_factor import Factor

logger = logging.getLogger(__name__)


class TEDSpreadComplete(Factor):
    """TED利差因子 - 完整实现"""
    
    id = "TED"
    name = "TED利差"
    units = "百分比"
    
    def __init__(self, cfg: Dict[str, Any]):
        """
        初始化TED利差因子
        
        Args:
            cfg: 因子配置参数
                - sofr_series: SOFR序列ID (默认: "SOFR")
                - tbill_series: 3个月国债序列ID (默认: "DTB3")
                - moving_avg_days: SOFR移动平均天数 (默认: 20)
                - min_data_points: 最少数据点数 (默认: 5)
        """
        super().__init__(cfg)
        
        # 从配置中获取参数
        self.sofr_series = cfg.get("sofr_series", "SOFR")
        self.tbill_series = cfg.get("tbill_series", "DTB3")
        self.moving_avg_days = cfg.get("moving_avg_days", 20)
        self.min_data_points = cfg.get("min_data_points", 5)
        
        logger.info(f"初始化TED利差因子: SOFR={self.sofr_series}, T-Bill={self.tbill_series}")
    
    def fetch(self) -> pd.DataFrame:
        """
        获取TED利差相关数据
        
        注意：在实际使用中，这个方法通常返回空DataFrame，
        真正的数据获取由聚合器中的FRED客户端完成。
        
        Returns:
            包含 'date' 和 'value' 列的DataFrame
        """
        try:
            logger.info(f"获取TED利差数据: SOFR={self.sofr_series}, T-Bill={self.tbill_series}")
            
            # 在实际实现中，这里应该通过FRED客户端获取数据
            # 但为了避免循环导入，我们返回空DataFrame
            # 聚合器会注入FRED客户端并获取真实数据
            
            return pd.DataFrame(columns=['date', 'value'])
            
        except Exception as e:
            logger.error(f"获取TED利差数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        计算TED利差相关指标
        
        Args:
            df: 原始数据DataFrame，包含'date'和'value'列
                注意：这里的df应该包含计算好的TED利差值
            
        Returns:
            包含以下键的字典：
            - original_value: 最新TED利差值
            - sofr_avg: SOFR移动平均值
            - tbill_latest: 最新3个月国债收益率
            - ted_spread: TED利差值
            - spread_percentile: 利差百分位排名
            - trend: 利差趋势
        """
        if df.empty:
            logger.warning("TED利差数据为空")
            return {
                "original_value": None,
                "sofr_avg": None,
                "tbill_latest": None,
                "ted_spread": None,
                "spread_percentile": None,
                "trend": None
            }
        
        try:
            # 确保数据按日期排序
            df = df.sort_values('date').reset_index(drop=True)
            values = df['value'].dropna()
            
            if values.empty:
                return {"original_value": None}
            
            # 1. 原始值（最新TED利差）
            original_value = float(values.iloc[-1])
            
            # 2. 模拟SOFR移动平均值（实际中需要从FRED获取SOFR数据）
            # 这里我们使用TED利差数据来模拟
            if len(values) >= self.moving_avg_days:
                sofr_avg = float(values.tail(self.moving_avg_days).mean())
            else:
                sofr_avg = None
            
            # 3. 模拟3个月国债收益率（实际中需要从FRED获取DTB3数据）
            # 这里我们假设T-Bill收益率 = SOFR平均值 - TED利差
            if sofr_avg is not None:
                tbill_latest = sofr_avg - original_value
            else:
                tbill_latest = None
            
            # 4. TED利差值（与原始值相同）
            ted_spread = original_value
            
            # 5. 利差百分位排名
            if len(values) >= 10:
                spread_percentile = float((values < original_value).sum() / len(values) * 100)
            else:
                spread_percentile = None
            
            # 6. 趋势分析
            trend = None
            if len(values) >= 3:
                recent_3 = values.tail(3)
                if recent_3.iloc[-1] > recent_3.iloc[0]:
                    trend = "扩大"
                elif recent_3.iloc[-1] < recent_3.iloc[0]:
                    trend = "缩小"
                else:
                    trend = "平稳"
            
            # 7. 异常值检测
            is_outlier = False
            if len(values) >= 20:
                mean_val = values.mean()
                std_val = values.std()
                if abs(original_value - mean_val) > 2 * std_val:
                    is_outlier = True
            
            # 8. 风险等级判断
            risk_level = "正常"
            if original_value < 0.1:
                risk_level = "极低"
            elif original_value < 0.3:
                risk_level = "低"
            elif original_value < 0.8:
                risk_level = "正常"
            elif original_value < 1.2:
                risk_level = "高"
            else:
                risk_level = "极高"
            
            result = {
                "original_value": original_value,
                "sofr_avg": sofr_avg,
                "tbill_latest": tbill_latest,
                "ted_spread": ted_spread,
                "spread_percentile": spread_percentile,
                "trend": trend,
                "is_outlier": is_outlier,
                "risk_level": risk_level
            }
            
            logger.debug(f"TED利差指标计算完成: {result}")
            return result
            
        except Exception as e:
            logger.error(f"计算TED利差指标失败: {e}")
            return {"original_value": None}
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """
        计算TED利差风险评分
        
        TED利差的风险评分基于以下逻辑：
        - 低风险：TED < 0.3（银行间流动性充足）
        - 中等风险：0.3 ≤ TED < 0.8（正常水平）
        - 高风险：0.8 ≤ TED < 1.2（银行间流动性紧张）
        - 极高风险：TED ≥ 1.2（银行间流动性危机）
        
        Args:
            metrics: 计算得到的指标
            global_cfg: 全局配置，包含评分区间等
            
        Returns:
            0-100的风险评分
        """
        from core.scoring import calculate_factor_score
        
        # 使用原始值进行评分
        value = metrics.get("original_value")
        
        if value is None:
            logger.warning("TED利差值为None，返回评分0")
            return 0.0
        
        # 调用通用评分函数
        score = calculate_factor_score(self.id, value, global_cfg)
        
        # 额外的风险调整
        if metrics.get("is_outlier", False):
            # 如果是异常值，增加风险评分
            score = min(100, score + 15)
            logger.info(f"TED利差异常值检测，调整评分: {score}")
        
        if metrics.get("trend") == "扩大":
            # 如果利差扩大，增加风险评分
            score = min(100, score + 10)
            logger.info(f"TED利差扩大趋势，调整评分: {score}")
        
        # 基于百分位排名的调整
        percentile = metrics.get("spread_percentile")
        if percentile is not None:
            if percentile > 90:  # 超过90%的历史值
                score = min(100, score + 5)
                logger.info(f"TED利差百分位排名过高，调整评分: {score}")
            elif percentile < 10:  # 低于10%的历史值
                score = max(0, score - 5)
                logger.info(f"TED利差百分位排名过低，调整评分: {score}")
        
        logger.debug(f"TED利差评分: {score:.2f} (值: {value:.4f})")
        return score
    
    def get_risk_description(self, score: float) -> str:
        """
        获取风险等级描述
        
        Args:
            score: 风险评分
            
        Returns:
            风险等级描述
        """
        if score < 30:
            return "银行间流动性充足，市场稳定"
        elif score < 50:
            return "银行间流动性正常，风险可控"
        elif score < 70:
            return "银行间流动性紧张，需要关注"
        else:
            return "银行间流动性危机，风险极高"
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        验证数据质量
        
        Args:
            df: 数据DataFrame
            
        Returns:
            数据是否有效
        """
        if df.empty:
            return False
        
        # 检查必要列
        if 'date' not in df.columns or 'value' not in df.columns:
            logger.error("数据缺少必要列: date 或 value")
            return False
        
        # 检查数据类型
        try:
            pd.to_datetime(df['date'])
            pd.to_numeric(df['value'])
        except Exception as e:
            logger.error(f"数据类型转换失败: {e}")
            return False
        
        # 检查数据范围
        values = pd.to_numeric(df['value'], errors='coerce').dropna()
        if values.empty:
            return False
        
        # TED利差通常在-1到5之间
        if values.min() < -1 or values.max() > 10:
            logger.warning(f"TED利差值超出正常范围: {values.min():.4f} - {values.max():.4f}")
        
        return True
    
    def get_fred_series_ids(self) -> list:
        """
        获取需要的FRED序列ID列表
        
        Returns:
            FRED序列ID列表
        """
        return [self.sofr_series, self.tbill_series]
    
    def calculate_ted_spread(self, sofr_data: pd.Series, tbill_data: pd.Series) -> pd.DataFrame:
        """
        计算TED利差（实际实现中由聚合器调用）
        
        Args:
            sofr_data: SOFR数据序列
            tbill_data: 3个月国债数据序列
            
        Returns:
            包含TED利差的DataFrame
        """
        try:
            # 确保两个序列都有数据
            if sofr_data.empty or tbill_data.empty:
                return pd.DataFrame(columns=['date', 'value'])
            
            # 计算SOFR移动平均
            sofr_avg = sofr_data.rolling(window=self.moving_avg_days).mean()
            
            # 计算TED利差 = SOFR移动平均 - 3个月国债收益率
            ted_spread = sofr_avg - tbill_data
            
            # 创建结果DataFrame
            result = pd.DataFrame({
                'date': ted_spread.index,
                'value': ted_spread.values
            }).dropna()
            
            logger.info(f"计算TED利差完成，数据点数: {len(result)}")
            return result
            
        except Exception as e:
            logger.error(f"计算TED利差失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])


# 使用示例和测试代码
if __name__ == "__main__":
    # 创建测试数据
    import pandas as pd
    from datetime import datetime, timedelta
    
    # 生成模拟TED利差数据
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    values = [0.2 + i * 0.01 + (i % 5 - 2) * 0.05 for i in range(30)]  # 模拟TED利差数据
    
    test_data = pd.DataFrame({
        'date': dates,
        'value': values
    })
    
    # 创建TED利差因子实例
    ted_factor = TEDSpreadComplete({
        "sofr_series": "SOFR",
        "tbill_series": "DTB3",
        "moving_avg_days": 20,
        "min_data_points": 5
    })
    
    print(f"因子信息:")
    print(f"  ID: {ted_factor.id}")
    print(f"  名称: {ted_factor.name}")
    print(f"  单位: {ted_factor.units}")
    print(f"  需要的FRED序列: {ted_factor.get_fred_series_ids()}")
    
    # 验证数据
    if ted_factor.validate_data(test_data):
        print("✓ 数据验证通过")
        
        # 计算指标
        metrics = ted_factor.compute(test_data)
        print(f"\n计算指标:")
        for key, value in metrics.items():
            print(f"  {key}: {value}")
        
        # 计算评分
        global_config = {
            "bands": {
                "TED": [0.1, 1.2]  # 低风险0.1，高风险1.2
            }
        }
        score = ted_factor.score(metrics, global_config)
        print(f"\n风险评分: {score:.2f}")
        
        # 获取风险描述
        risk_desc = ted_factor.get_risk_description(score)
        print(f"风险描述: {risk_desc}")
        
    else:
        print("✗ 数据验证失败")
