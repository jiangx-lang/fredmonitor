"""
VIX波动率因子 - 完整实现示例

基于VIXCLS序列计算市场恐慌指数，展示完整的因子实现模式。
"""

import pandas as pd
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta

from .base_factor import Factor

logger = logging.getLogger(__name__)


class VIXComplete(Factor):
    """VIX波动率因子 - 完整实现"""
    
    id = "VIX"
    name = "VIX 波动率"
    units = "指数"
    
    def __init__(self, cfg: Dict[str, Any]):
        """
        初始化VIX因子
        
        Args:
            cfg: 因子配置参数
                - series_id: FRED序列ID (默认: "VIXCLS")
                - moving_avg_days: 移动平均天数 (默认: 5)
                - volatility_window: 波动率计算窗口 (默认: 20)
        """
        super().__init__(cfg)
        
        # 从配置中获取参数
        self.series_id = cfg.get("series_id", "VIXCLS")
        self.moving_avg_days = cfg.get("moving_avg_days", 5)
        self.volatility_window = cfg.get("volatility_window", 20)
        
        logger.info(f"初始化VIX因子: {self.series_id}")
    
    def fetch(self) -> pd.DataFrame:
        """
        获取VIX原始数据
        
        注意：在实际使用中，这个方法通常返回空DataFrame，
        真正的数据获取由聚合器中的FRED客户端完成。
        
        Returns:
            包含 'date' 和 'value' 列的DataFrame
        """
        try:
            logger.info(f"获取VIX数据: {self.series_id}")
            
            # 在实际实现中，这里应该通过FRED客户端获取数据
            # 但为了避免循环导入，我们返回空DataFrame
            # 聚合器会注入FRED客户端并获取真实数据
            
            return pd.DataFrame(columns=['date', 'value'])
            
        except Exception as e:
            logger.error(f"获取VIX数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        计算VIX相关指标
        
        Args:
            df: 原始数据DataFrame，包含'date'和'value'列
            
        Returns:
            包含以下键的字典：
            - original_value: 最新VIX值
            - moving_avg: 移动平均值
            - volatility: 历史波动率
            - percentile_rank: 百分位排名
            - trend: 趋势方向
        """
        if df.empty:
            logger.warning("VIX数据为空")
            return {
                "original_value": None,
                "moving_avg": None,
                "volatility": None,
                "percentile_rank": None,
                "trend": None
            }
        
        try:
            # 确保数据按日期排序
            df = df.sort_values('date').reset_index(drop=True)
            values = df['value'].dropna()
            
            if values.empty:
                return {"original_value": None}
            
            # 1. 原始值（最新值）
            original_value = float(values.iloc[-1])
            
            # 2. 移动平均值
            if len(values) >= self.moving_avg_days:
                moving_avg = float(values.tail(self.moving_avg_days).mean())
            else:
                moving_avg = None
            
            # 3. 历史波动率（标准差）
            if len(values) >= self.volatility_window:
                volatility = float(values.tail(self.volatility_window).std())
            else:
                volatility = None
            
            # 4. 百分位排名（当前值在历史数据中的位置）
            if len(values) >= 10:  # 至少需要10个数据点
                percentile_rank = float((values < original_value).sum() / len(values) * 100)
            else:
                percentile_rank = None
            
            # 5. 趋势分析
            trend = None
            if len(values) >= 3:
                recent_3 = values.tail(3)
                if recent_3.iloc[-1] > recent_3.iloc[0]:
                    trend = "上升"
                elif recent_3.iloc[-1] < recent_3.iloc[0]:
                    trend = "下降"
                else:
                    trend = "平稳"
            
            # 6. 异常值检测
            is_outlier = False
            if len(values) >= 20:  # 至少需要20个数据点
                mean_val = values.mean()
                std_val = values.std()
                if abs(original_value - mean_val) > 2 * std_val:
                    is_outlier = True
            
            result = {
                "original_value": original_value,
                "moving_avg": moving_avg,
                "volatility": volatility,
                "percentile_rank": percentile_rank,
                "trend": trend,
                "is_outlier": is_outlier
            }
            
            logger.debug(f"VIX指标计算完成: {result}")
            return result
            
        except Exception as e:
            logger.error(f"计算VIX指标失败: {e}")
            return {"original_value": None}
    
    def score(self, metrics: Dict[str, Any], global_cfg: Dict[str, Any]) -> float:
        """
        计算VIX风险评分
        
        VIX的风险评分基于以下逻辑：
        - 低风险：VIX < 15（市场平静）
        - 中等风险：15 ≤ VIX < 25（正常波动）
        - 高风险：25 ≤ VIX < 35（市场紧张）
        - 极高风险：VIX ≥ 35（市场恐慌）
        
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
            logger.warning("VIX值为None，返回评分0")
            return 0.0
        
        # 调用通用评分函数
        score = calculate_factor_score(self.id, value, global_cfg)
        
        # 额外的风险调整
        if metrics.get("is_outlier", False):
            # 如果是异常值，增加风险评分
            score = min(100, score + 10)
            logger.info(f"VIX异常值检测，调整评分: {score}")
        
        if metrics.get("trend") == "上升":
            # 如果趋势上升，略微增加风险评分
            score = min(100, score + 5)
            logger.info(f"VIX上升趋势，调整评分: {score}")
        
        logger.debug(f"VIX评分: {score:.2f} (值: {value:.2f})")
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
            return "市场平静，投资者情绪稳定"
        elif score < 50:
            return "市场正常波动，风险可控"
        elif score < 70:
            return "市场紧张，需要关注风险"
        else:
            return "市场恐慌，风险极高"
    
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
        
        # VIX通常在0-100之间
        if values.min() < 0 or values.max() > 200:
            logger.warning(f"VIX值超出正常范围: {values.min():.2f} - {values.max():.2f}")
        
        return True


# 使用示例和测试代码
if __name__ == "__main__":
    # 创建测试数据
    import pandas as pd
    from datetime import datetime, timedelta
    
    # 生成模拟VIX数据
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    values = [15 + i * 0.5 + (i % 7 - 3) * 2 for i in range(30)]  # 模拟VIX数据
    
    test_data = pd.DataFrame({
        'date': dates,
        'value': values
    })
    
    # 创建VIX因子实例
    vix_factor = VIXComplete({
        "series_id": "VIXCLS",
        "moving_avg_days": 5,
        "volatility_window": 20
    })
    
    print(f"因子信息:")
    print(f"  ID: {vix_factor.id}")
    print(f"  名称: {vix_factor.name}")
    print(f"  单位: {vix_factor.units}")
    
    # 验证数据
    if vix_factor.validate_data(test_data):
        print("✓ 数据验证通过")
        
        # 计算指标
        metrics = vix_factor.compute(test_data)
        print(f"\n计算指标:")
        for key, value in metrics.items():
            print(f"  {key}: {value}")
        
        # 计算评分
        global_config = {
            "bands": {
                "VIX": [12, 30]  # 低风险12，高风险30
            }
        }
        score = vix_factor.score(metrics, global_config)
        print(f"\n风险评分: {score:.2f}")
        
        # 获取风险描述
        risk_desc = vix_factor.get_risk_description(score)
        print(f"风险描述: {risk_desc}")
        
    else:
        print("✗ 数据验证失败")
