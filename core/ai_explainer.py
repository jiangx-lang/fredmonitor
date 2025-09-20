"""
AI解读器

提供AI辅助的分析解读功能。
"""

import os
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class AIExplainer:
    """AI解读器"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化AI解读器
        
        Args:
            config: AI配置
        """
        self.config = config
        self.enabled = config.get("enable_ai_commentary", False)
        self.language = config.get("language", "zh-CN")
        self.temperature = config.get("temperature", 0.4)
        self.max_tokens = config.get("max_tokens", 1000)
        
        # 初始化AI客户端
        self.client = None
        if self.enabled:
            self._init_ai_client()
    
    def _init_ai_client(self) -> None:
        """初始化AI客户端"""
        try:
            provider = os.getenv("AI_PROVIDER", "openai")
            
            if provider == "openai":
                from openai import OpenAI
                api_key = os.getenv("AI_API_KEY")
                if api_key:
                    self.client = OpenAI(api_key=api_key)
                    logger.info("OpenAI客户端初始化成功")
                else:
                    logger.warning("未设置AI_API_KEY，AI功能将不可用")
                    self.enabled = False
            else:
                logger.warning(f"不支持的AI提供商: {provider}")
                self.enabled = False
                
        except ImportError:
            logger.warning("未安装OpenAI库，AI功能将不可用")
            self.enabled = False
        except Exception as e:
            logger.error(f"初始化AI客户端失败: {e}")
            self.enabled = False
    
    def generate_overall_commentary(self, analysis_result: Dict[str, Any]) -> Optional[str]:
        """
        生成总体解读
        
        Args:
            analysis_result: 分析结果
            
        Returns:
            解读文本或None
        """
        if not self.enabled or not self.client:
            return None
        
        try:
            # 构建提示词
            prompt = self._build_overall_prompt(analysis_result)
            
            # 调用AI API
            response = self.client.chat.completions.create(
                model=os.getenv("AI_MODEL", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": "你是一个专业的宏观经济分析师，擅长解读金融市场风险指标。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            commentary = response.choices[0].message.content
            logger.info("生成AI总体解读成功")
            return commentary
            
        except Exception as e:
            logger.error(f"生成AI总体解读失败: {e}")
            return None
    
    def generate_factor_commentary(self, factor_id: str, factor_data: Dict[str, Any]) -> Optional[str]:
        """
        生成因子解读
        
        Args:
            factor_id: 因子ID
            factor_data: 因子数据
            
        Returns:
            解读文本或None
        """
        if not self.enabled or not self.client:
            return None
        
        try:
            # 构建提示词
            prompt = self._build_factor_prompt(factor_id, factor_data)
            
            # 调用AI API
            response = self.client.chat.completions.create(
                model=os.getenv("AI_MODEL", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": "你是一个专业的宏观经济分析师，擅长解读单个金融指标。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=200  # 因子解读较短
            )
            
            commentary = response.choices[0].message.content
            logger.info(f"生成AI因子解读成功: {factor_id}")
            return commentary
            
        except Exception as e:
            logger.error(f"生成AI因子解读失败 {factor_id}: {e}")
            return None
    
    def _build_overall_prompt(self, analysis_result: Dict[str, Any]) -> str:
        """构建总体解读提示词"""
        date = analysis_result["date"].strftime("%Y年%m月%d日")
        total_score = analysis_result["total_score"]
        risk_level = analysis_result["risk_level"]
        
        factor_scores = analysis_result["factor_scores"]
        factor_values = analysis_result["factor_values"]
        
        # 构建因子信息
        factor_info = []
        for factor_id, score in factor_scores.items():
            value = factor_values.get(factor_id, "N/A")
            if isinstance(value, float):
                value_str = f"{value:.4f}"
            else:
                value_str = str(value)
            factor_info.append(f"- {factor_id}: {value_str} (评分: {score:.2f})")
        
        factor_text = "\n".join(factor_info)
        
        prompt = f"""
请基于以下宏观金融风险分析结果，生成一份300-500字的专业解读报告：

分析日期：{date}
综合风险评分：{total_score:.2f}
风险等级：{risk_level}

各因子详细情况：
{factor_text}

请从以下角度进行分析：
1. 当前整体市场风险状况
2. 主要风险来源和驱动因素
3. 各因子间的相互影响
4. 对未来市场走势的启示
5. 投资建议和风险提示

要求：
- 语言专业但易懂
- 逻辑清晰，层次分明
- 基于数据客观分析
- 避免过度解读
"""
        return prompt
    
    def _build_factor_prompt(self, factor_id: str, factor_data: Dict[str, Any]) -> str:
        """构建因子解读提示词"""
        value = factor_data.get("original_value", "N/A")
        score = factor_data.get("score", 0)
        
        if isinstance(value, float):
            value_str = f"{value:.4f}"
        else:
            value_str = str(value)
        
        prompt = f"""
请对以下宏观金融指标进行50-120字的专业解读：

因子：{factor_id}
当前值：{value_str}
风险评分：{score:.2f}

请简要说明：
1. 该指标的含义
2. 当前值的市场含义
3. 风险水平评估
4. 对市场的影响

要求简洁明了，突出重点。
"""
        return prompt
