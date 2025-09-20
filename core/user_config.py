"""
用户配置管理模块
处理用户偏好设置和自动确认选项
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class UserConfig:
    """用户配置管理器"""
    
    def __init__(self, config_dir: str = "config"):
        """
        初始化用户配置
        
        Args:
            config_dir: 配置文件目录
        """
        self.config_dir = Path(config_dir)
        self.user_prefs_file = self.config_dir / "user_preferences.yaml"
        self.settings_file = self.config_dir / "settings.yaml"
        
        # 默认配置
        self.default_config = {
            "user": {
                "id": "admin",
                "name": "管理员",
                "auto_confirm": True
            },
            "auto_confirm": {
                "terminal_commands": True,
                "file_operations": True,
                "code_modifications": True,
                "data_sync": True,
                "factor_calculations": True,
                "report_generation": True,
                "database_operations": True
            },
            "defaults": {
                "skip_confirmation_dialogs": True,
                "auto_proceed_on_errors": False,
                "save_results_automatically": True
            }
        }
        
        self.config = self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """加载用户配置"""
        config = self.default_config.copy()
        
        # 加载用户偏好设置
        if self.user_prefs_file.exists():
            try:
                with open(self.user_prefs_file, 'r', encoding='utf-8') as f:
                    user_prefs = yaml.safe_load(f)
                    config.update(user_prefs)
                logger.info("用户偏好设置已加载")
            except Exception as e:
                logger.warning(f"加载用户偏好设置失败: {e}")
        
        # 加载主设置文件
        if self.settings_file.exists():
            try:
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    settings = yaml.safe_load(f)
                    # 合并用户设置
                    if "user" in settings:
                        config["user"].update(settings["user"])
                logger.info("主设置文件已加载")
            except Exception as e:
                logger.warning(f"加载主设置文件失败: {e}")
        
        return config
    
    def get_user_id(self) -> str:
        """获取用户ID"""
        return self.config.get("user", {}).get("id", "admin")
    
    def get_user_name(self) -> str:
        """获取用户名称"""
        return self.config.get("user", {}).get("name", "管理员")
    
    def should_auto_confirm(self, operation_type: str = "all") -> bool:
        """
        检查是否应该自动确认操作
        
        Args:
            operation_type: 操作类型
            
        Returns:
            是否自动确认
        """
        # 检查全局自动确认设置
        if self.config.get("user", {}).get("auto_confirm", False):
            return True
        
        # 检查特定操作类型的自动确认设置
        auto_confirm_config = self.config.get("auto_confirm", {})
        
        if operation_type == "all":
            return all(auto_confirm_config.values())
        
        return auto_confirm_config.get(operation_type, True)
    
    def skip_confirmation_dialogs(self) -> bool:
        """是否跳过确认对话框"""
        return self.config.get("defaults", {}).get("skip_confirmation_dialogs", True)
    
    def auto_proceed_on_errors(self) -> bool:
        """是否在错误时自动继续"""
        return self.config.get("defaults", {}).get("auto_proceed_on_errors", False)
    
    def save_results_automatically(self) -> bool:
        """是否自动保存结果"""
        return self.config.get("defaults", {}).get("save_results_automatically", True)
    
    def get_preferred_data_drive(self) -> str:
        """获取偏好的数据存储盘"""
        return self.config.get("paths", {}).get("preferred_data_drive", "D:")
    
    def is_trusted_operation(self, operation: str) -> bool:
        """检查是否为信任的操作"""
        trusted_ops = self.config.get("security", {}).get("trusted_operations", ["all"])
        return "all" in trusted_ops or operation in trusted_ops
    
    def log_user_action(self, action: str, details: str = ""):
        """记录用户操作"""
        user_id = self.get_user_id()
        logger.info(f"用户 {user_id} 执行操作: {action}")
        if details:
            logger.info(f"操作详情: {details}")


# 全局用户配置实例
user_config = UserConfig()


def get_user_config() -> UserConfig:
    """获取全局用户配置实例"""
    return user_config


def should_auto_confirm(operation_type: str = "all") -> bool:
    """便捷函数：检查是否应该自动确认"""
    return user_config.should_auto_confirm(operation_type)


def get_user_id() -> str:
    """便捷函数：获取用户ID"""
    return user_config.get_user_id()


def log_user_action(action: str, details: str = ""):
    """便捷函数：记录用户操作"""
    user_config.log_user_action(action, details)


# 使用示例
if __name__ == "__main__":
    # 测试用户配置
    config = UserConfig()
    
    print(f"用户ID: {config.get_user_id()}")
    print(f"用户名称: {config.get_user_name()}")
    print(f"自动确认所有操作: {config.should_auto_confirm('all')}")
    print(f"自动确认终端命令: {config.should_auto_confirm('terminal_commands')}")
    print(f"跳过确认对话框: {config.skip_confirmation_dialogs()}")
    print(f"偏好数据盘: {config.get_preferred_data_drive()}")
    
    # 记录测试操作
    config.log_user_action("测试配置", "验证用户配置功能")
