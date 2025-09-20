"""
自动确认装饰器和工具函数
用于自动确认所有需要用户确认的操作
"""

import functools
import logging
from typing import Callable, Any, Optional
from .user_config import should_auto_confirm, get_user_id, log_user_action

logger = logging.getLogger(__name__)


def auto_confirm(operation_type: str = "all", message: str = ""):
    """
    自动确认装饰器
    
    Args:
        operation_type: 操作类型
        message: 确认消息
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            user_id = get_user_id()
            
            # 检查是否需要自动确认
            if should_auto_confirm(operation_type):
                logger.info(f"用户 {user_id} 自动确认操作: {func.__name__}")
                if message:
                    logger.info(f"操作说明: {message}")
                
                # 记录操作
                log_user_action(f"自动确认: {func.__name__}", message)
                
                # 直接执行操作
                return func(*args, **kwargs)
            else:
                # 需要手动确认的情况（虽然用户设置了自动确认，但保留这个逻辑）
                logger.info(f"用户 {user_id} 手动确认操作: {func.__name__}")
                return func(*args, **kwargs)
        
        return wrapper
    return decorator


def confirm_operation(operation_type: str = "all", message: str = "") -> bool:
    """
    确认操作（总是返回True，因为用户设置了自动确认）
    
    Args:
        operation_type: 操作类型
        message: 确认消息
        
    Returns:
        总是返回True（自动确认）
    """
    user_id = get_user_id()
    
    if should_auto_confirm(operation_type):
        logger.info(f"用户 {user_id} 自动确认: {message}")
        log_user_action("自动确认操作", f"{operation_type}: {message}")
        return True
    else:
        # 理论上不会执行到这里，因为用户设置了自动确认
        logger.warning(f"用户 {user_id} 需要手动确认: {message}")
        return True


def safe_execute(func: Callable, *args, operation_type: str = "all", 
                error_message: str = "", **kwargs) -> Any:
    """
    安全执行函数，自动处理确认和错误
    
    Args:
        func: 要执行的函数
        *args: 函数参数
        operation_type: 操作类型
        error_message: 错误消息
        **kwargs: 函数关键字参数
        
    Returns:
        函数执行结果
    """
    user_id = get_user_id()
    
    try:
        # 自动确认
        if should_auto_confirm(operation_type):
            logger.info(f"用户 {user_id} 安全执行: {func.__name__}")
            log_user_action("安全执行", f"{func.__name__}: {operation_type}")
            
            # 执行函数
            return func(*args, **kwargs)
        else:
            # 理论上不会执行到这里
            logger.warning(f"用户 {user_id} 需要手动确认执行: {func.__name__}")
            return func(*args, **kwargs)
            
    except Exception as e:
        error_msg = error_message or f"执行 {func.__name__} 时发生错误"
        logger.error(f"{error_msg}: {e}")
        log_user_action("执行错误", f"{func.__name__}: {error_msg}")
        
        # 根据用户配置决定是否继续
        if should_auto_confirm("auto_proceed_on_errors"):
            logger.info(f"用户 {user_id} 设置自动继续，忽略错误")
            return None
        else:
            raise


def log_and_execute(func: Callable, *args, operation_name: str = "", **kwargs) -> Any:
    """
    记录并执行操作
    
    Args:
        func: 要执行的函数
        *args: 函数参数
        operation_name: 操作名称
        **kwargs: 函数关键字参数
        
    Returns:
        函数执行结果
    """
    user_id = get_user_id()
    op_name = operation_name or func.__name__
    
    logger.info(f"用户 {user_id} 执行操作: {op_name}")
    log_user_action("执行操作", op_name)
    
    try:
        result = func(*args, **kwargs)
        logger.info(f"操作 {op_name} 执行成功")
        log_user_action("操作成功", op_name)
        return result
    except Exception as e:
        logger.error(f"操作 {op_name} 执行失败: {e}")
        log_user_action("操作失败", f"{op_name}: {e}")
        raise


# 便捷函数
def auto_confirm_terminal_command():
    """自动确认终端命令"""
    return confirm_operation("terminal_commands", "执行终端命令")


def auto_confirm_file_operation():
    """自动确认文件操作"""
    return confirm_operation("file_operations", "执行文件操作")


def auto_confirm_code_modification():
    """自动确认代码修改"""
    return confirm_operation("code_modifications", "修改代码")


def auto_confirm_data_sync():
    """自动确认数据同步"""
    return confirm_operation("data_sync", "同步数据")


def auto_confirm_factor_calculation():
    """自动确认因子计算"""
    return confirm_operation("factor_calculations", "计算因子")


def auto_confirm_report_generation():
    """自动确认报告生成"""
    return confirm_operation("report_generation", "生成报告")


def auto_confirm_database_operation():
    """自动确认数据库操作"""
    return confirm_operation("database_operations", "数据库操作")


# 使用示例
if __name__ == "__main__":
    # 测试自动确认功能
    
    @auto_confirm("terminal_commands", "测试终端命令")
    def test_command():
        print("执行测试命令")
        return "命令执行成功"
    
    # 测试安全执行
    def risky_operation():
        print("执行风险操作")
        return "操作完成"
    
    # 测试各种确认函数
    print("测试自动确认功能:")
    print(f"终端命令确认: {auto_confirm_terminal_command()}")
    print(f"文件操作确认: {auto_confirm_file_operation()}")
    print(f"代码修改确认: {auto_confirm_code_modification()}")
    
    # 测试装饰器
    result = test_command()
    print(f"装饰器测试结果: {result}")
    
    # 测试安全执行
    result = safe_execute(risky_operation, operation_type="all", error_message="测试错误处理")
    print(f"安全执行结果: {result}")
