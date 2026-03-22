#!/usr/bin/env python3
"""
FRED官方REST API轻量客户端

直接调用FRED官方API，避免第三方库的不稳定性。
"""

import os
import time
import threading
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from typing import Dict, Any, Optional
from tenacity import retry, wait_exponential, stop_after_attempt

logger = logging.getLogger(__name__)

# FRED API配置
API_ROOT = "https://api.stlouisfed.org/fred"

# 工业级重试：连接/读超时与 429/5xx 由 urllib3 在单次 Session 内指数退避重试
_FRED_HTTP_SESSION: Optional[requests.Session] = None


def _fred_http_session() -> requests.Session:
    global _FRED_HTTP_SESSION
    if _FRED_HTTP_SESSION is not None:
        return _FRED_HTTP_SESSION
    session = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_maxsize=16)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    _FRED_HTTP_SESSION = session
    return session

def get_api_key():
    """获取API密钥"""
    from dotenv import load_dotenv
    # 确保从项目根目录加载环境变量
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "macrolab.env")
    load_dotenv(env_path)
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError("FRED_API_KEY 环境变量未设置")
    return api_key

API_KEY = get_api_key()


class RateLimiter:
    """线程安全的速率限制器"""
    def __init__(self, min_interval_sec=0.2):
        self.min_interval = min_interval_sec
        self._lock = threading.Lock()
        self._last = 0.0
    
    def wait(self):
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.time()


# 全局速率限制器
RATE_LIMITER = RateLimiter(0.2)


def _params(extra: Dict[str, Any]) -> Dict[str, Any]:
    """构建API请求参数"""
    base = {
        "api_key": API_KEY,
        "file_type": "json"  # 明确要求JSON格式，避免默认XML
    }
    base.update(extra or {})
    return base


def _get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """发送GET请求到FRED API（速率限制 + Session 级 urllib3 重试 + 较长超时）"""
    RATE_LIMITER.wait()  # 全局速率限制

    url = f"{API_ROOT}/{path}"

    try:
        response = _fred_http_session().get(url, params=_params(params), timeout=(15, 60))
        response.raise_for_status()

        data = response.json()

        if "error_message" in data:
            raise ValueError(f"FRED API错误: {data['error_message']}")

        return data

    except requests.exceptions.RequestException as e:
        logger.debug("FRED _get failed after session retries: %s %s", path, e)
        raise ConnectionError(f"FRED API请求失败: {e}")


@retry(
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5)
)
def series_info(series_id: str) -> Dict[str, Any]:
    """
    获取序列元数据
    
    Args:
        series_id: FRED序列ID
        
    Returns:
        包含序列元数据的字典
    """
    return _get("series", {"series_id": series_id})


@retry(
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5)
)
def series_observations(
    series_id: str,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    frequency: Optional[str] = None,
    units: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    获取序列观测数据
    
    Args:
        series_id: FRED序列ID
        observation_start: 开始日期 (YYYY-MM-DD)
        observation_end: 结束日期 (YYYY-MM-DD)
        frequency: 数据频率 (d, w, m, q, sa, a)
        units: 数据单位 (lin, chg, ch1, pch, pc1, pca, cch, cch1, cca, log, log1)
        **kwargs: 其他参数
        
    Returns:
        包含观测数据的字典
    """
    params = {"series_id": series_id}
    
    if observation_start:
        params["observation_start"] = observation_start
    if observation_end:
        params["observation_end"] = observation_end
    if frequency:
        params["frequency"] = frequency
    if units:
        params["units"] = units
    
    params.update(kwargs)
    
    return _get("series/observations", params)


@retry(
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5)
)
def series_search(search_text: str, limit: int = 5, order_by: str = "popularity") -> dict:
    """搜索FRED系列"""
    return _get("series/search", {"search_text": search_text, "limit": limit, "order_by": order_by})


@retry(
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5)
)
def series_release(series_id: str) -> Dict[str, Any]:
    """
    获取序列所属的发布信息
    
    Args:
        series_id: FRED序列ID
        
    Returns:
        包含发布信息的字典
    """
    return _get("series/release", {"series_id": series_id})


@retry(
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5)
)
def release_dates(
    release_id: int,
    include_release_dates_with_no_data: bool = True,
    **kwargs
) -> Dict[str, Any]:
    """
    获取发布日期的历史记录
    
    Args:
        release_id: 发布ID
        include_release_dates_with_no_data: 是否包含无数据的发布日期
        **kwargs: 其他参数
        
    Returns:
        包含发布日期的字典
    """
    params = {
        "release_id": release_id,
        "include_release_dates_with_no_data": str(include_release_dates).lower()
    }
    params.update(kwargs)
    
    return _get("release/dates", params)


def polite_sleep():
    """
    礼貌性延迟，避免触发API限流
    
    FRED API限流约为120请求/分钟，这里使用0.25秒间隔
    相当于240请求/分钟，留有安全边际
    """
    time.sleep(0.25)


def get_next_release_date(series_id: str) -> str:
    """
    健壮版：查询序列对应的下一次发布日期
    
    - 无 release / 网络失败 → 返回 "N/A"
    - 有日期列表 → 选择 > 今天 的最早一个
    - 全程捕获异常，不阻塞主流程
    """
    try:
        # 获取序列的发布信息
        rel = series_release(series_id)
        releases = rel.get("releases") or []
        
        if not releases:
            return "N/A"
        
        rid = releases[0].get("id")
        if not rid:
            return "N/A"
        
        # 获取发布日期
        rd = release_dates(rid, include_release_dates_with_no_data=True)
        items = rd.get("release_dates", []) or []
        
        if not items:
            return "N/A"
        
        # 找到今天之后的最早发布日期
        today = pd.Timestamp.today().date()
        future = sorted(
            d["date"] for d in items 
            if pd.to_datetime(d["date"]).date() > today
        )
        
        return future[0] if future else "N/A"
        
    except Exception:
        # 静默失败，不打印错误信息避免日志污染
        return "N/A"


# === Category APIs ===
@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5))
def category(category_id: int) -> dict:
    """返回 {"category": {...}}"""
    return _get("category", {"category_id": category_id})

@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5))
def category_children(category_id: int) -> dict:
    """返回 {"categories": [{id,name,...}, ...]}"""
    return _get("category/children", {"category_id": category_id})

@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5))
def category_series(category_id: int, **kwargs) -> dict:
    """
    常用参数：
      order_by=popularity|series_id|title|last_updated
      sort_order=asc|desc
      limit=1000, offset=0
      filter_variable=frequency|units|seasonal_adjustment
      filter_value=M|Q|D|W|NSA|SA|Percent|Index|...
      tag_names=United States;Seasonally Adjusted
    返回 {"seriess": [{id,title,frequency,units,...}, ...]}
    """
    params = {"category_id": category_id}
    params.update(kwargs or {})
    return _get("category/series", params)


def test_connection() -> bool:
    """
    测试FRED API连接
    
    Returns:
        连接是否成功
    """
    try:
        # 测试获取VIX数据
        vix_info = series_info("VIXCLS")
        if "seriess" in vix_info and vix_info["seriess"]:
            print("✓ FRED API连接成功")
            return True
        else:
            print("✗ FRED API响应格式异常")
            return False
    except Exception as e:
        print(f"✗ FRED API连接失败: {e}")
        return False


# === Category APIs ===
@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5))
def category(category_id: int) -> dict:
    """返回 {"category": {...}}"""
    return _get("category", {"category_id": category_id})

@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5))
def category_children(category_id: int) -> dict:
    """返回 {"categories": [{id,name,...}, ...]}"""
    return _get("category/children", {"category_id": category_id})

@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5))
def category_series(category_id: int, **kwargs) -> dict:
    """
    常用参数：
      order_by=popularity|series_id|title|last_updated
      sort_order=asc|desc
      limit=1000, offset=0
      filter_variable=frequency|units|seasonal_adjustment
      filter_value=M|Q|D|W|NSA|SA|Percent|Index|...
      tag_names=United States;Seasonally Adjusted
    返回 {"seriess": [{id,title,frequency,units,...}, ...]}
    """
    params = {"category_id": category_id}
    params.update(kwargs or {})
    return _get("category/series", params)


if __name__ == "__main__":
    # 测试连接
    if test_connection():
        print("FRED HTTP客户端测试通过")
    else:
        print("FRED HTTP客户端测试失败")
