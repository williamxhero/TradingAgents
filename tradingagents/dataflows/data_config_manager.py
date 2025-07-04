"""
数据配置管理器
用于管理不同数据类型的平台偏好设置
支持全局配置和特定任务配置
"""

from typing import Dict, List, Any, Optional
import json
import os
from .config import get_config, DATA_DIR


class DataConfigManager:
    """数据配置管理器"""
    
    def __init__(self, config_file: str = None):
        """
        初始化配置管理器
        
        Args:
            config_file: 配置文件路径，默认使用 data_config.json
        """
        if config_file is None:
            config_file = os.path.join(DATA_DIR, "data_config.json")
        
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"加载配置文件失败: {e}")
                return self._get_default_config()
        else:
            return self._get_default_config()
    
    def _save_config(self):
        """保存配置文件"""
        try:
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存配置文件失败: {e}")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "platform_preferences": {
                "stock_data": ["akshare", "tushare"],
                "financial_data": ["akshare", "tushare", "simfin"],
                "financial_indicators": ["akshare", "tushare"],
                "news": ["akshare", "google", "finnhub"],
                "technical_indicators": ["stockstats", "myquant"],
                "market_data": ["akshare", "myquant"],
                "macro_data": ["akshare"],
                "sentiment": ["myquant"],
                "backtest": ["myquant"]
            },
            "platform_settings": {
                "akshare": {
                    "enabled": True,
                    "priority": 1,
                    "default_params": {
                        "period": "daily",
                        "adjust": "qfq"
                    }
                },
                "tushare": {
                    "enabled": True,
                    "priority": 2,
                    "default_params": {}
                },
                "myquant": {
                    "enabled": True,
                    "priority": 3,
                    "default_params": {
                        "frequency": "1d"
                    }
                },
                "finnhub": {
                    "enabled": True,
                    "priority": 4,
                    "default_params": {}
                },
                "yfinance": {
                    "enabled": True,
                    "priority": 5,
                    "default_params": {}
                },
                "google": {
                    "enabled": True,
                    "priority": 6,
                    "default_params": {}
                },
                "reddit": {
                    "enabled": True,
                    "priority": 7,
                    "default_params": {
                        "max_limit_per_day": 10
                    }
                },
                "simfin": {
                    "enabled": True,
                    "priority": 8,
                    "default_params": {
                        "freq": "annual"
                    }
                },
                "stockstats": {
                    "enabled": True,
                    "priority": 9,
                    "default_params": {
                        "online": False
                    }
                }
            },
            "data_quality_rules": {
                "stock_data": {
                    "min_data_points": 10,
                    "required_fields": ["date", "open", "high", "low", "close", "volume"]
                },
                "financial_data": {
                    "min_data_points": 1,
                    "required_fields": ["period"]
                }
            },
            "fallback_strategy": "next_available",  # next_available, all_platforms, fail
            "cache_settings": {
                "enabled": True,
                "ttl_hours": 24
            }
        }
    
    def get_platform_preferences(self, data_type: str) -> List[str]:
        """
        获取指定数据类型的平台偏好列表
        
        Args:
            data_type: 数据类型
            
        Returns:
            List[str]: 按优先级排序的平台列表
        """
        preferences = self.config.get("platform_preferences", {})
        return preferences.get(data_type, [])
    
    def set_platform_preferences(self, data_type: str, platforms: List[str]):
        """
        设置指定数据类型的平台偏好
        
        Args:
            data_type: 数据类型
            platforms: 平台列表，按优先级排序
        """
        if "platform_preferences" not in self.config:
            self.config["platform_preferences"] = {}
        
        self.config["platform_preferences"][data_type] = platforms
        self._save_config()
    
    def get_platform_settings(self, platform: str) -> Dict[str, Any]:
        """
        获取指定平台的设置
        
        Args:
            platform: 平台名称
            
        Returns:
            Dict[str, Any]: 平台设置
        """
        settings = self.config.get("platform_settings", {})
        return settings.get(platform, {})
    
    def set_platform_settings(self, platform: str, settings: Dict[str, Any]):
        """
        设置指定平台的配置
        
        Args:
            platform: 平台名称
            settings: 平台设置
        """
        if "platform_settings" not in self.config:
            self.config["platform_settings"] = {}
        
        self.config["platform_settings"][platform] = settings
        self._save_config()
    
    def enable_platform(self, platform: str):
        """启用平台"""
        if "platform_settings" not in self.config:
            self.config["platform_settings"] = {}
        
        if platform not in self.config["platform_settings"]:
            self.config["platform_settings"][platform] = {}
        
        self.config["platform_settings"][platform]["enabled"] = True
        self._save_config()
    
    def disable_platform(self, platform: str):
        """禁用平台"""
        if "platform_settings" not in self.config:
            self.config["platform_settings"] = {}
        
        if platform not in self.config["platform_settings"]:
            self.config["platform_settings"][platform] = {}
        
        self.config["platform_settings"][platform]["enabled"] = False
        self._save_config()
    
    def is_platform_enabled(self, platform: str) -> bool:
        """检查平台是否启用"""
        settings = self.get_platform_settings(platform)
        return settings.get("enabled", True)
    
    def get_enabled_platforms(self, data_type: str = None) -> List[str]:
        """
        获取启用的平台列表
        
        Args:
            data_type: 数据类型，如果指定则返回该类型的偏好平台
            
        Returns:
            List[str]: 启用的平台列表
        """
        if data_type:
            preferences = self.get_platform_preferences(data_type)
            return [p for p in preferences if self.is_platform_enabled(p)]
        else:
            all_platforms = self.config.get("platform_settings", {}).keys()
            return [p for p in all_platforms if self.is_platform_enabled(p)]
    
    def get_platform_priority(self, platform: str) -> int:
        """获取平台优先级"""
        settings = self.get_platform_settings(platform)
        return settings.get("priority", 999)
    
    def set_platform_priority(self, platform: str, priority: int):
        """设置平台优先级"""
        if "platform_settings" not in self.config:
            self.config["platform_settings"] = {}
        
        if platform not in self.config["platform_settings"]:
            self.config["platform_settings"][platform] = {}
        
        self.config["platform_settings"][platform]["priority"] = priority
        self._save_config()
    
    def get_default_params(self, platform: str) -> Dict[str, Any]:
        """获取平台默认参数"""
        settings = self.get_platform_settings(platform)
        return settings.get("default_params", {})
    
    def set_default_params(self, platform: str, params: Dict[str, Any]):
        """设置平台默认参数"""
        if "platform_settings" not in self.config:
            self.config["platform_settings"] = {}
        
        if platform not in self.config["platform_settings"]:
            self.config["platform_settings"][platform] = {}
        
        self.config["platform_settings"][platform]["default_params"] = params
        self._save_config()
    
    def create_task_config(
        self,
        task_name: str,
        data_requests: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        创建任务配置
        
        Args:
            task_name: 任务名称
            data_requests: 数据请求列表，每个请求包含 data_type 和其他参数
            
        Returns:
            Dict[str, Any]: 任务配置
        """
        task_config = {}
        
        for i, request in enumerate(data_requests):
            data_type = request.get("data_type")
            if not data_type:
                continue
            
            # 获取该数据类型的首选平台
            preferred_platforms = self.get_enabled_platforms(data_type)
            if not preferred_platforms:
                continue
            
            platform = preferred_platforms[0]  # 使用优先级最高的平台
            
            # 合并默认参数和请求参数
            default_params = self.get_default_params(platform)
            request_params = request.copy()
            request_params.pop("data_type", None)
            
            merged_params = {**default_params, **request_params}
            
            task_key = f"{task_name}_{data_type}_{i}"
            task_config[task_key] = {
                "platform": platform,
                "data_type": data_type,
                "params": merged_params
            }
        
        return task_config
    
    def create_multi_platform_task_config(
        self,
        task_name: str,
        data_type: str,
        params: Dict[str, Any],
        platforms: List[str] = None
    ) -> Dict[str, Any]:
        """
        创建多平台对比任务配置
        
        Args:
            task_name: 任务名称
            data_type: 数据类型
            params: 通用参数
            platforms: 指定平台列表，如果为None则使用偏好设置
            
        Returns:
            Dict[str, Any]: 多平台任务配置
        """
        if platforms is None:
            platforms = self.get_enabled_platforms(data_type)
        
        task_config = {}
        
        for platform in platforms:
            if not self.is_platform_enabled(platform):
                continue
            
            # 合并默认参数和请求参数
            default_params = self.get_default_params(platform)
            merged_params = {**default_params, **params}
            
            task_key = f"{task_name}_{platform}"
            task_config[task_key] = {
                "platform": platform,
                "data_type": data_type,
                "params": merged_params
            }
        
        return task_config
    
    def get_fallback_strategy(self) -> str:
        """获取失败回退策略"""
        return self.config.get("fallback_strategy", "next_available")
    
    def set_fallback_strategy(self, strategy: str):
        """
        设置失败回退策略
        
        Args:
            strategy: 回退策略 (next_available, all_platforms, fail)
        """
        valid_strategies = ["next_available", "all_platforms", "fail"]
        if strategy not in valid_strategies:
            raise ValueError(f"无效的回退策略: {strategy}，支持的策略: {valid_strategies}")
        
        self.config["fallback_strategy"] = strategy
        self._save_config()
    
    def export_config(self, file_path: str):
        """导出配置到文件"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
            print(f"配置已导出到: {file_path}")
        except Exception as e:
            print(f"导出配置失败: {e}")
    
    def import_config(self, file_path: str):
        """从文件导入配置"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                imported_config = json.load(f)
            
            self.config.update(imported_config)
            self._save_config()
            print(f"配置已从 {file_path} 导入")
        except Exception as e:
            print(f"导入配置失败: {e}")
    
    def reset_to_default(self):
        """重置为默认配置"""
        self.config = self._get_default_config()
        self._save_config()
        print("配置已重置为默认值")
    
    def print_config_summary(self):
        """打印配置摘要"""
        print("=== 数据配置摘要 ===")
        
        print("\n平台状态:")
        for platform, settings in self.config.get("platform_settings", {}).items():
            status = "启用" if settings.get("enabled", True) else "禁用"
            priority = settings.get("priority", 999)
            print(f"  {platform}: {status} (优先级: {priority})")
        
        print("\n数据类型偏好:")
        for data_type, platforms in self.config.get("platform_preferences", {}).items():
            enabled_platforms = [p for p in platforms if self.is_platform_enabled(p)]
            print(f"  {data_type}: {' -> '.join(enabled_platforms)}")
        
        print(f"\n回退策略: {self.get_fallback_strategy()}")
        
        cache_settings = self.config.get("cache_settings", {})
        cache_status = "启用" if cache_settings.get("enabled", True) else "禁用"
        cache_ttl = cache_settings.get("ttl_hours", 24)
        print(f"缓存设置: {cache_status} (TTL: {cache_ttl}小时)")


# 创建全局配置管理器实例
config_manager = DataConfigManager()


# 便捷函数
def get_preferred_platforms(data_type: str) -> List[str]:
    """获取数据类型的偏好平台列表"""
    return config_manager.get_enabled_platforms(data_type)


def set_platform_preference(data_type: str, platforms: List[str]):
    """设置数据类型的平台偏好"""
    config_manager.set_platform_preferences(data_type, platforms)


def enable_platform(platform: str):
    """启用平台"""
    config_manager.enable_platform(platform)


def disable_platform(platform: str):
    """禁用平台"""
    config_manager.disable_platform(platform)


def create_task_config(task_name: str, data_requests: List[Dict[str, Any]]) -> Dict[str, Any]:
    """创建任务配置"""
    return config_manager.create_task_config(task_name, data_requests)


def print_config_summary():
    """打印配置摘要"""
    config_manager.print_config_summary()
