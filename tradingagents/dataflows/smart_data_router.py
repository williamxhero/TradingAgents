"""
智能数据路由器
根据配置自动选择最佳数据平台，提供失败回退和数据质量验证
"""

from typing import Dict, List, Any, Optional, Tuple
import time
import logging
from datetime import datetime
from .unified_data_manager import unified_manager
from .data_config_manager import config_manager


class SmartDataRouter:
    """智能数据路由器"""
    
    def __init__(self):
        """初始化路由器"""
        self.logger = logging.getLogger(__name__)
        self.request_history = []
        self.platform_performance = {}
    
    def route_data_request(
        self,
        data_type: str,
        params: Dict[str, Any],
        preferred_platforms: List[str] = None,
        enable_fallback: bool = True,
        quality_check: bool = True
    ) -> Dict[str, Any]:
        """
        智能路由数据请求
        
        Args:
            data_type: 数据类型
            params: 请求参数
            preferred_platforms: 指定平台列表，如果为None则使用配置
            enable_fallback: 是否启用失败回退
            quality_check: 是否进行数据质量检查
            
        Returns:
            Dict[str, Any]: 包含数据、元信息和性能统计的结果
        """
        start_time = time.time()
        
        # 获取平台列表
        if preferred_platforms is None:
            preferred_platforms = config_manager.get_enabled_platforms(data_type)
        
        if not preferred_platforms:
            return {
                "success": False,
                "error": f"没有可用的平台支持数据类型: {data_type}",
                "data": None,
                "metadata": {
                    "data_type": data_type,
                    "platforms_tried": [],
                    "execution_time": time.time() - start_time
                }
            }
        
        # 按优先级排序平台
        sorted_platforms = self._sort_platforms_by_priority(preferred_platforms)
        
        # 尝试获取数据
        result = self._try_platforms(data_type, params, sorted_platforms, enable_fallback, quality_check)
        
        # 记录请求历史
        execution_time = time.time() - start_time
        self._record_request(data_type, result["metadata"]["platforms_tried"], result["success"], execution_time)
        
        result["metadata"]["execution_time"] = execution_time
        return result
    
    def _sort_platforms_by_priority(self, platforms: List[str]) -> List[str]:
        """根据优先级和性能排序平台"""
        def platform_score(platform: str) -> Tuple[int, float]:
            # 优先级（数字越小优先级越高）
            priority = config_manager.get_platform_priority(platform)
            
            # 性能得分（成功率 * 平均响应时间的倒数）
            perf = self.platform_performance.get(platform, {"success_rate": 0.5, "avg_time": 5.0})
            performance_score = perf["success_rate"] / max(perf["avg_time"], 0.1)
            
            return (priority, -performance_score)  # 负号使得性能好的排在前面
        
        return sorted(platforms, key=platform_score)
    
    def _try_platforms(
        self,
        data_type: str,
        params: Dict[str, Any],
        platforms: List[str],
        enable_fallback: bool,
        quality_check: bool
    ) -> Dict[str, Any]:
        """尝试从多个平台获取数据"""
        platforms_tried = []
        errors = []
        
        for platform in platforms:
            if not config_manager.is_platform_enabled(platform):
                continue
            
            platforms_tried.append(platform)
            
            try:
                # 合并默认参数
                default_params = config_manager.get_default_params(platform)
                merged_params = {**default_params, **params}
                
                # 获取数据
                platform_start_time = time.time()
                data = unified_manager.get_data(platform, data_type, **merged_params)
                platform_time = time.time() - platform_start_time
                
                # 更新平台性能统计
                self._update_platform_performance(platform, True, platform_time)
                
                # 数据质量检查
                if quality_check and not self._validate_data_quality(data, data_type):
                    error_msg = f"平台 {platform} 返回的数据质量不符合要求"
                    errors.append(error_msg)
                    self.logger.warning(error_msg)
                    
                    if not enable_fallback:
                        return {
                            "success": False,
                            "error": error_msg,
                            "data": data,
                            "metadata": {
                                "data_type": data_type,
                                "platform_used": platform,
                                "platforms_tried": platforms_tried,
                                "errors": errors,
                                "platform_time": platform_time
                            }
                        }
                    continue
                
                # 成功获取数据
                return {
                    "success": True,
                    "data": data,
                    "metadata": {
                        "data_type": data_type,
                        "platform_used": platform,
                        "platforms_tried": platforms_tried,
                        "platform_time": platform_time,
                        "quality_passed": quality_check
                    }
                }
                
            except Exception as e:
                error_msg = f"平台 {platform} 获取数据失败: {str(e)}"
                errors.append(error_msg)
                self.logger.error(error_msg)
                
                # 更新平台性能统计
                self._update_platform_performance(platform, False, 0)
                
                if not enable_fallback:
                    return {
                        "success": False,
                        "error": error_msg,
                        "data": None,
                        "metadata": {
                            "data_type": data_type,
                            "platform_used": platform,
                            "platforms_tried": platforms_tried,
                            "errors": errors
                        }
                    }
        
        # 所有平台都失败了
        return {
            "success": False,
            "error": f"所有平台都无法获取 {data_type} 数据",
            "data": None,
            "metadata": {
                "data_type": data_type,
                "platforms_tried": platforms_tried,
                "errors": errors
            }
        }
    
    def _validate_data_quality(self, data: str, data_type: str) -> bool:
        """验证数据质量"""
        if not data or data.strip() == "":
            return False
        
        # 检查是否包含错误信息
        error_indicators = ["错误", "失败", "Error", "Failed", "未获取到", "No data"]
        for indicator in error_indicators:
            if indicator in data:
                return False
        
        # 根据数据类型进行特定检查
        quality_rules = config_manager.config.get("data_quality_rules", {})
        type_rules = quality_rules.get(data_type, {})
        
        # 检查最小数据点数量（简单的行数检查）
        min_data_points = type_rules.get("min_data_points", 1)
        data_lines = [line for line in data.split('\n') if line.strip()]
        if len(data_lines) < min_data_points:
            return False
        
        # 检查必需字段（简单的关键词检查）
        required_fields = type_rules.get("required_fields", [])
        for field in required_fields:
            if field not in data:
                return False
        
        return True
    
    def _update_platform_performance(self, platform: str, success: bool, response_time: float):
        """更新平台性能统计"""
        if platform not in self.platform_performance:
            self.platform_performance[platform] = {
                "total_requests": 0,
                "successful_requests": 0,
                "total_time": 0.0,
                "success_rate": 0.0,
                "avg_time": 0.0
            }
        
        perf = self.platform_performance[platform]
        perf["total_requests"] += 1
        
        if success:
            perf["successful_requests"] += 1
            perf["total_time"] += response_time
        
        # 更新统计指标
        perf["success_rate"] = perf["successful_requests"] / perf["total_requests"]
        if perf["successful_requests"] > 0:
            perf["avg_time"] = perf["total_time"] / perf["successful_requests"]
    
    def _record_request(self, data_type: str, platforms_tried: List[str], success: bool, execution_time: float):
        """记录请求历史"""
        record = {
            "timestamp": datetime.now().isoformat(),
            "data_type": data_type,
            "platforms_tried": platforms_tried,
            "success": success,
            "execution_time": execution_time
        }
        
        self.request_history.append(record)
        
        # 保持历史记录在合理范围内
        if len(self.request_history) > 1000:
            self.request_history = self.request_history[-500:]
    
    def get_multi_platform_data(
        self,
        data_type: str,
        params: Dict[str, Any],
        platforms: List[str] = None,
        parallel: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """
        获取多平台数据对比
        
        Args:
            data_type: 数据类型
            params: 请求参数
            platforms: 指定平台列表
            parallel: 是否并行请求（暂未实现）
            
        Returns:
            Dict[str, Dict[str, Any]]: 各平台的结果
        """
        if platforms is None:
            platforms = config_manager.get_enabled_platforms(data_type)
        
        results = {}
        
        for platform in platforms:
            if not config_manager.is_platform_enabled(platform):
                results[platform] = {
                    "success": False,
                    "error": f"平台 {platform} 已禁用",
                    "data": None
                }
                continue
            
            # 为每个平台单独路由请求
            result = self.route_data_request(
                data_type=data_type,
                params=params,
                preferred_platforms=[platform],
                enable_fallback=False,
                quality_check=True
            )
            
            results[platform] = result
        
        return results
    
    def get_best_platform_for_data_type(self, data_type: str) -> Optional[str]:
        """获取指定数据类型的最佳平台"""
        platforms = config_manager.get_enabled_platforms(data_type)
        if not platforms:
            return None
        
        sorted_platforms = self._sort_platforms_by_priority(platforms)
        return sorted_platforms[0] if sorted_platforms else None
    
    def get_platform_performance_report(self) -> Dict[str, Any]:
        """获取平台性能报告"""
        report = {
            "performance_summary": self.platform_performance.copy(),
            "total_requests": len(self.request_history),
            "recent_requests": self.request_history[-10:] if self.request_history else [],
            "platform_rankings": {}
        }
        
        # 按数据类型生成平台排名
        for data_type in config_manager.config.get("platform_preferences", {}):
            platforms = config_manager.get_enabled_platforms(data_type)
            sorted_platforms = self._sort_platforms_by_priority(platforms)
            report["platform_rankings"][data_type] = sorted_platforms
        
        return report
    
    def optimize_platform_preferences(self):
        """根据性能统计优化平台偏好设置"""
        for data_type, current_platforms in config_manager.config.get("platform_preferences", {}).items():
            # 获取该数据类型的所有可用平台
            available_platforms = [p for p in current_platforms if config_manager.is_platform_enabled(p)]
            
            if len(available_platforms) <= 1:
                continue
            
            # 根据性能重新排序
            optimized_platforms = self._sort_platforms_by_priority(available_platforms)
            
            # 如果顺序有变化，更新配置
            if optimized_platforms != available_platforms:
                config_manager.set_platform_preferences(data_type, optimized_platforms)
                self.logger.info(f"已优化 {data_type} 的平台偏好: {' -> '.join(optimized_platforms)}")
    
    def reset_performance_stats(self):
        """重置性能统计"""
        self.platform_performance.clear()
        self.request_history.clear()
        self.logger.info("已重置平台性能统计")
    
    def create_smart_pipeline(self, pipeline_requests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        创建智能数据管道
        
        Args:
            pipeline_requests: 管道请求列表，每个请求包含 data_type 和 params
            
        Returns:
            Dict[str, Any]: 管道执行结果
        """
        results = {}
        total_start_time = time.time()
        
        for i, request in enumerate(pipeline_requests):
            data_type = request.get("data_type")
            params = request.get("params", {})
            task_name = request.get("name", f"task_{i}")
            
            if not data_type:
                results[task_name] = {
                    "success": False,
                    "error": "缺少 data_type 参数",
                    "data": None
                }
                continue
            
            # 智能路由请求
            result = self.route_data_request(
                data_type=data_type,
                params=params,
                preferred_platforms=request.get("platforms"),
                enable_fallback=request.get("enable_fallback", True),
                quality_check=request.get("quality_check", True)
            )
            
            results[task_name] = result
        
        # 添加管道级别的元信息
        total_time = time.time() - total_start_time
        successful_tasks = sum(1 for r in results.values() if r.get("success", False))
        
        results["_pipeline_metadata"] = {
            "total_tasks": len(pipeline_requests),
            "successful_tasks": successful_tasks,
            "success_rate": successful_tasks / len(pipeline_requests) if pipeline_requests else 0,
            "total_execution_time": total_time
        }
        
        return results


# 创建全局智能路由器实例
smart_router = SmartDataRouter()


# 便捷函数
def get_data_smart(data_type: str, **params) -> Dict[str, Any]:
    """智能获取数据的便捷函数"""
    return smart_router.route_data_request(data_type, params)


def get_multi_platform_data_smart(data_type: str, platforms: List[str] = None, **params) -> Dict[str, Dict[str, Any]]:
    """智能获取多平台数据的便捷函数"""
    return smart_router.get_multi_platform_data(data_type, params, platforms)


def get_best_platform(data_type: str) -> Optional[str]:
    """获取数据类型的最佳平台"""
    return smart_router.get_best_platform_for_data_type(data_type)


def get_performance_report() -> Dict[str, Any]:
    """获取平台性能报告"""
    return smart_router.get_platform_performance_report()


def optimize_preferences():
    """优化平台偏好设置"""
    smart_router.optimize_platform_preferences()
