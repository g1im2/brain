#!/usr/bin/env python3
"""
集成就绪度验证脚本

验证services/brain与services/execution的集成就绪度，
重点测试代码结构、错误处理和基础功能。
"""

import asyncio
import logging
import sys
import os
from datetime import datetime
from typing import Dict, List, Any

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from services.brain.config import IntegrationConfig
from services.brain.adapters.strategy_adapter import StrategyAdapter
from services.brain.adapters.execution_request_mapper import ExecutionRequestMapper
from services.brain.coordinators.system_coordinator import SystemCoordinator

try:
    from services.brain.adapters.tactical_adapter import TacticalAdapter
except ImportError:
    TacticalAdapter = None

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IntegrationReadinessTester:
    """集成就绪度测试器"""
    
    def __init__(self):
        """初始化测试器"""
        self.config = IntegrationConfig()
        self.test_results = {
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'test_details': []
        }
    
    async def run_readiness_tests(self) -> Dict[str, Any]:
        """运行集成就绪度测试"""
        logger.info("🚀 开始执行集成就绪度验证...")
        
        try:
            # 1. 代码结构验证
            await self._test_code_structure()
            
            # 2. 配置验证
            await self._test_configuration()
            
            # 3. 适配器初始化验证
            await self._test_adapter_initialization()
            
            # 4. 请求映射验证
            await self._test_request_mapping()
            
            # 5. 错误处理验证
            await self._test_error_handling()
            
            # 6. 概念映射验证
            await self._test_concept_mapping()
            
        except Exception as e:
            logger.error(f"测试执行过程中发生错误: {e}")
            self._record_test_result("全局测试执行", False, str(e))
        
        return self._generate_test_report()
    
    async def _test_code_structure(self) -> None:
        """测试代码结构"""
        logger.info("📁 测试代码结构...")
        
        try:
            # 测试StrategyAdapter导入
            self._record_test_result("StrategyAdapter导入", StrategyAdapter is not None, "成功导入StrategyAdapter")
            
            # 测试ExecutionRequestMapper导入
            self._record_test_result("ExecutionRequestMapper导入", ExecutionRequestMapper is not None, "成功导入ExecutionRequestMapper")
            
            # 测试旧TacticalAdapter已移除
            if TacticalAdapter is None:
                self._record_test_result("TacticalAdapter移除", True, "TacticalAdapter已成功移除")
            else:
                self._record_test_result("TacticalAdapter移除", False, "TacticalAdapter仍然存在")
            
            # 测试SystemCoordinator更新
            coordinator = SystemCoordinator(self.config)
            has_strategy_adapter = hasattr(coordinator, '_strategy_adapter')
            self._record_test_result("SystemCoordinator更新", has_strategy_adapter, 
                                   "包含_strategy_adapter属性" if has_strategy_adapter else "缺少_strategy_adapter属性")
            
        except Exception as e:
            self._record_test_result("代码结构验证", False, f"结构验证异常: {e}")
    
    async def _test_configuration(self) -> None:
        """测试配置"""
        logger.info("⚙️ 测试配置...")
        
        try:
            # 测试配置初始化
            config = IntegrationConfig()
            self._record_test_result("配置初始化", True, "配置成功初始化")
            
            # 测试execution服务配置
            has_execution_url = hasattr(config.service, 'execution_service_url')
            self._record_test_result("Execution服务配置", has_execution_url, 
                                   "包含execution_service_url配置" if has_execution_url else "缺少execution_service_url配置")
            
        except Exception as e:
            self._record_test_result("配置验证", False, f"配置验证异常: {e}")
    
    async def _test_adapter_initialization(self) -> None:
        """测试适配器初始化"""
        logger.info("🔧 测试适配器初始化...")
        
        try:
            # 测试StrategyAdapter初始化
            strategy_adapter = StrategyAdapter(self.config)
            self._record_test_result("StrategyAdapter初始化", True, "StrategyAdapter成功初始化")
            
            # 测试适配器属性
            has_http_client = hasattr(strategy_adapter, '_http_client')
            self._record_test_result("HTTP客户端属性", has_http_client, 
                                   "包含_http_client属性" if has_http_client else "缺少_http_client属性")
            
            has_request_mapper = hasattr(strategy_adapter, '_request_mapper')
            self._record_test_result("请求映射器属性", has_request_mapper, 
                                   "包含_request_mapper属性" if has_request_mapper else "缺少_request_mapper属性")
            
            # 测试方法存在性
            methods_to_check = [
                'connect_to_system',
                'disconnect_from_system',
                'health_check',
                'request_strategy_analysis',
                'request_backtest_validation',
                'request_realtime_validation'
            ]
            
            for method_name in methods_to_check:
                has_method = hasattr(strategy_adapter, method_name)
                self._record_test_result(f"方法{method_name}", has_method, 
                                       f"包含{method_name}方法" if has_method else f"缺少{method_name}方法")
            
        except Exception as e:
            self._record_test_result("适配器初始化验证", False, f"初始化验证异常: {e}")
    
    async def _test_request_mapping(self) -> None:
        """测试请求映射"""
        logger.info("🔄 测试请求映射...")
        
        try:
            # 测试ExecutionRequestMapper初始化
            mapper = ExecutionRequestMapper()
            self._record_test_result("RequestMapper初始化", True, "ExecutionRequestMapper成功初始化")
            
            # 测试分析器名称映射
            test_analyzer_mapping = {
                'livermore_analyzer': 'livermore',
                'multi_indicator_analyzer': 'multi_indicator',
                'dow_theory_analyzer': 'dow_theory',
                'hong_hao_analyzer': 'hong_hao'
            }
            
            mapping_success = True
            for brain_name, execution_name in test_analyzer_mapping.items():
                mapped_name = mapper._map_analyzer_name(brain_name)
                if mapped_name != execution_name:
                    mapping_success = False
                    break
            
            self._record_test_result("分析器名称映射", mapping_success, 
                                   "分析器名称映射正确" if mapping_success else "分析器名称映射错误")
            
            # 测试请求格式转换
            test_request = {
                'symbols': ['000001.SZ', '000002.SZ'],
                'analyzers': ['livermore_analyzer', 'multi_indicator_analyzer']
            }
            
            try:
                mapped_request = mapper.map_strategy_analysis_request(test_request)
                has_required_fields = all(field in mapped_request for field in ['symbols', 'analyzers'])
                self._record_test_result("请求格式转换", has_required_fields, 
                                       "请求格式转换成功" if has_required_fields else "请求格式转换失败")
            except Exception as e:
                self._record_test_result("请求格式转换", False, f"转换异常: {e}")
            
        except Exception as e:
            self._record_test_result("请求映射验证", False, f"映射验证异常: {e}")
    
    async def _test_error_handling(self) -> None:
        """测试错误处理"""
        logger.info("🛡️ 测试错误处理...")
        
        try:
            strategy_adapter = StrategyAdapter(self.config)
            
            # 测试未连接状态的错误处理
            try:
                await strategy_adapter.request_strategy_analysis({}, [])
                self._record_test_result("未连接错误处理", False, "应该抛出异常但没有")
            except Exception as e:
                error_message = str(e)
                is_correct_error = "not connected" in error_message.lower()
                self._record_test_result("未连接错误处理", is_correct_error, 
                                       f"正确处理未连接错误: {error_message}" if is_correct_error else f"错误处理不正确: {error_message}")
            
            # 测试无效参数错误处理
            try:
                await strategy_adapter.request_backtest_validation(None, None)
                self._record_test_result("无效参数错误处理", False, "应该抛出异常但没有")
            except Exception as e:
                self._record_test_result("无效参数错误处理", True, f"正确处理无效参数: {str(e)}")
            
        except Exception as e:
            self._record_test_result("错误处理验证", False, f"错误处理验证异常: {e}")
    
    async def _test_concept_mapping(self) -> None:
        """测试概念映射"""
        logger.info("🎯 测试概念映射...")
        
        try:
            # 测试概念重命名
            concept_mappings = {
                'tactical_analysis': 'strategy_analysis',
                'trading_signals': 'analysis_results',
                'signal_validation': 'strategy_validation'
            }
            
            # 检查SystemCoordinator中的方法名更新
            coordinator = SystemCoordinator(self.config)
            
            # 检查是否有新的方法名
            has_strategy_analysis = hasattr(coordinator, '_execute_strategy_analysis')
            self._record_test_result("策略分析方法", has_strategy_analysis, 
                                   "包含_execute_strategy_analysis方法" if has_strategy_analysis else "缺少_execute_strategy_analysis方法")
            
            has_strategy_validation = hasattr(coordinator, '_execute_strategy_validation')
            self._record_test_result("策略验证方法", has_strategy_validation, 
                                   "包含_execute_strategy_validation方法" if has_strategy_validation else "缺少_execute_strategy_validation方法")
            
            # 检查是否移除了旧的方法名
            has_old_tactical = hasattr(coordinator, '_execute_tactical_analysis')
            self._record_test_result("旧方法移除", not has_old_tactical, 
                                   "旧的tactical方法已移除" if not has_old_tactical else "旧的tactical方法仍存在")
            
        except Exception as e:
            self._record_test_result("概念映射验证", False, f"概念映射验证异常: {e}")
    
    def _record_test_result(self, test_name: str, success: bool, details: str) -> None:
        """记录测试结果"""
        self.test_results['total_tests'] += 1
        if success:
            self.test_results['passed_tests'] += 1
        else:
            self.test_results['failed_tests'] += 1
        
        self.test_results['test_details'].append({
            'test_name': test_name,
            'success': success,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
        
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"{status} {test_name}: {details}")
    
    def _generate_test_report(self) -> Dict[str, Any]:
        """生成测试报告"""
        success_rate = (self.test_results['passed_tests'] / self.test_results['total_tests'] * 100) if self.test_results['total_tests'] > 0 else 0
        
        # 计算集成就绪度评分
        readiness_score = self._calculate_readiness_score()
        
        report = {
            'summary': {
                'total_tests': self.test_results['total_tests'],
                'passed_tests': self.test_results['passed_tests'],
                'failed_tests': self.test_results['failed_tests'],
                'success_rate': f"{success_rate:.1f}%",
                'readiness_score': f"{readiness_score:.1f}%",
                'overall_status': "READY" if readiness_score >= 80 else "NOT_READY"
            },
            'test_details': self.test_results['test_details'],
            'timestamp': datetime.now().isoformat()
        }
        
        return report
    
    def _calculate_readiness_score(self) -> float:
        """计算集成就绪度评分"""
        if self.test_results['total_tests'] == 0:
            return 0.0
        
        # 基础成功率
        base_score = (self.test_results['passed_tests'] / self.test_results['total_tests']) * 100
        
        # 关键测试权重调整
        critical_tests = [
            'StrategyAdapter导入',
            'TacticalAdapter移除', 
            'SystemCoordinator更新',
            'StrategyAdapter初始化',
            '分析器名称映射',
            '策略分析方法'
        ]
        
        critical_passed = 0
        critical_total = 0
        
        for test_detail in self.test_results['test_details']:
            if test_detail['test_name'] in critical_tests:
                critical_total += 1
                if test_detail['success']:
                    critical_passed += 1
        
        if critical_total > 0:
            critical_score = (critical_passed / critical_total) * 100
            # 关键测试占70%权重，其他测试占30%权重
            final_score = (critical_score * 0.7) + (base_score * 0.3)
        else:
            final_score = base_score
        
        return min(final_score, 100.0)


async def main():
    """主函数"""
    tester = IntegrationReadinessTester()
    
    try:
        # 运行就绪度测试
        report = await tester.run_readiness_tests()
        
        # 打印测试报告
        print("\n" + "="*80)
        print("🎯 services/brain与services/execution集成就绪度报告")
        print("="*80)
        print(f"总测试数: {report['summary']['total_tests']}")
        print(f"通过测试: {report['summary']['passed_tests']}")
        print(f"失败测试: {report['summary']['failed_tests']}")
        print(f"成功率: {report['summary']['success_rate']}")
        print(f"集成就绪度: {report['summary']['readiness_score']}")
        print(f"总体状态: {report['summary']['overall_status']}")
        print("="*80)
        
        # 详细结果
        print("\n📋 详细测试结果:")
        for detail in report['test_details']:
            status = "✅" if detail['success'] else "❌"
            print(f"{status} {detail['test_name']}: {detail['details']}")
        
        # 返回适当的退出码
        return 0 if report['summary']['overall_status'] == "READY" else 1
        
    except Exception as e:
        logger.error(f"测试执行失败: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
