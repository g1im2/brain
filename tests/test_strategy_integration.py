#!/usr/bin/env python3
"""
StrategyAdapter集成测试脚本

用于验证services/brain与services/execution的真实HTTP集成功能
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
from services.brain.coordinators.system_coordinator import SystemCoordinator
from services.brain.routers.signal_router import SignalRouter
from services.brain.coordinators.workflow_engine import WorkflowEngine

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StrategyIntegrationTester:
    """策略集成测试器"""
    
    def __init__(self):
        """初始化测试器"""
        self.config = IntegrationConfig()
        self.strategy_adapter = None
        self.system_coordinator = None
        self.signal_router = None
        self.workflow_engine = None
        
        # 测试结果统计
        self.test_results = {
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'test_details': []
        }
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """运行所有集成测试"""
        logger.info("🚀 开始执行StrategyAdapter集成测试...")
        
        try:
            # 1. 基础连接测试
            await self._test_basic_connection()
            
            # 2. 策略分析功能测试
            await self._test_strategy_analysis()
            
            # 3. 回测验证功能测试
            await self._test_backtest_validation()
            
            # 4. 实时验证功能测试
            await self._test_realtime_validation()
            
            # 5. 系统组件集成测试
            await self._test_system_coordinator_integration()
            
            # 6. 信号路由集成测试
            await self._test_signal_router_integration()
            
            # 7. 工作流引擎集成测试
            await self._test_workflow_engine_integration()
            
            # 8. 错误处理测试
            await self._test_error_handling()
            
            # 9. 性能基准测试
            await self._test_performance_benchmarks()
            
        except Exception as e:
            logger.error(f"测试执行过程中发生错误: {e}")
            self._record_test_result("全局测试执行", False, str(e))
        
        finally:
            await self._cleanup()
        
        return self._generate_test_report()
    
    async def _test_basic_connection(self) -> None:
        """测试基础HTTP连接"""
        logger.info("📡 测试基础HTTP连接...")
        
        try:
            self.strategy_adapter = StrategyAdapter(self.config)
            
            # 测试连接
            connected = await self.strategy_adapter.connect_to_system()
            self._record_test_result("基础HTTP连接", connected, "连接成功" if connected else "连接失败")
            
            if connected:
                # 测试健康检查
                health_ok = await self.strategy_adapter.health_check()
                self._record_test_result("健康检查", health_ok, "健康检查通过" if health_ok else "健康检查失败")
            
        except Exception as e:
            self._record_test_result("基础HTTP连接", False, f"连接异常: {e}")
    
    async def _test_strategy_analysis(self) -> None:
        """测试策略分析功能"""
        logger.info("📊 测试策略分析功能...")
        
        try:
            if not self.strategy_adapter:
                raise Exception("StrategyAdapter未初始化")
            
            # 准备测试数据
            portfolio_instruction = {
                'symbols': ['000001.SZ', '000002.SZ'],
                'weights': {'000001.SZ': 0.6, '000002.SZ': 0.4},
                'rebalance_frequency': 'monthly'
            }
            symbols = ['000001.SZ', '000002.SZ']
            
            # 执行策略分析
            start_time = datetime.now()
            results = await self.strategy_adapter.request_strategy_analysis(portfolio_instruction, symbols)
            end_time = datetime.now()
            
            # 验证结果
            success = isinstance(results, list) and len(results) > 0
            duration = (end_time - start_time).total_seconds()
            
            self._record_test_result(
                "策略分析功能", 
                success, 
                f"返回{len(results) if success else 0}个结果，耗时{duration:.3f}秒"
            )
            
            # 验证性能要求（<500ms）
            performance_ok = duration < 0.5
            self._record_test_result(
                "策略分析性能", 
                performance_ok, 
                f"耗时{duration:.3f}秒，{'符合' if performance_ok else '不符合'}性能要求(<500ms)"
            )
            
        except Exception as e:
            self._record_test_result("策略分析功能", False, f"测试异常: {e}")
    
    async def _test_backtest_validation(self) -> None:
        """测试回测验证功能"""
        logger.info("🔄 测试回测验证功能...")
        
        try:
            if not self.strategy_adapter:
                raise Exception("StrategyAdapter未初始化")
            
            # 准备测试数据
            symbols = ['000001.SZ', '000002.SZ']
            strategy_config = {
                'analyzers': ['livermore', 'multi_indicator'],
                'start_date': '2023-01-01',
                'end_date': '2023-12-31',
                'initial_capital': 1000000,
                'commission': 0.001
            }
            
            # 执行回测验证
            start_time = datetime.now()
            result = await self.strategy_adapter.request_backtest_validation(symbols, strategy_config)
            end_time = datetime.now()
            
            # 验证结果
            success = isinstance(result, dict) and 'status' in result
            duration = (end_time - start_time).total_seconds()
            
            self._record_test_result(
                "回测验证功能", 
                success, 
                f"状态: {result.get('status', 'unknown') if success else 'failed'}，耗时{duration:.3f}秒"
            )
            
            # 验证性能要求（<5s）
            performance_ok = duration < 5.0
            self._record_test_result(
                "回测验证性能", 
                performance_ok, 
                f"耗时{duration:.3f}秒，{'符合' if performance_ok else '不符合'}性能要求(<5s)"
            )
            
        except Exception as e:
            self._record_test_result("回测验证功能", False, f"测试异常: {e}")
    
    async def _test_realtime_validation(self) -> None:
        """测试实时验证功能"""
        logger.info("⚡ 测试实时验证功能...")
        
        try:
            if not self.strategy_adapter:
                raise Exception("StrategyAdapter未初始化")
            
            # 准备测试数据
            pool_config = {
                'pool_name': 'test_pool',
                'symbols': ['000001.SZ', '000002.SZ'],
                'pool_type': 'dynamic',
                'max_stocks': 10
            }
            
            # 执行实时验证
            start_time = datetime.now()
            result = await self.strategy_adapter.request_realtime_validation(pool_config)
            end_time = datetime.now()
            
            # 验证结果
            success = isinstance(result, dict) and 'status' in result
            duration = (end_time - start_time).total_seconds()
            
            self._record_test_result(
                "实时验证功能", 
                success, 
                f"状态: {result.get('status', 'unknown') if success else 'failed'}，耗时{duration:.3f}秒"
            )
            
        except Exception as e:
            self._record_test_result("实时验证功能", False, f"测试异常: {e}")
    
    async def _test_system_coordinator_integration(self) -> None:
        """测试SystemCoordinator集成"""
        logger.info("🎯 测试SystemCoordinator集成...")
        
        try:
            self.system_coordinator = SystemCoordinator(self.config)
            
            # 启动SystemCoordinator
            started = await self.system_coordinator.start()
            self._record_test_result("SystemCoordinator启动", started, "启动成功" if started else "启动失败")
            
            if started:
                # 测试分析周期执行
                portfolio_instruction = {
                    'symbols': ['000001.SZ'],
                    'weights': {'000001.SZ': 1.0}
                }
                
                cycle_result = await self.system_coordinator.execute_analysis_cycle(portfolio_instruction)
                success = cycle_result is not None
                self._record_test_result("分析周期执行", success, "执行成功" if success else "执行失败")
            
        except Exception as e:
            self._record_test_result("SystemCoordinator集成", False, f"测试异常: {e}")
    
    async def _test_signal_router_integration(self) -> None:
        """测试SignalRouter集成"""
        logger.info("📡 测试SignalRouter集成...")
        
        try:
            self.signal_router = SignalRouter(self.config)
            
            # 启动SignalRouter
            started = await self.signal_router.start()
            self._record_test_result("SignalRouter启动", started, "启动成功" if started else "启动失败")
            
        except Exception as e:
            self._record_test_result("SignalRouter集成", False, f"测试异常: {e}")
    
    async def _test_workflow_engine_integration(self) -> None:
        """测试WorkflowEngine集成"""
        logger.info("⚙️ 测试WorkflowEngine集成...")
        
        try:
            self.workflow_engine = WorkflowEngine(self.config)
            
            # 启动WorkflowEngine
            started = await self.workflow_engine.start()
            self._record_test_result("WorkflowEngine启动", started, "启动成功" if started else "启动失败")
            
        except Exception as e:
            self._record_test_result("WorkflowEngine集成", False, f"测试异常: {e}")
    
    async def _test_error_handling(self) -> None:
        """测试错误处理机制"""
        logger.info("🛡️ 测试错误处理机制...")
        
        try:
            if not self.strategy_adapter:
                raise Exception("StrategyAdapter未初始化")
            
            # 测试无效参数处理
            try:
                await self.strategy_adapter.request_strategy_analysis(None, [])
                self._record_test_result("错误处理-无效参数", False, "应该抛出异常但没有")
            except Exception:
                self._record_test_result("错误处理-无效参数", True, "正确处理无效参数")
            
        except Exception as e:
            self._record_test_result("错误处理机制", False, f"测试异常: {e}")
    
    async def _test_performance_benchmarks(self) -> None:
        """测试性能基准"""
        logger.info("🏃 测试性能基准...")
        
        try:
            if not self.strategy_adapter:
                raise Exception("StrategyAdapter未初始化")
            
            # 并发请求测试
            tasks = []
            for i in range(3):
                portfolio_instruction = {
                    'symbols': [f'00000{i+1}.SZ'],
                    'weights': {f'00000{i+1}.SZ': 1.0}
                }
                task = self.strategy_adapter.request_strategy_analysis(
                    portfolio_instruction, [f'00000{i+1}.SZ']
                )
                tasks.append(task)
            
            start_time = datetime.now()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            end_time = datetime.now()
            
            # 验证并发处理能力
            success_count = sum(1 for r in results if not isinstance(r, Exception))
            duration = (end_time - start_time).total_seconds()
            
            self._record_test_result(
                "并发处理能力", 
                success_count >= 2, 
                f"3个并发请求中{success_count}个成功，总耗时{duration:.3f}秒"
            )
            
        except Exception as e:
            self._record_test_result("性能基准测试", False, f"测试异常: {e}")
    
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
        
        report = {
            'summary': {
                'total_tests': self.test_results['total_tests'],
                'passed_tests': self.test_results['passed_tests'],
                'failed_tests': self.test_results['failed_tests'],
                'success_rate': f"{success_rate:.1f}%",
                'overall_status': "PASS" if success_rate >= 80 else "FAIL"
            },
            'test_details': self.test_results['test_details'],
            'timestamp': datetime.now().isoformat()
        }
        
        return report
    
    async def _cleanup(self) -> None:
        """清理资源"""
        logger.info("🧹 清理测试资源...")
        
        try:
            if self.strategy_adapter:
                await self.strategy_adapter.disconnect_from_system()
            
            if self.system_coordinator:
                await self.system_coordinator.stop()
            
            if self.signal_router:
                await self.signal_router.stop()
            
            if self.workflow_engine:
                await self.workflow_engine.stop()
                
        except Exception as e:
            logger.warning(f"清理资源时发生错误: {e}")


async def main():
    """主函数"""
    tester = StrategyIntegrationTester()
    
    try:
        # 运行所有测试
        report = await tester.run_all_tests()
        
        # 打印测试报告
        print("\n" + "="*80)
        print("🎯 StrategyAdapter集成测试报告")
        print("="*80)
        print(f"总测试数: {report['summary']['total_tests']}")
        print(f"通过测试: {report['summary']['passed_tests']}")
        print(f"失败测试: {report['summary']['failed_tests']}")
        print(f"成功率: {report['summary']['success_rate']}")
        print(f"总体状态: {report['summary']['overall_status']}")
        print("="*80)
        
        # 返回适当的退出码
        return 0 if report['summary']['overall_status'] == "PASS" else 1
        
    except Exception as e:
        logger.error(f"测试执行失败: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
