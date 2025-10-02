"""
Execution服务请求映射器

负责将Brain层的通用请求格式转换为Execution服务的RESTful API格式，
处理概念映射、数据格式转换和响应标准化。
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class ExecutionRequestMapper:
    """Execution服务请求映射器
    
    将Brain层的策略分析请求转换为Execution服务的API格式，
    处理分析器名称映射、参数转换和响应格式标准化。
    """
    
    def __init__(self):
        """初始化请求映射器"""
        # 分析器名称映射 (Brain层 -> Execution服务)
        self._analyzer_mapping = {
            'livermore_analyzer': 'livermore',
            'multi_indicator_analyzer': 'multi_indicator',
            'dow_theory_analyzer': 'dow_theory',
            'hong_hao_analyzer': 'hong_hao',
            'all': 'all'
        }
        
        # 反向映射 (Execution服务 -> Brain层)
        self._reverse_analyzer_mapping = {v: k for k, v in self._analyzer_mapping.items()}
        
        logger.info("ExecutionRequestMapper initialized")
    
    def map_strategy_analysis_request(self, brain_request: Dict[str, Any]) -> Dict[str, Any]:
        """映射策略分析请求
        
        Args:
            brain_request: Brain层的策略分析请求
            
        Returns:
            Dict[str, Any]: Execution服务的分析请求格式
        """
        try:
            # 提取基础参数
            symbols = brain_request.get('symbols', [])
            portfolio_instruction = brain_request.get('portfolio_instruction', {})
            
            # 从组合指令中提取分析器配置
            analyzers = self._extract_analyzers_from_instruction(portfolio_instruction)
            config = self._extract_config_from_instruction(portfolio_instruction)
            
            # 构造Execution服务请求
            execution_request = {
                'symbols': symbols,
                'analyzers': [self._analyzer_mapping.get(analyzer, analyzer) for analyzer in analyzers],
                'config': {
                    'lookback_days': config.get('lookback_days', 60),
                    'cache_enabled': config.get('cache_enabled', True),
                    'parallel': len(symbols) > 1,
                    'timeout_seconds': config.get('timeout_seconds', 300)
                }
            }

            # 透传可选的回调URL
            callback_url = brain_request.get('callback_url')
            if callback_url:
                execution_request['config']['callback_url'] = callback_url

            logger.debug(f"Mapped strategy analysis request: {len(symbols)} symbols, {len(analyzers)} analyzers")
            return execution_request
            
        except Exception as e:
            logger.error(f"Failed to map strategy analysis request: {e}")
            raise
    
    def map_backtest_request(self, brain_request: Dict[str, Any]) -> Dict[str, Any]:
        """映射回测请求
        
        Args:
            brain_request: Brain层的回测请求
            
        Returns:
            Dict[str, Any]: Execution服务的回测请求格式
        """
        try:
            symbols = brain_request.get('symbols', [])
            strategy_config = brain_request.get('strategy_config', {})

            # 构造回测请求
            backtest_request = {
                'strategy': {
                    'type': 'multi_analyzer',
                    'analyzers': [self._analyzer_mapping.get(a, a) for a in strategy_config.get('analyzers', ['livermore'])],
                    'weights': strategy_config.get('weights', {})
                },
                'universe': {
                    'symbols': symbols
                },
                'config': {
                    'start_date': strategy_config.get('start_date', '2023-01-01'),
                    'end_date': strategy_config.get('end_date', '2023-12-31'),
                    'initial_capital': strategy_config.get('initial_capital', 1000000),
                    'commission': strategy_config.get('commission', 0.001)
                }
            }

            # 透传可选的回调URL
            callback_url = brain_request.get('callback_url')
            if callback_url:
                backtest_request['config']['callback_url'] = callback_url

            logger.debug(f"Mapped backtest request: {len(symbols)} symbols")
            return backtest_request
            
        except Exception as e:
            logger.error(f"Failed to map backtest request: {e}")
            raise
    
    def map_quantum_pool_request(self, brain_request: Dict[str, Any]) -> Dict[str, Any]:
        """映射实时验证股票池请求
        
        Args:
            brain_request: Brain层的实时验证请求
            
        Returns:
            Dict[str, Any]: Execution服务的股票池创建请求格式
        """
        try:
            pool_config = brain_request.get('pool_config', {})
            
            quantum_request = {
                'name': brain_request.get('pool_name', f"brain_pool_{datetime.now().strftime('%Y%m%d_%H%M%S')}"),
                'strategy_type': pool_config.get('strategy_type', 'livermore'),
                'config': {
                    'max_capacity': pool_config.get('max_capacity', 50),
                    'max_holding_days': pool_config.get('max_holding_days', 30),
                    'risk_threshold': pool_config.get('risk_threshold', 0.05)
                }
            }
            
            logger.debug(f"Mapped quantum pool request: {quantum_request['name']}")
            return quantum_request
            
        except Exception as e:
            logger.error(f"Failed to map quantum pool request: {e}")
            raise
    
    def map_analysis_response(self, execution_response: Dict[str, Any]) -> Dict[str, Any]:
        """映射分析响应为Brain层格式
        
        Args:
            execution_response: Execution服务的分析响应
            
        Returns:
            Dict[str, Any]: Brain层的标准分析结果格式
        """
        try:
            if not execution_response.get('success', False):
                return {
                    'status': 'failed',
                    'error': execution_response.get('error', {}).get('message', 'Unknown error'),
                    'analysis_results': []
                }
            
            data = execution_response.get('data', {})
            
            # 转换分析结果
            analysis_results = []
            if 'results' in data:
                for symbol, result in data['results'].items():
                    analysis_result = {
                        'symbol': symbol,
                        'signal_type': self._determine_signal_type(result),
                        'strength': result.get('overall_score', 0.5),
                        'confidence': result.get('confidence', 0.5),
                        'target_weight': self._calculate_target_weight(result),
                        'analyzer_scores': self._map_analyzer_scores(result.get('analyzer_scores', {})),
                        'timestamp': datetime.now().isoformat(),
                        'source': 'strategy_analysis_system'
                    }
                    analysis_results.append(analysis_result)
            
            return {
                'status': 'success',
                'data': {
                    'analysis_results': analysis_results,
                    'analysis_summary': {
                        'total_symbols': len(analysis_results),
                        'buy_signals': len([r for r in analysis_results if r['signal_type'] == 'buy']),
                        'sell_signals': len([r for r in analysis_results if r['signal_type'] == 'sell']),
                        'hold_signals': len([r for r in analysis_results if r['signal_type'] == 'hold']),
                        'average_confidence': sum(r['confidence'] for r in analysis_results) / len(analysis_results) if analysis_results else 0
                    }
                },
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to map analysis response: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'analysis_results': []
            }
    
    def map_backtest_response(self, execution_response: Dict[str, Any]) -> Dict[str, Any]:
        """映射回测响应为Brain层格式
        
        Args:
            execution_response: Execution服务的回测响应
            
        Returns:
            Dict[str, Any]: Brain层的标准回测结果格式
        """
        try:
            if not execution_response.get('success', False):
                return {
                    'status': 'failed',
                    'error': execution_response.get('error', {}).get('message', 'Unknown error'),
                    'validation_results': {}
                }
            
            data = execution_response.get('data', {})
            
            return {
                'status': 'success',
                'data': {
                    'validation_type': 'backtest',
                    'validation_score': data.get('performance', {}).get('total_return', 0.0),
                    'confidence_adjustment': self._calculate_confidence_adjustment(data),
                    'risk_assessment': {
                        'max_drawdown': data.get('performance', {}).get('max_drawdown', 0.0),
                        'volatility': data.get('performance', {}).get('volatility', 0.0),
                        'sharpe_ratio': data.get('performance', {}).get('sharpe_ratio', 0.0)
                    },
                    'recommendations': self._generate_backtest_recommendations(data),
                    'backtest_period': {
                        'start_date': data.get('config', {}).get('start_date'),
                        'end_date': data.get('config', {}).get('end_date')
                    }
                },
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to map backtest response: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'validation_results': {}
            }
    
    def _extract_analyzers_from_instruction(self, portfolio_instruction: Dict[str, Any]) -> List[str]:
        """从组合指令中提取分析器列表"""
        # 根据组合指令的风险水平和市场状态选择分析器
        risk_level = portfolio_instruction.get('risk_level', 'medium')
        market_sentiment = portfolio_instruction.get('market_sentiment', 'neutral')
        
        if risk_level == 'high' and market_sentiment == 'positive':
            return ['livermore_analyzer', 'multi_indicator_analyzer']
        elif risk_level == 'low':
            return ['dow_theory_analyzer', 'hong_hao_analyzer']
        else:
            return ['all']
    
    def _extract_config_from_instruction(self, portfolio_instruction: Dict[str, Any]) -> Dict[str, Any]:
        """从组合指令中提取配置参数"""
        return {
            'lookback_days': portfolio_instruction.get('lookback_days', 60),
            'cache_enabled': True,
            'timeout_seconds': 300
        }
    
    def _determine_signal_type(self, result: Dict[str, Any]) -> str:
        """根据分析结果确定信号类型"""
        score = result.get('overall_score', 0.5)
        if score > 0.6:
            return 'buy'
        elif score < 0.4:
            return 'sell'
        else:
            return 'hold'
    
    def _calculate_target_weight(self, result: Dict[str, Any]) -> float:
        """计算目标权重"""
        score = result.get('overall_score', 0.5)
        confidence = result.get('confidence', 0.5)
        return min(score * confidence * 0.1, 0.05)  # 最大5%权重
    
    def _map_analyzer_scores(self, analyzer_scores: Dict[str, Any]) -> Dict[str, Any]:
        """映射分析器评分"""
        mapped_scores = {}
        for analyzer, score in analyzer_scores.items():
            brain_analyzer = self._reverse_analyzer_mapping.get(analyzer, analyzer)
            mapped_scores[brain_analyzer] = score
        return mapped_scores

    def _map_analyzer_name(self, brain_analyzer_name: str) -> str:
        """映射分析器名称从Brain层到Execution服务"""
        return self._analyzer_mapping.get(brain_analyzer_name, brain_analyzer_name)
    
    def _calculate_confidence_adjustment(self, backtest_data: Dict[str, Any]) -> float:
        """计算置信度调整"""
        performance = backtest_data.get('performance', {})
        sharpe_ratio = performance.get('sharpe_ratio', 0.0)
        max_drawdown = performance.get('max_drawdown', 0.0)
        
        # 基于夏普比率和最大回撤计算置信度调整
        if sharpe_ratio > 1.0 and max_drawdown < 0.1:
            return 0.1  # 提升置信度
        elif sharpe_ratio < 0.5 or max_drawdown > 0.2:
            return -0.1  # 降低置信度
        else:
            return 0.0  # 无调整
    
    def _generate_backtest_recommendations(self, backtest_data: Dict[str, Any]) -> List[str]:
        """生成回测建议"""
        recommendations = []
        performance = backtest_data.get('performance', {})
        
        if performance.get('sharpe_ratio', 0) > 1.0:
            recommendations.append("策略表现优秀，建议执行")
        
        if performance.get('max_drawdown', 0) > 0.15:
            recommendations.append("注意控制最大回撤风险")
        
        if performance.get('volatility', 0) > 0.3:
            recommendations.append("策略波动较大，建议降低仓位")
        
        return recommendations if recommendations else ["策略表现正常，可以考虑执行"]
