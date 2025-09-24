"""
适配器模块

包含各层系统的适配器组件，提供标准化的系统接口。
"""

from adapters.macro_adapter import MacroAdapter
from adapters.portfolio_adapter import PortfolioAdapter
from adapters.strategy_adapter import StrategyAdapter
from adapters.flowhub_adapter import FlowhubAdapter

__all__ = [
    'MacroAdapter',
    'PortfolioAdapter',
    'StrategyAdapter',
    'FlowhubAdapter'
]
