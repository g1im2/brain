"""
宏观数据抓取配置

定义每个宏观数据类型的最大历史数据范围和抓取参数。
基于AKShare等数据源的实际支持能力。
"""

from datetime import datetime, date
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


class MacroDataConfig:
    """宏观数据配置管理器"""
    
    # 数据源历史数据支持范围配置
    DATA_SOURCE_RANGES = {
        # ==================== 日度数据 ====================
        'interest-rate-data': {
            'frequency': 'daily',
            'max_history_years': 20,  # AKShare支持约20年历史数据
            'start_date': '2004-01-01',  # 利率数据从2004年开始较为完整
            'data_source': 'akshare',
            'api_method': 'macro_china_bond_public',
            'description': '利率收益率数据，包括国债收益率、银行间利率等'
        },
        
        'stock-index-data': {
            'frequency': 'daily',
            'max_history_years': 30,  # 股票指数历史数据较为完整
            'start_date': '1990-12-19',  # 上证指数开始日期
            'data_source': 'akshare',
            'api_method': 'stock_zh_index_daily',
            'description': '股票指数数据，包括上证指数、深证成指、创业板指等'
        },
        
        'market-flow-data': {
            'frequency': 'daily',
            'max_history_years': 10,  # 资金流数据相对较新
            'start_date': '2014-01-01',
            'data_source': 'akshare',
            'api_method': 'stock_market_fund_flow',
            'description': '市场资金流数据，包括北向资金、南向资金等'
        },
        
        'commodity-price-data': {
            'frequency': 'daily',
            'max_history_years': 15,  # 商品价格数据
            'start_date': '2009-01-01',
            'data_source': 'akshare',
            'api_method': 'futures_main_sina',
            'description': '商品价格数据，包括原油、黄金、铜等大宗商品'
        },
        
        # ==================== 月度数据 ====================
        'price-index-data': {
            'frequency': 'monthly',
            'max_history_years': 25,  # CPI/PPI数据历史较长
            'start_date': '1999-01',
            'data_source': 'akshare',
            'api_method': 'macro_china_cpi',
            'description': '价格指数数据，包括CPI、PPI等'
        },
        
        'money-supply-data': {
            'frequency': 'monthly',
            'max_history_years': 25,  # 货币供应量数据
            'start_date': '1999-01',
            'data_source': 'akshare',
            'api_method': 'macro_china_money_supply',
            'description': '货币供应量数据，包括M0、M1、M2等'
        },
        
        'social-financing-data': {
            'frequency': 'monthly',
            'max_history_years': 15,  # 社会融资规模数据
            'start_date': '2009-01',
            'data_source': 'akshare',
            'api_method': 'macro_china_shrzgm',
            'description': '社会融资规模数据'
        },
        
        'investment-data': {
            'frequency': 'monthly',
            'max_history_years': 20,  # 固定资产投资数据
            'start_date': '2004-01',
            'data_source': 'akshare',
            'api_method': 'macro_china_gdzctz',
            'description': '投资统计数据，包括固定资产投资等'
        },
        
        'industrial-data': {
            'frequency': 'monthly',
            'max_history_years': 20,  # 工业增加值数据
            'start_date': '2004-01',
            'data_source': 'akshare',
            'api_method': 'macro_china_industrial_production',
            'description': '工业生产数据，包括工业增加值等'
        },
        
        'sentiment-index-data': {
            'frequency': 'monthly',
            'max_history_years': 20,  # PMI等景气指数
            'start_date': '2004-01',
            'data_source': 'akshare',
            'api_method': 'macro_china_pmi',
            'description': '景气指数数据，包括制造业PMI、非制造业PMI等'
        },
        
        'inventory-cycle-data': {
            'frequency': 'monthly',
            'max_history_years': 10,  # 库存数据相对较新
            'start_date': '2014-01',
            'data_source': 'manual',  # 需要手动采集
            'api_method': 'not_available',
            'description': '库存周期数据，需要从统计局工业企业数据中计算'
        },
        
        # ==================== 季度数据 ====================
        'gdp-data': {
            'frequency': 'quarterly',
            'max_history_years': 30,  # GDP数据历史最长
            'start_date': '1992Q1',
            'data_source': 'akshare',
            'api_method': 'macro_china_gdp',
            'description': 'GDP核算数据，包括总量和分产业数据'
        },
        
        # ==================== 年度数据 ====================
        'innovation-data': {
            'frequency': 'yearly',
            'max_history_years': 15,  # 创新数据相对较新
            'start_date': '2009',
            'data_source': 'manual',  # 需要手动采集
            'api_method': 'not_available',
            'description': '技术创新数据，包括研发投入、专利申请等'
        },
        
        'demographic-data': {
            'frequency': 'yearly',
            'max_history_years': 20,  # 人口数据
            'start_date': '2004',
            'data_source': 'manual',  # 需要手动采集
            'api_method': 'not_available',
            'description': '人口统计数据，包括城镇化率、人口结构等'
        }
    }
    
    @classmethod
    def get_data_config(cls, data_type: str) -> Dict[str, Any]:
        """获取指定数据类型的配置
        
        Args:
            data_type: 数据类型
            
        Returns:
            Dict[str, Any]: 数据配置
        """
        return cls.DATA_SOURCE_RANGES.get(data_type, {})
    
    @classmethod
    def get_max_history_range(cls, data_type: str) -> Dict[str, str]:
        """获取指定数据类型的最大历史范围
        
        Args:
            data_type: 数据类型
            
        Returns:
            Dict[str, str]: 包含start和end的日期范围
        """
        config = cls.get_data_config(data_type)
        if not config:
            logger.warning(f"No config found for data type: {data_type}")
            return {}
        
        frequency = config.get('frequency', 'daily')
        start_date = config.get('start_date')
        
        if not start_date:
            logger.warning(f"No start date configured for {data_type}")
            return {}
        
        # 计算结束日期（当前日期）
        today = date.today()
        
        if frequency == 'daily':
            end_date = today.strftime('%Y-%m-%d')
        elif frequency == 'monthly':
            end_date = today.strftime('%Y-%m')
        elif frequency == 'quarterly':
            # 计算当前季度
            quarter = (today.month - 1) // 3 + 1
            end_date = f"{today.year}Q{quarter}"
        elif frequency == 'yearly':
            end_date = str(today.year)
        else:
            end_date = today.strftime('%Y-%m-%d')
        
        return {
            'start': start_date,
            'end': end_date,
            'frequency': frequency
        }
    
    @classmethod
    def is_data_source_available(cls, data_type: str) -> bool:
        """检查数据源是否可用
        
        Args:
            data_type: 数据类型
            
        Returns:
            bool: 数据源是否可用
        """
        config = cls.get_data_config(data_type)
        data_source = config.get('data_source', '')
        
        # 检查是否需要手动采集
        if data_source == 'manual':
            logger.warning(f"{data_type} requires manual data collection")
            return False
        
        # 检查API方法是否可用
        api_method = config.get('api_method', '')
        if api_method == 'not_available':
            logger.warning(f"{data_type} API method not available")
            return False
        
        return True
    
    @classmethod
    def get_all_available_data_types(cls) -> List[str]:
        """获取所有可用的数据类型
        
        Returns:
            List[str]: 可用的数据类型列表
        """
        available_types = []
        for data_type in cls.DATA_SOURCE_RANGES.keys():
            if cls.is_data_source_available(data_type):
                available_types.append(data_type)
        return available_types
    
    @classmethod
    def get_data_types_by_frequency(cls, frequency: str) -> List[str]:
        """根据频率获取数据类型
        
        Args:
            frequency: 数据频率 (daily, monthly, quarterly, yearly)
            
        Returns:
            List[str]: 指定频率的数据类型列表
        """
        data_types = []
        for data_type, config in cls.DATA_SOURCE_RANGES.items():
            if config.get('frequency') == frequency and cls.is_data_source_available(data_type):
                data_types.append(data_type)
        return data_types
    
    @classmethod
    def get_task_schedule_info(cls) -> Dict[str, Any]:
        """获取任务调度信息
        
        Returns:
            Dict[str, Any]: 任务调度信息
        """
        schedule_info = {
            'daily_tasks': {
                'count': len(cls.get_data_types_by_frequency('daily')),
                'types': cls.get_data_types_by_frequency('daily'),
                'schedule': 'Every day 18:30-18:45'
            },
            'monthly_tasks': {
                'count': len(cls.get_data_types_by_frequency('monthly')),
                'types': cls.get_data_types_by_frequency('monthly'),
                'schedule': 'Daily incremental check 19:00-19:30'
            },
            'quarterly_tasks': {
                'count': len(cls.get_data_types_by_frequency('quarterly')),
                'types': cls.get_data_types_by_frequency('quarterly'),
                'schedule': 'Daily incremental check 19:35'
            },
            'yearly_tasks': {
                'count': len(cls.get_data_types_by_frequency('yearly')),
                'types': cls.get_data_types_by_frequency('yearly'),
                'schedule': 'Yearly on Jan 15th 20:00-20:05'
            }
        }
        
        return schedule_info
