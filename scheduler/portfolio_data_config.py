"""
Portfolio项目数据抓取配置

定义不同数据类型的抓取策略、频率、优先级等配置信息
"""

from typing import Dict, List, Any
from datetime import datetime, timedelta


class PortfolioDataConfig:
    """Portfolio数据抓取配置管理"""

    # ==================== 数据更新模式配置 ====================
    
    DATA_UPDATE_MODES = {
        'incremental': {
            'description': '增量追加型',
            'strategy': 'append_only',
            'conflict_resolution': 'skip_existing',
            'batch_size': 1000,
            'retry_count': 3
        },
        'full_update': {
            'description': '全量更新型',
            'strategy': 'upsert',
            'conflict_resolution': 'update_existing',
            'batch_size': 500,
            'retry_count': 2
        },
        'snapshot': {
            'description': '时点快照型',
            'strategy': 'versioned_insert',
            'conflict_resolution': 'create_new_version',
            'batch_size': 200,
            'retry_count': 2
        }
    }

    # ==================== 数据类型配置 ====================
    
    DATA_TYPES_CONFIG = {
        'stock_basic': {
            'name': '股票基础信息',
            'update_mode': 'full_update',
            'priority': 'high',
            'schedule': {
                'frequency': 'weekly',
                'day_of_week': 'sunday',
                'time': '19:00:00'
            },
            'data_source': 'tushare',
            'fallback_source': 'akshare',
            'table_name': 'stock_basic',
            'key_fields': ['ts_code'],
            'required_fields': ['ts_code', 'symbol', 'name', 'market'],
            'validation_rules': {
                'ts_code': r'^[0-9]{6}\.(SH|SZ)$',
                'symbol': r'^[0-9]{6}$',
                'market': ['主板', '中小板', '创业板', '科创板']
            }
        },
        
        'index_components': {
            'name': '指数成分股权重',
            'update_mode': 'snapshot',
            'priority': 'high',
            'schedule': {
                'frequency': 'quarterly',
                'months': [3, 6, 9, 12],
                'day_of_month': 'third_friday',
                'time': '20:00:00'
            },
            'data_source': 'tushare',
            'fallback_source': None,
            'table_name': 'index_components',
            'key_fields': ['index_code', 'symbol', 'trade_date', 'adjustment_date'],
            'required_fields': ['index_code', 'symbol', 'weight'],
            'target_indices': ['000300', '000905', '000852', '399006'],
            'validation_rules': {
                'index_code': r'^[0-9]{6}$',
                'symbol': r'^[0-9]{6}$',
                'weight': {'min': 0, 'max': 100}
            }
        },
        
        'adj_factors': {
            'name': '复权因子',
            'update_mode': 'incremental',
            'priority': 'medium',
            'schedule': {
                'frequency': 'daily',
                'time': '18:50:00',
                'trading_days_only': True
            },
            'data_source': 'tushare',
            'fallback_source': 'akshare',
            'table_name': 'adj_factors',
            'key_fields': ['symbol', 'trade_date', 'factor_type'],
            'required_fields': ['symbol', 'trade_date', 'adj_factor'],
            'validation_rules': {
                'symbol': r'^[0-9]{6}$',
                'adj_factor': {'min': 0.001, 'max': 1000},
                'factor_type': ['hfq', 'qfq']
            }
        },
        
        'industry_classification': {
            'name': '行业分类',
            'update_mode': 'full_update',
            'priority': 'low',
            'schedule': {
                'frequency': 'monthly',
                'day_of_month': 'first_sunday',
                'time': '19:30:00'
            },
            'data_source': 'tushare',
            'fallback_source': None,
            'table_name': 'industry_classification',
            'key_fields': ['symbol', 'industry_name', 'effective_date'],
            'required_fields': ['symbol', 'industry_name'],
            'validation_rules': {
                'symbol': r'^[0-9]{6}$',
                'industry_name': {'min_length': 2, 'max_length': 50}
            }
        }
    }

    # ==================== 调度策略配置 ====================
    
    SCHEDULING_CONFIG = {
        'timezone': 'Asia/Shanghai',
        'trading_calendar': 'SSE',  # 上海证券交易所交易日历
        'retry_policy': {
            'max_retries': 3,
            'retry_delay': 300,  # 5分钟
            'exponential_backoff': True
        },
        'timeout_config': {
            'connection_timeout': 30,
            'read_timeout': 300,
            'total_timeout': 600
        },
        'rate_limiting': {
            'requests_per_minute': 200,
            'burst_limit': 50
        }
    }

    # ==================== 数据质量配置 ====================
    
    DATA_QUALITY_CONFIG = {
        'validation_enabled': True,
        'auto_repair': True,
        'quality_checks': {
            'completeness': {
                'enabled': True,
                'threshold': 0.95  # 95%完整度
            },
            'consistency': {
                'enabled': True,
                'cross_table_checks': True
            },
            'timeliness': {
                'enabled': True,
                'max_delay_hours': 24
            },
            'accuracy': {
                'enabled': True,
                'outlier_detection': True
            }
        },
        'alert_thresholds': {
            'error_rate': 0.05,  # 5%错误率
            'missing_data': 0.1,  # 10%缺失率
            'delay_hours': 6      # 6小时延迟
        }
    }

    # ==================== 监控配置 ====================
    
    MONITORING_CONFIG = {
        'metrics_enabled': True,
        'logging_level': 'INFO',
        'performance_tracking': True,
        'alerts': {
            'email_enabled': False,
            'webhook_enabled': True,
            'slack_enabled': False
        },
        'dashboard': {
            'enabled': True,
            'refresh_interval': 300  # 5分钟
        }
    }

    @classmethod
    def get_data_type_config(cls, data_type: str) -> Dict[str, Any]:
        """获取指定数据类型的配置"""
        return cls.DATA_TYPES_CONFIG.get(data_type, {})

    @classmethod
    def get_update_mode_config(cls, update_mode: str) -> Dict[str, Any]:
        """获取指定更新模式的配置"""
        return cls.DATA_UPDATE_MODES.get(update_mode, {})

    @classmethod
    def get_all_data_types(cls) -> List[str]:
        """获取所有数据类型列表"""
        return list(cls.DATA_TYPES_CONFIG.keys())

    @classmethod
    def get_high_priority_data_types(cls) -> List[str]:
        """获取高优先级数据类型"""
        return [
            data_type for data_type, config in cls.DATA_TYPES_CONFIG.items()
            if config.get('priority') == 'high'
        ]

    @classmethod
    def get_daily_data_types(cls) -> List[str]:
        """获取需要每日更新的数据类型"""
        return [
            data_type for data_type, config in cls.DATA_TYPES_CONFIG.items()
            if config.get('schedule', {}).get('frequency') == 'daily'
        ]

    @classmethod
    def get_incremental_data_types(cls) -> List[str]:
        """获取增量更新的数据类型"""
        return [
            data_type for data_type, config in cls.DATA_TYPES_CONFIG.items()
            if config.get('update_mode') == 'incremental'
        ]

    @classmethod
    def validate_data_type(cls, data_type: str) -> bool:
        """验证数据类型是否有效"""
        return data_type in cls.DATA_TYPES_CONFIG

    @classmethod
    def get_validation_rules(cls, data_type: str) -> Dict[str, Any]:
        """获取数据验证规则"""
        config = cls.get_data_type_config(data_type)
        return config.get('validation_rules', {})

    @classmethod
    def get_schedule_info(cls, data_type: str) -> Dict[str, Any]:
        """获取调度信息"""
        config = cls.get_data_type_config(data_type)
        return config.get('schedule', {})

    @classmethod
    def should_run_today(cls, data_type: str, current_date: datetime = None) -> bool:
        """判断指定数据类型今天是否应该运行"""
        if current_date is None:
            current_date = datetime.now()
            
        schedule = cls.get_schedule_info(data_type)
        frequency = schedule.get('frequency')
        
        if frequency == 'daily':
            return True
        elif frequency == 'weekly':
            day_of_week = schedule.get('day_of_week', 'sunday')
            target_weekday = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
                            'friday': 4, 'saturday': 5, 'sunday': 6}.get(day_of_week, 6)
            return current_date.weekday() == target_weekday
        elif frequency == 'monthly':
            day_of_month = schedule.get('day_of_month', 'first_sunday')
            if day_of_month == 'first_sunday':
                return (current_date.weekday() == 6 and current_date.day <= 7)
        elif frequency == 'quarterly':
            months = schedule.get('months', [3, 6, 9, 12])
            day_of_month = schedule.get('day_of_month', 'third_friday')
            if (current_date.month in months and 
                day_of_month == 'third_friday' and
                current_date.weekday() == 4 and
                15 <= current_date.day <= 21):
                return True
                
        return False

    @classmethod
    def get_next_run_time(cls, data_type: str, current_time: datetime = None) -> datetime:
        """获取下次运行时间"""
        if current_time is None:
            current_time = datetime.now()
            
        schedule = cls.get_schedule_info(data_type)
        # 这里可以实现更复杂的下次运行时间计算逻辑
        # 简化实现：返回明天同一时间
        return current_time + timedelta(days=1)
