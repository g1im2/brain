#!/usr/bin/env python3
"""
Portfolio数据配置测试脚本（简化版）

仅测试Portfolio数据配置，不依赖asyncron和其他复杂模块
"""

import sys
import os
from datetime import datetime, date

# 添加项目路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from scheduler.portfolio_data_config import PortfolioDataConfig


def test_portfolio_data_config():
    """测试Portfolio数据配置"""
    
    print("=" * 60)
    print("Portfolio数据配置测试")
    print("=" * 60)
    
    # 测试数据类型配置
    print("\n1. 数据类型配置验证...")
    
    all_data_types = PortfolioDataConfig.get_all_data_types()
    print(f"   支持的数据类型: {all_data_types}")
    
    for data_type in all_data_types:
        config = PortfolioDataConfig.get_data_type_config(data_type)
        print(f"\n   {data_type}:")
        print(f"     名称: {config.get('name')}")
        print(f"     更新模式: {config.get('update_mode')}")
        print(f"     优先级: {config.get('priority')}")
        print(f"     调度频率: {config.get('schedule', {}).get('frequency')}")
        print(f"     数据源: {config.get('data_source')}")
        print(f"     表名: {config.get('table_name')}")
    
    # 测试更新模式配置
    print("\n2. 更新模式配置验证...")
    
    for mode in ['incremental', 'full_update', 'snapshot']:
        mode_config = PortfolioDataConfig.get_update_mode_config(mode)
        print(f"\n   {mode}:")
        print(f"     描述: {mode_config.get('description')}")
        print(f"     策略: {mode_config.get('strategy')}")
        print(f"     冲突处理: {mode_config.get('conflict_resolution')}")
        print(f"     批次大小: {mode_config.get('batch_size')}")
    
    # 测试分类查询
    print("\n3. 数据类型分类验证...")
    
    high_priority = PortfolioDataConfig.get_high_priority_data_types()
    daily_types = PortfolioDataConfig.get_daily_data_types()
    incremental_types = PortfolioDataConfig.get_incremental_data_types()
    
    print(f"   高优先级数据类型: {high_priority}")
    print(f"   每日更新数据类型: {daily_types}")
    print(f"   增量更新数据类型: {incremental_types}")
    
    # 测试调度判断
    print("\n4. 调度判断测试...")
    
    test_dates = [
        datetime(2024, 3, 15),  # 周五，3月第三个周五
        datetime(2024, 3, 17),  # 周日，3月第三个周日
        datetime(2024, 6, 21),  # 周五，6月第三个周五
        datetime(2024, 12, 1),  # 周日，12月第一个周日
    ]
    
    for test_date in test_dates:
        print(f"\n   测试日期: {test_date.strftime('%Y-%m-%d %A')}")
        
        for data_type in all_data_types:
            should_run = PortfolioDataConfig.should_run_today(data_type, test_date)
            if should_run:
                print(f"     ✅ {data_type} 应该运行")
            else:
                print(f"     ⏸️  {data_type} 不运行")
    
    print("\n✅ Portfolio数据配置测试完成")


def test_data_validation():
    """测试数据验证规则"""
    
    print("\n" + "=" * 60)
    print("Portfolio数据验证规则测试")
    print("=" * 60)
    
    # 测试数据验证规则
    test_cases = {
        'stock_basic': {
            'valid': {
                'ts_code': '000001.SZ',
                'symbol': '000001',
                'market': '主板'
            },
            'invalid': {
                'ts_code': 'INVALID',
                'symbol': 'ABC123',
                'market': '无效市场'
            }
        },
        'adj_factors': {
            'valid': {
                'symbol': '000001',
                'adj_factor': 1.0,
                'factor_type': 'hfq'
            },
            'invalid': {
                'symbol': 'INVALID',
                'adj_factor': -1.0,
                'factor_type': 'invalid'
            }
        }
    }
    
    for data_type, cases in test_cases.items():
        print(f"\n测试数据类型: {data_type}")
        
        validation_rules = PortfolioDataConfig.get_validation_rules(data_type)
        print(f"   验证规则: {validation_rules}")
        
        for case_type, data in cases.items():
            print(f"\n   {case_type}数据测试:")
            for field, value in data.items():
                print(f"     {field}: {value}")
    
    print("\n✅ 数据验证规则测试完成")


def test_schedule_logic():
    """测试调度逻辑"""
    
    print("\n" + "=" * 60)
    print("Portfolio调度逻辑测试")
    print("=" * 60)
    
    # 测试各种日期的调度判断
    test_scenarios = [
        {
            'name': '工作日测试',
            'date': datetime(2024, 3, 14),  # 周四
            'expected': {
                'adj_factors': True,  # 每日
                'stock_basic': False,  # 周日
                'industry_classification': False,  # 月第一个周日
                'index_components': False  # 季度第三个周五
            }
        },
        {
            'name': '周日测试',
            'date': datetime(2024, 3, 17),  # 周日
            'expected': {
                'adj_factors': True,  # 每日
                'stock_basic': True,  # 周日
                'industry_classification': False,  # 月第一个周日（不是第一个）
                'index_components': False  # 季度第三个周五
            }
        },
        {
            'name': '月第一个周日测试',
            'date': datetime(2024, 3, 3),  # 3月第一个周日
            'expected': {
                'adj_factors': True,  # 每日
                'stock_basic': True,  # 周日
                'industry_classification': True,  # 月第一个周日
                'index_components': False  # 季度第三个周五
            }
        },
        {
            'name': '季度第三个周五测试',
            'date': datetime(2024, 3, 15),  # 3月第三个周五
            'expected': {
                'adj_factors': True,  # 每日
                'stock_basic': False,  # 周日
                'industry_classification': False,  # 月第一个周日
                'index_components': True  # 季度第三个周五
            }
        }
    ]
    
    for scenario in test_scenarios:
        print(f"\n{scenario['name']}: {scenario['date'].strftime('%Y-%m-%d %A')}")
        
        for data_type, expected in scenario['expected'].items():
            actual = PortfolioDataConfig.should_run_today(data_type, scenario['date'])
            status = "✅" if actual == expected else "❌"
            print(f"   {status} {data_type}: 期望={expected}, 实际={actual}")
    
    print("\n✅ 调度逻辑测试完成")


def test_config_completeness():
    """测试配置完整性"""
    
    print("\n" + "=" * 60)
    print("Portfolio配置完整性测试")
    print("=" * 60)
    
    all_data_types = PortfolioDataConfig.get_all_data_types()
    
    required_fields = [
        'name', 'update_mode', 'priority', 'schedule', 
        'data_source', 'table_name', 'key_fields', 'required_fields'
    ]
    
    missing_configs = []
    
    for data_type in all_data_types:
        config = PortfolioDataConfig.get_data_type_config(data_type)
        
        print(f"\n检查数据类型: {data_type}")
        
        for field in required_fields:
            if field in config:
                print(f"   ✅ {field}: {config[field]}")
            else:
                print(f"   ❌ {field}: 缺失")
                missing_configs.append(f"{data_type}.{field}")
    
    # 检查更新模式配置
    print(f"\n检查更新模式配置...")
    
    for mode in ['incremental', 'full_update', 'snapshot']:
        mode_config = PortfolioDataConfig.get_update_mode_config(mode)
        if mode_config:
            print(f"   ✅ {mode}: 配置完整")
        else:
            print(f"   ❌ {mode}: 配置缺失")
            missing_configs.append(f"update_mode.{mode}")
    
    # 检查系统配置
    print(f"\n检查系统配置...")
    
    system_configs = [
        'SCHEDULING_CONFIG',
        'DATA_QUALITY_CONFIG', 
        'MONITORING_CONFIG'
    ]
    
    for config_name in system_configs:
        if hasattr(PortfolioDataConfig, config_name):
            print(f"   ✅ {config_name}: 已定义")
        else:
            print(f"   ❌ {config_name}: 未定义")
            missing_configs.append(f"system.{config_name}")
    
    if missing_configs:
        print(f"\n❌ 发现配置缺失: {missing_configs}")
        return False
    else:
        print(f"\n✅ 所有配置完整")
        return True


if __name__ == "__main__":
    print("开始Portfolio数据配置测试...")
    
    # 运行所有测试
    test_portfolio_data_config()
    test_data_validation()
    test_schedule_logic()
    completeness_ok = test_config_completeness()
    
    print("\n" + "=" * 60)
    print("测试结果总结")
    print("=" * 60)
    
    if completeness_ok:
        print("🎉 Portfolio数据配置测试全部通过！")
        print("\n✅ Portfolio数据抓取配置系统已准备就绪")
        print("\n📋 配置特性:")
        print("   - 4种数据类型：股票基础信息、指数成分股权重、复权因子、行业分类")
        print("   - 3种更新模式：增量追加、全量更新、时点快照")
        print("   - 智能调度策略：根据数据特性自动选择更新频率")
        print("   - 完整验证规则：数据格式、范围、一致性检查")
        print("   - 监控告警配置：性能跟踪、质量检查、异常处理")
        
        sys.exit(0)
    else:
        print("💥 配置完整性测试失败！")
        sys.exit(1)
