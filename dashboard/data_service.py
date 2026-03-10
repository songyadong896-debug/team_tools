# data_service.py
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from db_manager import db_manager
from sqlalchemy import select, and_, or_
from database import AsyncSessionLocal
from charging_models import *
from decimal import Decimal

class DataService:
    def __init__(self):
        """初始化数据服务"""
        self.cache = {}  # 内存缓存
        self.cache_timestamps = {}  # 缓存时间戳
        self.cache_ttl = 3600  # 缓存1小时
    
    def clear_cache(self):
        """清空缓存"""
        self.cache.clear()
        self.cache_timestamps.clear()
    
    def get_cache_key(self, version_id: int, region: str, data_type: str) -> str:
        """生成缓存键"""
        return f"{version_id}_{region}_{data_type}"
    
    def is_cache_valid(self, key: str) -> bool:
        """检查缓存是否有效"""
        if key not in self.cache_timestamps:
            return False
        
        elapsed = (datetime.now() - self.cache_timestamps[key]).total_seconds()
        return elapsed < self.cache_ttl
    
    async def get_region_data(self, region: str, data_type: str = 'monthly', 
                       version_id: Optional[int] = None) -> pd.DataFrame:
        """获取区域数据（优先从缓存）"""
        if version_id is None:
            version_id = await db_manager.get_latest_version_id()
        
        if version_id is None:
            return pd.DataFrame()
        
        cache_key = self.get_cache_key(version_id, region, data_type)
        
        # 检查缓存
        if cache_key in self.cache and self.is_cache_valid(cache_key):
            return self.cache[cache_key]
        
        # 从数据库加载
        data = await self.load_from_database(version_id, region, data_type)
        
        # 更新缓存
        self.cache[cache_key] = data
        self.cache_timestamps[cache_key] = datetime.now()
        
        return data
    
    async def load_from_database(self, version_id: int, region: str, data_type: str) -> pd.DataFrame:
        """从数据库加载数据"""
        
        if data_type in ['monthly', 'weekly', 'quarterly']:
            # 加载预聚合数据
            sql = """
                SELECT * FROM charging_aggregated_station_data
                WHERE version_id = :version_id AND region = :region AND aggregation_type = :aggregation_type
                ORDER BY stat_date
            """
            df = await db_manager.execute_query_df(sql, {
                'version_id': version_id, 
                'region': region, 
                'aggregation_type': data_type
            })
            # 确保日期列是 datetime 类型
            if not df.empty and 'stat_date' in df.columns:
                df['stat_date'] = pd.to_datetime(df['stat_date'])
            return df
        
        elif data_type == 'penetration_monthly':
            sql = """
                SELECT * FROM charging_aggregated_penetration_data
                WHERE version_id = :version_id AND region = :region AND aggregation_type = 'monthly'
                ORDER BY stat_date
            """
            df = await db_manager.execute_query_df(sql, {'version_id': version_id, 'region': region})
            # 确保日期列是 datetime 类型
            if not df.empty and 'stat_date' in df.columns:
                df['stat_date'] = pd.to_datetime(df['stat_date'])
            return df
        
        elif data_type == 'penetration_weekly':
            sql = """
                SELECT * FROM charging_aggregated_penetration_data
                WHERE version_id = :version_id AND region = :region AND aggregation_type = 'weekly'
                ORDER BY stat_date
            """
            df = await db_manager.execute_query_df(sql, {'version_id': version_id, 'region': region})
            # 确保日期列是 datetime 类型
            if not df.empty and 'stat_date' in df.columns:
                df['stat_date'] = pd.to_datetime(df['stat_date'])
            return df
        
        elif data_type == 'daily':
            # 加载原始日数据
            sql = """
                SELECT * FROM charging_station_data_daily
                WHERE version_id = :version_id AND region = :region
                ORDER BY stat_date
            """
            df = await db_manager.execute_query_df(sql, {'version_id': version_id, 'region': region})
            # 确保日期列是 datetime 类型
            if not df.empty and 'stat_date' in df.columns:
                df['stat_date'] = pd.to_datetime(df['stat_date'])
            return df
        
        elif data_type == 'penetration_daily':
            sql = """
                SELECT * FROM charging_penetration_data_daily
                WHERE version_id = :version_id AND region = :region
                ORDER BY stat_date
            """
            df = await db_manager.execute_query_df(sql, {'version_id': version_id, 'region': region})
            # 确保日期列是 datetime 类型
            if not df.empty and 'stat_date' in df.columns:
                df['stat_date'] = pd.to_datetime(df['stat_date'])
            return df
        
        elif data_type == 'target':
            sql = """
                SELECT * FROM charging_target_data
                WHERE version_id = :version_id AND region = :region
            """
            return await db_manager.execute_query_df(sql, {'version_id': version_id, 'region': region})
        
        else:
            return pd.DataFrame()
    
    def apply_filters(self, df: pd.DataFrame, filters: Dict, data_type: str = 'station') -> pd.DataFrame:
        """在内存中应用筛选条件（使用pandas）"""
        if df.empty or not filters:
            return df
        
        # 省份筛选
        if filters.get('provinces') and len(filters['provinces']) > 0:
            df = df[df['province'].isin(filters['provinces'])]
        
        # 城市筛选
        if filters.get('cities') and len(filters['cities']) > 0:
            df = df[df['city'].isin(filters['cities'])]
        
        if data_type == 'penetration':
            # 车型筛选
            if filters.get('carModels') and len(filters['carModels']) > 0:
                df = df[df['car_model'].isin(filters['carModels'])]
            
            # 车型组筛选
            if filters.get('carTypes') and len(filters['carTypes']) > 0:
                selected_models = []
                if 'hybrid' in filters['carTypes']:
                    selected_models.extend(['L9', 'L8', 'L7', 'L6', 'ONE'])
                if 'electric' in filters['carTypes']:
                    selected_models.extend(['MEGA', 'W01', 'i6', 'i8'])
                
                if selected_models:
                    df = df[df['car_model'].isin(selected_models)]
        
        elif data_type == 'station':
            # 场站类型筛选
            if filters.get('stationTypes') and len(filters['stationTypes']) > 0:
                type_map = {
                    'store': '城市门店',
                    'self': '城市自营',
                    'franchise': '城市加盟'
                }
                mapped_types = [type_map.get(t, t) for t in filters['stationTypes']]
                df = df[df['station_type'].isin(mapped_types)]
            
            # 新老站筛选
            if filters.get('stationAges') and len(filters['stationAges']) > 0:
                age_map = {
                    'new': '新站',
                    'old': '老站'
                }
                mapped_ages = [age_map.get(a, a) for a in filters['stationAges']]
                df = df[df['station_age'].isin(mapped_ages)]
        
        return df
    
    def calculate_metrics(self, station_df: pd.DataFrame, penetration_df: pd.DataFrame) -> Dict:
        """计算各项指标"""
        metrics = {}
        
        # 1. 公充渗透率
        if len(penetration_df) > 0:
            total_self = penetration_df['self_charge_volume'].sum()
            total_public = penetration_df['public_charge_volume'].sum()
            penetration_rate = (total_self / total_public * 100) if total_public > 0 else 0
            metrics['penetration'] = {
                'value': f"{penetration_rate:.1f}%",
                'numerator': float(total_self),
                'denominator': float(total_public)
            }
        else:
            metrics['penetration'] = {'value': '0.0%', 'numerator': 0, 'denominator': 0}
        
        # 2. Net收入
        if len(station_df) > 0:
            revenue = (
                station_df['service_fee_after_share'].sum() +
                station_df['card_sales_revenue'].sum() +
                station_df['parking_fee_revenue'].sum() -
                station_df['coupon_discount'].sum() -
                station_df['owner_discount'].sum() -
                station_df['card_discount'].sum()
            ) / 10000  # 转换为万元
            metrics['revenue'] = {'value': f"{revenue:.0f}万", 'amount': float(revenue)}
        else:
            metrics['revenue'] = {'value': '0万', 'amount': 0}
        
        # 3. 单枪日服务费收入
        if len(station_df) > 0 and 'gun_days' in station_df.columns:
            total_gun_days = station_df['gun_days'].sum()
            if total_gun_days > 0:
                net_service_fee = (
                    station_df['service_fee_before_share'].sum() -
                    station_df['owner_discount'].sum() -
                    station_df['coupon_discount'].sum() -
                    station_df['card_discount'].sum()
                )
                service_fee = net_service_fee / total_gun_days
                metrics['service'] = {'value': f"{service_fee:.0f}元", 'amount': float(service_fee)}
            else:
                metrics['service'] = {'value': '0元', 'amount': 0}
        else:
            metrics['service'] = {'value': '0元', 'amount': 0}
        
        # 4. 单枪日电量
        if len(station_df) > 0 and 'gun_days' in station_df.columns:
            total_gun_days = station_df['gun_days'].sum()
            if total_gun_days > 0:
                power = station_df['charging_volume'].sum() / total_gun_days
                metrics['power'] = {'value': f"{power:.0f}度", 'amount': float(power)}
            else:
                metrics['power'] = {'value': '0度', 'amount': 0}
        else:
            metrics['power'] = {'value': '0度', 'amount': 0}
        
        return metrics
    
    async def get_national_metrics(self, version_id: Optional[int] = None) -> Dict:
        """获取全国指标"""
        if version_id is None:
            version_id = await db_manager.get_latest_version_id()
        
        if version_id is None:
            return {}
        
        # ========== 新增：获取当前季度 ==========
        current_date = datetime.now()
        current_year = current_date.year
        current_quarter = (current_date.month - 1) // 3 + 1
        quarter_key = f"{current_year}-Q{current_quarter}"
        
        # ========== 修改：查询季度聚合数据 ==========
        station_sql = """
            SELECT * FROM charging_aggregated_station_data
            WHERE version_id = :version_id 
            AND aggregation_type = 'quarterly'
            AND period_key = :quarter_key
        """
        station_df = await db_manager.execute_query_df(station_sql, {
            'version_id': version_id,
            'quarter_key': quarter_key
        })
        
        penetration_sql = """
            SELECT * FROM charging_aggregated_penetration_data
            WHERE version_id = :version_id 
            AND aggregation_type = 'quarterly'
            AND period_key = :quarter_key
        """
        penetration_df = await db_manager.execute_query_df(penetration_sql, {
            'version_id': version_id,
            'quarter_key': quarter_key
        })
        
        # 计算指标（现在是季度累计）
        metrics = self.calculate_metrics(station_df, penetration_df)
        
        # 获取目标值（保持原样）
        target_sql = """
            SELECT metric_name, target_value
            FROM charging_target_data
            WHERE version_id = :version_id AND region = '全国' AND target_period = 'Q1'
        """
        target_df = await db_manager.execute_query_df(target_sql, {'version_id': version_id})
        
        # 合并目标值
        national_data = {}
        metric_map = {
            '公充渗透率': 'penetration',
            'net收入': 'revenue',
            '单桩日服务费收入': 'service',
            '单桩日均充电量': 'power'
        }
        
        # ✅ 修改：安全地处理 target_df
        if not target_df.empty:
            for _, row in target_df.iterrows():
                # ✅ 使用数据库的实际列名 metric_name 和 target_value
                metric_name = row.get('metric_name', '')
                target_value = row.get('target_value', 0)
                
                metric_key = metric_map.get(metric_name)
                if metric_key and metric_key in metrics:
                    target = convert_to_float(target_value)  # ✅ 使用 convert_to_float 确保类型安全
                    actual = metrics[metric_key].get('amount', 0)
                    
                    if metric_key == 'penetration':
                        target_str = f"{target:.1f}%"
                        actual_str = metrics[metric_key]['value']
                        # 对于百分比，actual已经是百分比形式
                        actual_num = float(actual_str.replace('%', '')) if isinstance(actual_str, str) else actual
                        achievement_rate = (actual_num / target * 100) if target > 0 else 0
                    elif metric_key == 'revenue':
                        target_str = f"{target:,.0f}万"
                        actual_str = metrics[metric_key]['value']
                        achievement_rate = (actual / target * 100) if target > 0 else 0
                    elif metric_key == 'service':
                        target_str = f"{target:.0f}元"
                        actual_str = metrics[metric_key]['value']
                        achievement_rate = (actual / target * 100) if target > 0 else 0
                    else:  # power
                        target_str = f"{target:.0f}度"
                        actual_str = metrics[metric_key]['value']
                        achievement_rate = (actual / target * 100) if target > 0 else 0
                    
                    national_data[metric_key] = {
                        'target': target_str,
                        'actual': actual_str,
                        'achievementRate': round(min(100, achievement_rate), 2)
                    }
        else:
            # ✅ 如果没有目标数据，返回默认值
            for metric_key in ['penetration', 'revenue', 'service', 'power']:
                if metric_key in metrics:
                    national_data[metric_key] = {
                        'target': '--',
                        'actual': metrics[metric_key]['value'],
                        'achievementRate': 0
                    }
        
        return national_data
    
def convert_to_float(value):
    """将Decimal或其他类型安全转换为float"""
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except:
        return 0.0

# 创建全局数据服务实例
data_service = DataService()