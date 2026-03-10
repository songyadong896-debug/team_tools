# aggregation_service.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from db_manager import db_manager

class AggregationService:
    
    @staticmethod
    def aggregate_by_month(df: pd.DataFrame, data_type: str = 'station') -> pd.DataFrame:
        """月度聚合"""
        df['统计日期'] = pd.to_datetime(df['统计日期'])
        df['年月'] = df['统计日期'].dt.to_period('M')
        
        if data_type == 'station' and '场站ID' in df.columns:
            # 第一步：按场站计算每个月的运营天数
            station_monthly = df.groupby(['年月', '区域', '省份', '城市', '场站类型', '新老站', '场站ID']).agg({
                '统计日期': 'nunique',  # 运营天数
                '场站枪数': 'mean',     # 平均枪数
                '订单服务费收入（扣除分成）': 'sum',
                '电卡销售收入': 'sum',
                '占位费收入': 'sum',
                '优惠券优惠金额': 'sum',
                '车主优惠金额': 'sum',
                '电卡优惠金额': 'sum',
                '充电量': 'sum',
                '订单服务费收入（不扣除分成）': 'sum'
            }).reset_index()
            
            station_monthly.rename(columns={'统计日期': '运营天数'}, inplace=True)
            station_monthly['枪天数'] = station_monthly['场站枪数'] * station_monthly['运营天数']
            
            # 第二步：按维度汇总
            monthly_df = station_monthly.groupby(['年月', '区域', '省份', '城市', '场站类型', '新老站']).agg({
                '订单服务费收入（扣除分成）': 'sum',
                '电卡销售收入': 'sum',
                '占位费收入': 'sum',
                '优惠券优惠金额': 'sum',
                '车主优惠金额': 'sum',
                '电卡优惠金额': 'sum',
                '充电量': 'sum',
                '订单服务费收入（不扣除分成）': 'sum',
                '枪天数': 'sum',
                '场站ID': 'nunique',
                '场站枪数': 'sum'
            }).reset_index()
            
            monthly_df.rename(columns={'场站ID': '场站数'}, inplace=True)
            
        elif data_type == 'penetration' and '车型' in df.columns:
            # 渗透率数据聚合
            group_cols = ['年月', '区域', '省份', '城市', '车型']
            monthly_df = df.groupby(group_cols).agg({
                '自建站充电量': 'sum',
                '公充电量': 'sum'
            }).reset_index()
        
        # 转换日期格式
        monthly_df['统计日期'] = monthly_df['年月'].apply(lambda x: x.to_timestamp())
        monthly_df.drop('年月', axis=1, inplace=True)
        
        return monthly_df
    
    @staticmethod
    def aggregate_by_week(df: pd.DataFrame, data_type: str = 'station') -> pd.DataFrame:
        """周度聚合 - ISO周标准（周一到周日）"""
        df = df.copy()
        df['统计日期'] = pd.to_datetime(df['统计日期'])
        
        # 使用ISO周标准
        iso_cal = df['统计日期'].dt.isocalendar()
        df['iso_year'] = iso_cal.year
        df['iso_week'] = iso_cal.week
        df['年周'] = df['iso_year'].astype(str) + '-W' + df['iso_week'].astype(str).str.zfill(2)
            
        if data_type == 'station' and '场站ID' in df.columns:
            # 第一步：按场站和周计算
            station_weekly = df.groupby(['年周', '区域', '省份', '城市', '场站类型', '新老站', '场站ID']).agg({
                '统计日期': 'nunique',
                '场站枪数': 'first',
                '订单服务费收入（扣除分成）': 'sum',
                '电卡销售收入': 'sum',
                '占位费收入': 'sum',
                '优惠券优惠金额': 'sum',
                '车主优惠金额': 'sum',
                '电卡优惠金额': 'sum',
                '充电量': 'sum',
                '订单服务费收入（不扣除分成）': 'sum'
            }).reset_index()
            
            station_weekly.rename(columns={'统计日期': '运营天数'}, inplace=True)
            station_weekly['枪天数'] = station_weekly['场站枪数'] * station_weekly['运营天数']
            
            # 第二步：按维度汇总
            weekly_df = station_weekly.groupby(['年周', '区域', '省份', '城市', '场站类型', '新老站']).agg({
                '订单服务费收入（扣除分成）': 'sum',
                '电卡销售收入': 'sum',
                '占位费收入': 'sum',
                '优惠券优惠金额': 'sum',
                '车主优惠金额': 'sum',
                '电卡优惠金额': 'sum',
                '充电量': 'sum',
                '订单服务费收入（不扣除分成）': 'sum',
                '枪天数': 'sum',
                '场站ID': 'nunique',
                '场站枪数': 'sum'
            }).reset_index()
            
            weekly_df.rename(columns={'场站ID': '场站数'}, inplace=True)
            
        elif data_type == 'penetration' and '车型' in df.columns:
            group_cols = ['年周', '区域', '省份', '城市', '车型']
            weekly_df = df.groupby(group_cols).agg({
                '自建站充电量': 'sum',
                '公充电量': 'sum'
            }).reset_index()
        
        # 计算每周的周一日期
        def get_monday_of_week(year, week):
            """获取指定ISO年周的周一日期"""
            jan_4 = pd.Timestamp(year, 1, 4)
            week_1_monday = jan_4 - pd.Timedelta(days=jan_4.dayofweek)
            return week_1_monday + pd.Timedelta(weeks=week-1)
        
        weekly_df['year'] = weekly_df['年周'].str.extract(r'(\d{4})-W\d{2}')[0].astype(int)
        weekly_df['week'] = weekly_df['年周'].str.extract(r'\d{4}-W(\d{2})')[0].astype(int)
        weekly_df['统计日期'] = weekly_df.apply(
            lambda row: get_monday_of_week(row['year'], row['week']), axis=1
        )
        weekly_df.drop(['year', 'week'], axis=1, inplace=True)
        return weekly_df
    
    @staticmethod
    def aggregate_by_quarter(df: pd.DataFrame, data_type: str = 'station') -> pd.DataFrame:
        """季度聚合"""
        df = df.copy()
        df['统计日期'] = pd.to_datetime(df['统计日期'])
        df['年份'] = df['统计日期'].dt.year
        df['季度'] = df['统计日期'].dt.quarter
        df['年季'] = df['年份'].astype(str) + '-Q' + df['季度'].astype(str)
        
        if data_type == 'station' and '场站ID' in df.columns:
            # 第一步：计算每个场站在季度内的运营天数
            station_quarterly = df.groupby(['年季', '年份', '季度', '区域', '省份', '城市', '场站类型', '新老站', '场站ID']).agg({
                '统计日期': 'nunique',
                '场站枪数': 'mean',
                '订单服务费收入（扣除分成）': 'sum',
                '电卡销售收入': 'sum',
                '占位费收入': 'sum',
                '优惠券优惠金额': 'sum',
                '车主优惠金额': 'sum',
                '电卡优惠金额': 'sum',
                '充电量': 'sum',
                '订单服务费收入（不扣除分成）': 'sum'
            }).reset_index()
            
            station_quarterly.rename(columns={'统计日期': '运营天数'}, inplace=True)
            station_quarterly['枪天数'] = station_quarterly['场站枪数'] * station_quarterly['运营天数']
            
            # 第二步：按维度汇总
            quarterly_df = station_quarterly.groupby(['年季', '年份', '季度', '区域', '省份', '城市', '场站类型', '新老站']).agg({
                '订单服务费收入（扣除分成）': 'sum',
                '电卡销售收入': 'sum',
                '占位费收入': 'sum',
                '优惠券优惠金额': 'sum',
                '车主优惠金额': 'sum',
                '电卡优惠金额': 'sum',
                '充电量': 'sum',
                '订单服务费收入（不扣除分成）': 'sum',
                '枪天数': 'sum',
                '场站ID': 'nunique',
                '场站枪数': 'sum'
            }).reset_index()
            
            quarterly_df.rename(columns={'场站ID': '场站数'}, inplace=True)
            
        elif data_type == 'penetration' and '车型' in df.columns:
            quarterly_df = df.groupby(['年季', '年份', '季度', '区域', '省份', '城市', '车型']).agg({
                '自建站充电量': 'sum',
                '公充电量': 'sum'
            }).reset_index()
        
        # 设置统计日期为季度第一天
        quarterly_df['统计日期'] = quarterly_df.apply(
            lambda row: pd.Timestamp(row['年份'], (row['季度']-1)*3+1, 1), axis=1
        )
        
        return quarterly_df
    
    @staticmethod
    async def save_aggregated_data(version_id: int, station_df: pd.DataFrame, penetration_df: pd.DataFrame):
        """保存聚合数据到数据库"""
        
        # 1. 月度聚合
        station_monthly = AggregationService.aggregate_by_month(station_df, 'station')
        penetration_monthly = AggregationService.aggregate_by_month(penetration_df, 'penetration')
        
        # 保存月度数据
        for _, group in station_monthly.groupby(['统计日期']):
            period_key = group.iloc[0]['统计日期'].strftime('%Y-%m')
            await db_manager.insert_aggregated_station_data(version_id, group, 'monthly', period_key)
        
        for _, group in penetration_monthly.groupby(['统计日期']):
            period_key = group.iloc[0]['统计日期'].strftime('%Y-%m')
            await db_manager.insert_aggregated_penetration_data(version_id, group, 'monthly', period_key)
        
        # 2. 周度聚合
        station_weekly = AggregationService.aggregate_by_week(station_df, 'station')
        penetration_weekly = AggregationService.aggregate_by_week(penetration_df, 'penetration')
        
        # 保存周度数据
        for week in station_weekly['年周'].unique():
            week_data = station_weekly[station_weekly['年周'] == week]
            await db_manager.insert_aggregated_station_data(version_id, week_data, 'weekly', week)
        
        for week in penetration_weekly['年周'].unique():
            week_data = penetration_weekly[penetration_weekly['年周'] == week]
            await db_manager.insert_aggregated_penetration_data(version_id, week_data, 'weekly', week)
        
        # 3. 季度聚合
        station_quarterly = AggregationService.aggregate_by_quarter(station_df, 'station')
        penetration_quarterly = AggregationService.aggregate_by_quarter(penetration_df, 'penetration')
        
        # 保存季度数据
        for quarter in station_quarterly['年季'].unique():
            quarter_data = station_quarterly[station_quarterly['年季'] == quarter]
            await db_manager.insert_aggregated_station_data(version_id, quarter_data, 'quarterly', quarter)
        
        for quarter in penetration_quarterly['年季'].unique():
            quarter_data = penetration_quarterly[penetration_quarterly['年季'] == quarter]
            await db_manager.insert_aggregated_penetration_data(version_id, quarter_data, 'quarterly', quarter)
    
    @staticmethod
    async def update_filter_cache(version_id: int, station_df: pd.DataFrame, penetration_df: pd.DataFrame):
        """更新筛选选项缓存"""
        regions = set(station_df['区域'].unique()) | set(penetration_df['区域'].unique())
        
        for region in regions:
            # 过滤区域数据
            region_station = station_df[station_df['区域'] == region]
            region_pen = penetration_df[penetration_df['区域'] == region]
            
            # 省份
            provinces = sorted(set(region_station['省份'].unique()) | set(region_pen['省份'].unique()))
            await db_manager.update_filter_cache(version_id, region, 'province', provinces)
            
            # 城市
            cities = sorted(set(region_station['城市'].unique()) | set(region_pen['城市'].unique()))
            await db_manager.update_filter_cache(version_id, region, 'city', cities)
            
            # 车型
            car_models = sorted(region_pen['车型'].unique()) if len(region_pen) > 0 else []
            await db_manager.update_filter_cache(version_id, region, 'car_model', car_models)
            
            # 场站类型
            station_types = sorted(region_station['场站类型'].unique()) if len(region_station) > 0 else []
            await db_manager.update_filter_cache(version_id, region, 'station_type', station_types)
            
            # 新老站
            station_ages = sorted(region_station['新老站'].unique()) if len(region_station) > 0 else []
            await db_manager.update_filter_cache(version_id, region, 'station_age', station_ages)

# 创建全局聚合服务实例
aggregation_service = AggregationService()