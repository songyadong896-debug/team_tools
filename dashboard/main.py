# ====== lcpcode1:引用管理 ===========
# 负责人：乔岳
# =============Start==================

# 标准库
from typing import Dict, List, Optional, Any, Tuple
import os
import uuid
import shutil
import traceback
import sys
import logging
import json
import io
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from pathlib import Path
from decimal import Decimal
from collections import defaultdict
from urllib.parse import quote
from typing import List, Optional, Union, Dict, Any

# 第三方库
from fastapi import FastAPI, Depends, HTTPException, status, Header, Request, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
import uvicorn
import pandas as pd
import numpy as np

# 本地模块
import models
import schemas
import crud
from database import async_engine, get_db, AsyncSessionLocal
from charging_models import *
from db_manager import db_manager
from data_service import data_service
from aggregation_service import aggregation_service
from src.dashboard.pxxdash.pxx_api import router as pxx_router
from src.tools.htmlmanage.htmlm_router import router as html_manage_router 
from function_datahandle import (
    DataRegistry,
    UploadRequest,
    UploadResponse
)

# ====== lcpcode1:引用管理 ===========
# 负责人：乔岳
# =============End==================

upload_tasks = {}

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def convert_decimals(obj):
    """递归转换字典/列表中的Decimal为float"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_decimals(item) for item in obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    else:
        return obj

# 筛选条件模型


class FilterRequest(BaseModel):
    provinces: Optional[List[str]] = []
    cities: Optional[List[str]] = []
    carModels: Optional[List[str]] = []
    carTypes: Optional[List[str]] = []
    stationTypes: Optional[List[str]] = []
    stationAges: Optional[List[str]] = []

# 看板数据模型


class DashboardData(BaseModel):
    data: Dict[str, Any]
    timestamp: Optional[str] = None
    user: Optional[str] = "system"  # 新增用户字段

# 看板指标计算器模型


class MetricsCalculator:
    """指标计算器 - 统一处理所有指标计算逻辑"""

    def __init__(self, raw_data: Dict):
        self.raw_data = raw_data
        self.current_date = datetime.now()

        # 添加数据验证
        self._validate_data()

        self.latest_month = self._get_latest_month()
        self.current_year = self.latest_month.split(
            '-')[0] if self.latest_month else str(self.current_date.year)

        # 新增：识别目标周期
        self.target_period = self._identify_target_period()

        # 修复：直接设置周期名称，而不是调用不存在的方法
        if 'H1' in self.target_period:
            self.period_name = f"{self.current_year}年H1"
        elif '全年' in self.target_period:
            self.period_name = f"{self.current_year}年全年"
        else:
            self.period_name = f"{self.current_year}年"

        self.ytd_days = self._get_ytd_days()

    def _identify_target_period(self) -> str:
        """识别目标周期"""
        target_data = self.raw_data.get('目标', [])
        if not target_data or len(target_data) <= 1:
            return f"{self.current_year}H1"  # 默认值

        # 从目标数据中提取周期（假设周期在第一列）
        for row in target_data[1:]:
            if len(row) >= 1 and row[0]:
                period = str(row[0]).strip()
                # 验证周期格式
                if len(period) >= 4 and period[:4].isdigit():
                    return period

        return f"{self.current_year}H1"

    def _validate_data(self):
        """验证数据完整性"""
        required_sheets = [
            '分月指标达成情况',
            '理想车主公充渗透率月度',
            '用户分层',
            '互联互通指标达成'
        ]

        for sheet in required_sheets:
            if sheet not in self.raw_data:
                print(f"警告：缺少数据表 {sheet}")
                self.raw_data[sheet] = []
            elif len(self.raw_data[sheet]) <= 1:
                print(f"警告：数据表 {sheet} 没有数据行")

    def _process_target_data(self):
        """处理目标数据为前端需要的格式"""
        target_data = self.raw_data.get('目标', [])
        if not target_data or len(target_data) <= 1:
            return {}

        result = {}
        headers = target_data[0]

        for row in target_data[1:]:
            if len(row) >= 4:
                metric = row[1]  # 指标名称
                dimension = row[2]  # 维度
                value = row[3]  # 目标值

                if metric not in result:
                    result[metric] = {}
                result[metric][dimension] = value

        return result

    def _determine_category(self, main_type: str, sub_type: str) -> str:
        """根据主类型和子类型确定类别"""
        if main_type == '城市':
            if sub_type in ['自营', '自建']:
                return '城市自营'
            elif '加盟' in sub_type:
                return '城市加盟'
            elif '门店' in sub_type:
                return '城市门店'
            else:
                return '城市整体'
        elif main_type == '高速':
            if '线上' in sub_type:
                return '高速线上'
            elif '线下' in sub_type:
                return '高速线下'
            else:
                return '高速整体'
        elif main_type == '旅游':
            return '旅游线路'
        else:
            return '全国整体'

    def _parse_month(self, date_str: str) -> Optional[str]:
        """统一的月份解析函数"""
        if not date_str:
            return None

        try:
            # 去除空格
            date_str = str(date_str).strip()

            # 处理各种日期格式
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) >= 2:
                    year = parts[0]
                    month = parts[1].zfill(2)
                    return f"{year}-{month}"
            elif '-' in date_str:
                # 已经是 YYYY-MM 格式
                if len(date_str) >= 7:
                    return date_str[:7]
            elif len(date_str) == 6 and date_str.isdigit():
                # YYYYMM 格式
                return f"{date_str[:4]}-{date_str[4:6]}"
            elif '年' in date_str and '月' in date_str:
                # 2024年10月 格式
                import re
                match = re.match(r'(\d{4})年(\d{1,2})月', date_str)
                if match:
                    year = match.group(1)
                    month = match.group(2).zfill(2)
                    return f"{year}-{month}"
        except Exception as e:
            print(f"解析月份失败: {date_str}, 错误: {e}")

        return None

    def _get_latest_month(self) -> str:
        """获取数据中的最新月份"""
        all_months = set()

        sources = [
            ('分月指标达成情况', 0),
            ('理想车主公充渗透率月度', 0),
            ('用户分层', 0),
            ('互联互通指标达成', 0)
        ]

        for source_name, time_col in sources:
            data = self.raw_data.get(source_name, [])
            if data and len(data) > 1:
                for row in data[1:]:
                    if len(row) > time_col and row[time_col]:
                        month = self._parse_month(str(row[time_col]))
                        if month:
                            all_months.add(month)

        return max(all_months) if all_months else datetime.now().strftime("%Y-%m")

    def _get_ytd_days(self) -> int:
        """获取年初至今的天数"""
        year_start = datetime(int(self.current_year), 1, 1)
        # 如果有最新月份，计算到该月底的天数
        if self.latest_month:
            year, month = self.latest_month.split('-')
            # 计算该月最后一天
            if int(month) == 12:
                next_month = datetime(int(year) + 1, 1, 1)
            else:
                next_month = datetime(int(year), int(month) + 1, 1)
            month_end = next_month - timedelta(days=1)
            return (month_end - year_start).days + 1
        else:
            return (self.current_date - year_start).days + 1

    def calculate_all_metrics(self) -> Dict:
        """计算所有指标"""
        monthly_data = self.raw_data.get('分月指标达成情况', [])

        return {
            "核心KPI": self._calculate_core_kpi(),
            "趋势数据": self._calculate_trends(),
            "用户分层": self._calculate_user_layer(),
            "区域数据": self._calculate_region_data(),
            "目标": self._process_target_data(),  # ✅ 修改这行，使用新方法
            "按时长统计": self._calculate_duration_stats(monthly_data),  # ✅ 新增这行
            "最新月份": self.latest_month,
            "数据日期": self.current_date.strftime("%Y-%m-%d"),
            "时间进度": self._calculate_time_progress()
        }

    def _calculate_time_progress(self) -> float:
        """根据目标周期计算时间进度"""
        # 提取年份
        year_str = self.target_period[:4]
        try:
            year = int(year_str)
        except:
            year = self.current_date.year

        # 根据周期类型确定起止日期
        if 'H1' in self.target_period:
            period_start = datetime(year, 1, 1)
            period_end = datetime(year, 6, 30)
        elif '全年' in self.target_period:
            # H2是全年目标
            period_start = datetime(year, 1, 1)
            period_end = datetime(year, 12, 31)
        else:
            # 全年目标
            period_start = datetime(year, 1, 1)
            period_end = datetime(year, 12, 31)

        # 计算进度
        if self.current_date < period_start:
            progress = 0
        elif self.current_date > period_end:
            progress = 100
        else:
            total_days = (period_end - period_start).days + 1
            elapsed_days = (self.current_date - period_start).days + 1
            progress = (elapsed_days / total_days) * 100

        return round(progress, 1)

    def _calculate_core_kpi(self) -> Dict:
        """计算核心KPI"""
        monthly_data = self.raw_data.get('分月指标达成情况', [])

        if not monthly_data or len(monthly_data) <= 1:
            return {}

        # 初始化结果结构
        kpi = {
            "充电网络站桩数": {},
            "充电网络收入": {},
            "总充电量": {},
            "单枪日服务费收入": {},
            "单枪日电量": {},
            "理想车主公充电量渗透率": {}
        }

        # 1. 计算站桩数（最新月份的累计值）
        latest_month_data = self._get_latest_month_stations(monthly_data)
        kpi["充电网络站桩数"] = latest_month_data

        # 2. 计算YTD累计指标（充电网络收入、总充电量）
        ytd_data = self._calculate_ytd_cumulative(monthly_data)
        kpi["充电网络收入"] = ytd_data["revenue"]
        kpi["总充电量"] = ytd_data["power"]

        # ✓ 新增：从分月单枪日指标表获取数据
        self.daily_metrics_data = self._process_daily_metrics_table()
        kpi["单枪日服务费收入"] = self.daily_metrics_data['ytd_metrics']['revenue']
        kpi["单枪日电量"] = self.daily_metrics_data['ytd_metrics']['power']

        # 4. 计算渗透率
        kpi["理想车主公充电量渗透率"] = self._calculate_penetration_rates()

        # 5. 添加互联互通数据
        self._add_interconnect_data(kpi)

        return kpi
    # ✓ 新增：从分月单枪日指标表读取数据

    def _process_daily_metrics_table(self) -> Dict:
        """处理分月单枪日指标表数据"""
        print("\n=== 处理单枪日指标表 ===")

        daily_data = self.raw_data.get('分月单枪日指标表', [])

        if not daily_data or len(daily_data) <= 1:
            print("警告：分月单枪日指标表为空或只有表头")
            return {
                'ytd_metrics': {'revenue': {}, 'power': {}},
                'trends': {'revenue': {}, 'power': {}},
                'duration_metrics': {}
            }

        headers = daily_data[0]
        cols = {
            'time': headers.index('时间（月）'),
            'region': headers.index('区域'),
            'province': headers.index('省份'),
            'city': headers.index('城市'),
            'main_type': headers.index('场站主类型'),
            'sub_type': headers.index('场站子类型'),
            'duration': headers.index('上线时长标签'),
            'daily_power': headers.index('单枪日电量'),
            'daily_revenue': headers.index('单枪日服务费')
        }

        # 存储全年累计数据
        yearly_cumulative = {'revenue': {}, 'power': {}}

        # 用于趋势数据
        trend_data = defaultdict(lambda: defaultdict(
            lambda: {'months': [], 'values': []}))

        # 按时长的全年累计数据
        yearly_duration_data = defaultdict(lambda: defaultdict(lambda: {
            'power': 0, 'revenue': 0
        }))

        # ✅ 新增：定义维度与场站类型的映射关系
        dimension_mapping = {
            '全国整体': {'main_type': '全国', 'sub_type': '全国'},
            '城市整体': {'main_type': '城市', 'sub_type': '整体'},
            '城市自营': {'main_type': '城市', 'sub_type': '自营'},
            '城市加盟': {'main_type': '城市', 'sub_type': '加盟'},
            '城市门店': {'main_type': '城市', 'sub_type': '门店'},
            '高速整体': {'main_type': '高速', 'sub_type': '整体'},
            '高速线上': {'main_type': '高速', 'sub_type': '线上'},
            '高速线下': {'main_type': '高速', 'sub_type': '线下'},
            '旅游线路': {'main_type': '旅游', 'sub_type': '旅游'}
        }

        # 处理每一行数据
        for row in daily_data[1:]:
            time_value = str(row[cols['time']])
            region = row[cols['region']]
            province = row[cols['province']]
            city = row[cols['city']]
            main_type = row[cols['main_type']]
            sub_type = row[cols['sub_type']]
            duration = row[cols['duration']]

            # ✅ 修改：严格筛选条件 - 只处理区域='全国'的数据
            if region != '全国':
                continue

            power = float(row[cols['daily_power']] or 0)
            revenue = float(row[cols['daily_revenue']] or 0)

            # ✅ 修改：遍历所有维度，只有场站类型匹配的才记录数据
            for dimension, mapping in dimension_mapping.items():
                # 检查场站主类型和子类型是否匹配
                if main_type == mapping['main_type'] and sub_type == mapping['sub_type']:
                    # 处理全年累计数据（卡片展示）
                    if time_value == '全年累计' and duration == '全部':
                        yearly_cumulative['power'][dimension] = power
                        yearly_cumulative['revenue'][dimension] = revenue
                        print(f"卡片数据 - {dimension}: 收入={revenue}, 电量={power}")

                    # 处理全年累计的时长分类数据（详细数据表）
                    elif time_value == '全年累计' and duration != '全部':
                        yearly_duration_data[duration][dimension]['power'] = power
                        yearly_duration_data[duration][dimension]['revenue'] = revenue
                        print(
                            f"时长数据 - {dimension}[{duration}]: 收入={revenue}, 电量={power}")

                    # 处理月度数据（趋势图）
                    elif time_value != '全年累计' and duration == '全部':
                        month = self._parse_month(time_value)
                        if month:
                            month_display = f"{int(month.split('-')[1])}月"
                            if month_display not in trend_data['单枪日服务费收入'][dimension]['months']:
                                trend_data['单枪日服务费收入'][dimension]['months'].append(
                                    month_display)
                                trend_data['单枪日服务费收入'][dimension]['values'].append(
                                    revenue)
                                trend_data['单枪日电量'][dimension]['months'].append(
                                    month_display)
                                trend_data['单枪日电量'][dimension]['values'].append(
                                    power)
                                print(
                                    f"趋势数据 - {dimension}[{month_display}]: 收入={revenue}, 电量={power}")

        # 使用全年累计数据作为YTD指标
        ytd_metrics = yearly_cumulative

        print(f"\n最终数据统计:")
        print(f"卡片展示数据: {ytd_metrics}")
        print(f"按时长分类数据条数: {len(yearly_duration_data)}")
        print(
            f"趋势数据维度数: 收入={len(trend_data['单枪日服务费收入'])}, 电量={len(trend_data['单枪日电量'])}")

        # 只保留最近6个月的趋势数据
        # for metric in trend_data:
        #     for dimension in trend_data[metric]:
        #         if len(trend_data[metric][dimension]['months']) > 6:
        #             trend_data[metric][dimension]['months'] = trend_data[metric][dimension]['months'][-6:]
        #             trend_data[metric][dimension]['values'] = trend_data[metric][dimension]['values'][-6:]

        return {
            'ytd_metrics': ytd_metrics,
            'trends': dict(trend_data),
            'duration_metrics': dict(yearly_duration_data)
        }

    def _get_latest_month_stations(self, monthly_data: List) -> Dict:
        """获取最新月份的站桩数（累计值）"""
        result = {}

        # 添加边界检查
        if not monthly_data or len(monthly_data) <= 1:
            print("警告：分月指标达成情况表没有数据")
            return result

        headers = monthly_data[0]

        # 安全地获取列索引
        def safe_index(header_name):
            try:
                return headers.index(header_name)
            except ValueError:
                print(f"警告：找不到列 '{header_name}'")
                return -1

        cols = {
            'time': safe_index('时间（月）'),
            'main_type': safe_index('场站主类型'),
            'sub_type': safe_index('场站子类型'),
            'stations': safe_index('自建站数量'),
            'piles': safe_index('桩数量')
        }

        # 检查必要的列是否存在
        if any(v == -1 for v in cols.values()):
            print("错误：缺少必要的列")
            return result

        # 初始化统计
        stats = defaultdict(lambda: {'stations': 0, 'piles': 0})
        piles_by_type = defaultdict(int)

        # 只处理最新月份的数据
        for i, row in enumerate(monthly_data[1:], 1):
            try:
                # 检查行数据是否完整
                if len(row) <= max(cols.values()):
                    print(f"警告：第{i+1}行数据不完整，跳过")
                    continue

                month = self._parse_month(str(row[cols['time']]))
                if month != self.latest_month:
                    continue

                main_type = str(row[cols['main_type']]
                                ) if cols['main_type'] < len(row) else ""
                sub_type = str(row[cols['sub_type']]
                               ) if cols['sub_type'] < len(row) else ""
                stations = int(row[cols['stations']]
                               or 0) if cols['stations'] < len(row) else 0
                piles = int(row[cols['piles']]
                            or 0) if cols['piles'] < len(row) else 0

                # 全国整体
                stats['全国整体']['stations'] += stations
                stats['全国整体']['piles'] += piles
                piles_by_type['全国整体'] += piles

                # 按主类型统计
                if main_type == '城市':
                    stats['城市整体']['stations'] += stations
                    stats['城市整体']['piles'] += piles
                    piles_by_type['城市整体'] += piles

                    if sub_type in ['自营', '自建']:
                        stats['城市自营']['stations'] += stations
                        stats['城市自营']['piles'] += piles
                        piles_by_type['城市自营'] += piles
                    elif '加盟' in sub_type:
                        stats['城市加盟']['stations'] += stations
                        stats['城市加盟']['piles'] += piles
                        piles_by_type['城市加盟'] += piles
                    elif '门店' in sub_type:
                        stats['城市门店']['stations'] += stations
                        stats['城市门店']['piles'] += piles
                        piles_by_type['城市门店'] += piles

                elif main_type == '高速':
                    stats['高速整体']['stations'] += stations
                    stats['高速整体']['piles'] += piles
                    piles_by_type['高速整体'] += piles

                    if '线上' in sub_type:
                        stats['高速线上']['stations'] += stations
                        stats['高速线上']['piles'] += piles
                        piles_by_type['高速线上'] += piles
                    elif '线下' in sub_type:
                        stats['高速线下']['stations'] += stations
                        stats['高速线下']['piles'] += piles
                        piles_by_type['高速线下'] += piles

                elif main_type == '旅游':
                    stats['旅游线路']['stations'] += stations
                    stats['旅游线路']['piles'] += piles
                    piles_by_type['旅游线路'] += piles

            except Exception as e:
                print(f"处理第{i+1}行数据时出错: {e}")
                continue

        # 转换为最终格式
        result = {k: dict(v) for k, v in stats.items()}
        result['piles'] = dict(piles_by_type)
        # 在方法末尾，确保piles字典包含所有需要的维度
        result['piles']['全国整体'] = sum(piles_by_type.values())
        # ✅ 关键修改：确保piles字典包含全国整体的桩数
        if '全国整体' in stats:
            result['piles']['全国整体'] = stats['全国整体']['piles']

        # 打印调试信息
        print(f"站桩数统计结果 - piles字典: {result['piles']}")

        return result

    def _calculate_ytd_cumulative(self, monthly_data: List) -> Dict:
        """计算年初至今累计值（收入、电量、服务费收入）"""
        headers = monthly_data[0]

        # 列索引
        cols = {
            'time': headers.index('时间（月）'),
            'main_type': headers.index('场站主类型'),
            'sub_type': headers.index('场站子类型'),
            'revenue': headers.index('充电收入'),
            'power': headers.index('充电量'),
            # 'service_revenue': headers.index('充电服务费收入')
        }

        # 初始化累计统计
        cumulative = defaultdict(lambda: {
            'revenue': 0,
            'power': 0,
            # 'service_revenue': 0
        })

        # 累计当年所有月份的数据
        for row in monthly_data[1:]:
            month = self._parse_month(str(row[cols['time']]))
            if not month or not month.startswith(self.current_year):
                continue

            main_type = str(row[cols['main_type']])
            sub_type = str(row[cols['sub_type']])
            revenue = float(row[cols['revenue']] or 0)
            power = float(row[cols['power']] or 0)
            # service_revenue = float(row[cols['service_revenue']] or 0)

            # 全国整体
            cumulative['全国整体']['revenue'] += revenue
            cumulative['全国整体']['power'] += power
            # cumulative['全国整体']['service_revenue'] += service_revenue

            # 按类型统计
            if main_type == '城市':
                cumulative['城市整体']['revenue'] += revenue
                cumulative['城市整体']['power'] += power
                # cumulative['城市整体']['service_revenue'] += service_revenue

                if sub_type in ['自营', '自建']:
                    cumulative['城市自营']['revenue'] += revenue
                    cumulative['城市自营']['power'] += power
                    # cumulative['城市自营']['service_revenue'] += service_revenue
                elif '加盟' in sub_type:
                    cumulative['城市加盟']['revenue'] += revenue
                    cumulative['城市加盟']['power'] += power
                    # cumulative['城市加盟']['service_revenue'] += service_revenue
                elif '门店' in sub_type:
                    cumulative['城市门店']['revenue'] += revenue
                    cumulative['城市门店']['power'] += power
                    # cumulative['城市门店']['service_revenue'] += service_revenue

            elif main_type == '高速':
                cumulative['高速整体']['revenue'] += revenue
                cumulative['高速整体']['power'] += power
                # cumulative['高速整体']['service_revenue'] += service_revenue

                if '线上' in sub_type:
                    cumulative['高速线上']['revenue'] += revenue
                    cumulative['高速线上']['power'] += power
                    # cumulative['高速线上']['service_revenue'] += service_revenue
                elif '线下' in sub_type:
                    cumulative['高速线下']['revenue'] += revenue
                    cumulative['高速线下']['power'] += power
                    # cumulative['高速线下']['service_revenue'] += service_revenue

            elif main_type == '旅游':
                cumulative['旅游线路']['revenue'] += revenue
                cumulative['旅游线路']['power'] += power
                # cumulative['旅游线路']['service_revenue'] += service_revenue

        # 返回格式化后的结果
        return {
            'revenue': {k: v['revenue'] for k, v in cumulative.items()},
            'power': {k: v['power'] for k, v in cumulative.items()},
            'cumulative': dict(cumulative)  # 保留完整数据用于计算单枪日指标
        }

    def _calculate_daily_metrics(self, cumulative_data: Dict, piles_data: Dict) -> Dict:
        """计算单枪日指标"""
        ytd_days = self._get_ytd_days()

        daily_revenue = {}
        daily_power = {}

        for category, data in cumulative_data.items():
            piles = piles_data.get(category, 0)
            if piles > 0 and ytd_days > 0:
                daily_revenue[category] = round(
                    data['revenue'] / piles / ytd_days, 1)
                daily_power[category] = round(
                    data['power'] / piles / ytd_days, 1)

        return {
            'revenue': daily_revenue,
            'power': daily_power
        }

    def _calculate_penetration_rates(self) -> Dict:
        """计算理想车主公充电量渗透率（YTD累计）"""
        penetration_data = self.raw_data.get('理想车主公充渗透率月度', [])

        if not penetration_data or len(penetration_data) <= 1:
            return {}

        headers = penetration_data[0]
        cols = {
            'time': headers.index('时间（月）'),
            'model': headers.index('车型'),
            'fast_charge': headers.index('理想快充电量'),
            'self_station': headers.index('理想自建站充电量')
        }

        # 按车型累计YTD数据
        model_ytd = defaultdict(lambda: {'fast': 0, 'self': 0})

        for row in penetration_data[1:]:
            month = self._parse_month(str(row[cols['time']]))
            if not month or not month.startswith(self.current_year):
                continue

            model = str(row[cols['model']])
            fast_charge = float(row[cols['fast_charge']] or 0)
            self_station = float(row[cols['self_station']] or 0)

            model_ytd[model]['fast'] += fast_charge
            model_ytd[model]['self'] += self_station

        # 计算渗透率
        result = {}
        for model, data in model_ytd.items():
            if data['fast'] > 0:
                result[model] = round((data['self'] / data['fast']) * 100, 1)

        # 计算整体渗透率
        phev_models = ['L6', 'L7', 'L8', 'L9', 'ONE']
        bev_models = ['MEGA', 'i8', 'i6']

        # 增程整体
        phev_fast = sum(model_ytd[m]['fast']
                        for m in phev_models if m in model_ytd)
        phev_self = sum(model_ytd[m]['self']
                        for m in phev_models if m in model_ytd)
        if phev_fast > 0:
            result['增程整体'] = round((phev_self / phev_fast) * 100, 1)

        # 纯电整体
        bev_fast = sum(model_ytd[m]['fast']
                       for m in bev_models if m in model_ytd)
        bev_self = sum(model_ytd[m]['self']
                       for m in bev_models if m in model_ytd)
        if bev_fast > 0:
            result['纯电整体'] = round((bev_self / bev_fast) * 100, 1)

        # 全部车型整体
        total_fast = sum(data['fast'] for data in model_ytd.values())
        total_self = sum(data['self'] for data in model_ytd.values())
        if total_fast > 0:
            result['全部车型整体'] = round((total_self / total_fast) * 100, 1)

        return result

    def _calculate_duration_stats(self, monthly_data: List) -> Dict:
        """计算按上线时长分类的统计数据"""
        if not monthly_data or len(monthly_data) <= 1:
            return {}

        headers = monthly_data[0]

        # 获取列索引
        cols = {
            'time': headers.index('时间（月）'),
            'main_type': headers.index('场站主类型'),
            'sub_type': headers.index('场站子类型'),
            'duration': headers.index('上线时长标签'),
            'revenue': headers.index('充电收入'),
            'power': headers.index('充电量'),
            'stations': headers.index('自建站数量'),
            'piles': headers.index('桩数量')
        }

        # 按时长和类型统计
        duration_stats = defaultdict(lambda: defaultdict(lambda: {
            'stations': 0, 'piles': 0, 'revenue': 0, 'power': 0, 'service_revenue': 0
        }))

        # 处理数据
        for row in monthly_data[1:]:
            try:
                month = self._parse_month(str(row[cols['time']]))
                if not month:
                    continue

                duration = row[cols['duration']]
                main_type = row[cols['main_type']]
                sub_type = row[cols['sub_type']]

                # 确定类别
                category = self._determine_category(main_type, sub_type)

                # ✅ 修改：站桩数只统计最新月份，收入和电量统计YTD
                if month == self.latest_month:
                    # 站桩数 - 只累加最新月份
                    duration_stats[duration][category]['stations'] += int(
                        row[cols['stations']] or 0)
                    duration_stats[duration][category]['piles'] += int(
                        row[cols['piles']] or 0)
                    duration_stats[duration]['全国整体']['stations'] += int(
                        row[cols['stations']] or 0)
                    duration_stats[duration]['全国整体']['piles'] += int(
                        row[cols['piles']] or 0)

                    if main_type == '城市':
                        duration_stats[duration]['城市整体']['stations'] += int(
                            row[cols['stations']] or 0)
                        duration_stats[duration]['城市整体']['piles'] += int(
                            row[cols['piles']] or 0)
                    elif main_type == '高速':
                        duration_stats[duration]['高速整体']['stations'] += int(
                            row[cols['stations']] or 0)
                        duration_stats[duration]['高速整体']['piles'] += int(
                            row[cols['piles']] or 0)

                # 收入和电量 - 累计当年所有月份
                if month.startswith(self.current_year):
                    duration_stats[duration][category]['revenue'] += float(
                        row[cols['revenue']] or 0)
                    duration_stats[duration][category]['power'] += float(
                        row[cols['power']] or 0)
                    duration_stats[duration]['全国整体']['revenue'] += float(
                        row[cols['revenue']] or 0)
                    duration_stats[duration]['全国整体']['power'] += float(
                        row[cols['power']] or 0)
                    if main_type == '城市':
                        duration_stats[duration]['城市整体']['revenue'] += float(
                            row[cols['revenue']] or 0)
                        duration_stats[duration]['城市整体']['power'] += float(
                            row[cols['power']] or 0)
                    elif main_type == '高速':
                        duration_stats[duration]['高速整体']['revenue'] += float(
                            row[cols['revenue']] or 0)
                        duration_stats[duration]['高速整体']['power'] += float(
                            row[cols['power']] or 0)

            except Exception as e:
                print(f"处理时长统计时出错: {e}")
                continue

        # ✅ 合并单枪日指标的时长数据（保持不变）
        if hasattr(self, 'daily_metrics_data') and self.daily_metrics_data:
            duration_daily_metrics = self.daily_metrics_data.get(
                'duration_metrics', {})

            # 将单枪日数据添加到duration_stats中
            for duration, dimensions in duration_daily_metrics.items():
                if duration not in duration_stats:
                    duration_stats[duration] = defaultdict(lambda: {
                        'stations': 0, 'piles': 0, 'revenue': 0, 'power': 0,
                        'daily_revenue': 0, 'daily_power': 0
                    })

                for dimension, metrics in dimensions.items():
                    duration_stats[duration][dimension]['daily_revenue'] = metrics['revenue']
                    duration_stats[duration][dimension]['daily_power'] = metrics['power']

        return dict(duration_stats)

    def _add_interconnect_data(self, kpi: Dict):
        """添加互联互通数据"""
        interconnect_data = self.raw_data.get('互联互通指标达成', [])

        if not interconnect_data or len(interconnect_data) <= 1:
            return

        headers = interconnect_data[0]

        # 简化列索引，只需要前7列
        cols = {
            'time': 0,  # 时间（月）
            'total_stations': 1,  # 全量站数
            'total_piles': 2,  # 全量桩数
            'premium_stations': 3,  # 优选站数
            'premium_piles': 4,  # 优选桩数
            'revenue': 5,  # 充电收入
            'power': 6,  # 充电量
        }

        # 找到最新月份的数据
        latest_row = None
        for row in interconnect_data[1:]:
            try:
                if len(row) <= cols['time']:
                    continue

                month = self._parse_month(str(row[cols['time']]))
                if month == self.latest_month:
                    latest_row = row
                    break
            except:
                continue

        if latest_row and len(latest_row) > cols['power']:
            # 站桩数
            kpi["充电网络站桩数"]['互联互通全量'] = {
                'stations': int(latest_row[cols['total_stations']] or 0),
                'piles': int(latest_row[cols['total_piles']] or 0)
            }
            kpi["充电网络站桩数"]['互联互通优选'] = {
                'stations': int(latest_row[cols['premium_stations']] or 0),
                'piles': int(latest_row[cols['premium_piles']] or 0)
            }

            # 收入和电量（需要累计YTD）
            ytd_revenue = 0
            ytd_power = 0

            for row in interconnect_data[1:]:
                try:
                    if len(row) <= cols['power']:
                        continue

                    month = self._parse_month(str(row[cols['time']]))
                    if month and month.startswith(self.current_year):
                        ytd_revenue += float(row[cols['revenue']] or 0)
                        ytd_power += float(row[cols['power']] or 0)
                except:
                    continue

            kpi["充电网络收入"]['互联互通'] = ytd_revenue
            kpi["总充电量"]['互联互通'] = ytd_power

            # 不再处理单枪日指标
    def _calculate_trends(self) -> Dict:
        """计算趋势数据"""
        monthly_data = self.raw_data.get('分月指标达成情况', [])

        if not monthly_data or len(monthly_data) <= 1:
            return {}

        headers = monthly_data[0]
        # 安全地获取列索引

        def safe_index(header_name):
            try:
                return headers.index(header_name)
            except ValueError:
                return -1
        cols = {
            'time': headers.index('时间（月）'),
            'main_type': headers.index('场站主类型'),
            'sub_type': headers.index('场站子类型'),
            'stations': headers.index('自建站数量'),
            'piles': headers.index('桩数量'),
            'revenue': headers.index('充电收入'),
            'power': headers.index('充电量'),
            # 'service_revenue': headers.index('充电服务费收入'),
            'days': headers.index('当月运营天数')
        }

        # 检查必要的列
        if any(v == -1 for k, v in cols.items() if k != 'service_revenue'):
            print("警告：分月指标达成情况表缺少必要的列")
            return {}

        # 获取当年所有岳飞
        all_months = set()
        for row in monthly_data[1:]:
            try:
                if len(row) > cols['time']:
                    month = self._parse_month(str(row[cols['time']]))
                    if month:
                        all_months.add(month)
            except:
                continue

        sorted_months = sorted(all_months)

        # 初始化趋势数据结构
        trends = defaultdict(lambda: defaultdict(
            lambda: {'months': [], 'values': []}))

        # 站桩数趋势（累计值）
        station_cumulative = defaultdict(lambda: defaultdict(int))

        # 按月份顺序处理
        for month in sorted_months:
            month_data = defaultdict(lambda: {
                'stations': 0, 'piles': 0, 'revenue': 0, 'power': 0, 'total_piles': 0, 'days': 0
            })

            # 聚合当月数据
            for row in monthly_data[1:]:
                try:
                    if len(row) <= max(cols.values()):
                        continue

                    if self._parse_month(str(row[cols['time']])) == month:
                        main_type = str(row[cols['main_type']])
                        sub_type = str(row[cols['sub_type']])

                        # 当月值
                        revenue = float(row[cols['revenue']] or 0)
                        power = float(row[cols['power']] or 0)
                        # service_revenue = float(row[cols['service_revenue']] or 0) if cols['service_revenue'] != -1 else revenue
                        piles = int(row[cols['piles']] or 0)
                        days = int(row[cols['days']] or 1)

                        # 累计站桩数
                        stations = int(row[cols['stations']] or 0)
                        station_cumulative[month]['全国整体'] += stations

                        # 全国整体
                        month_data['全国整体']['revenue'] += revenue
                        month_data['全国整体']['power'] += power
                        # month_data['全国整体']['service_revenue'] += service_revenue
                        month_data['全国整体']['total_piles'] += piles
                        month_data['全国整体']['days'] = max(
                            month_data['全国整体']['days'], days)

                        # 分类统计
                        category_map = {
                            ('城市', None): '城市整体',
                            ('城市', '自营'): '城市自营',
                            ('城市', '自建'): '城市自营',
                            ('城市', '加盟'): '城市加盟',
                            ('城市', '门店'): '城市门店',
                            ('高速', None): '高速整体',
                            ('高速', '线上'): '高速线上',
                            ('高速', '线下'): '高速线下',
                            ('旅游', None): '旅游线路'
                        }

                        for (m_type, s_type), category in category_map.items():
                            if main_type == m_type and (s_type is None or sub_type == s_type or s_type in sub_type):
                                month_data[category]['revenue'] += revenue
                                month_data[category]['power'] += power
                                # month_data[category]['service_revenue'] += service_revenue
                                month_data[category]['total_piles'] += piles
                                month_data[category]['days'] = max(
                                    month_data[category]['days'], days)
                                station_cumulative[month][category] += stations
                except Exception as e:
                    print(f"处理趋势数据时出错: {e}")
                    continue

            # 保存趋势数据
            month_display = f"{int(month.split('-')[1])}月"

            # 站桩数趋势（累计值）
            for category in station_cumulative[month].keys():
                if category not in trends['充电网络站桩数']:
                    trends['充电网络站桩数'][category] = {'months': [], 'values': []}
                trends['充电网络站桩数'][category]['months'].append(month_display)
                trends['充电网络站桩数'][category]['values'].append(
                    station_cumulative[month][category])

            # 其他指标趋势（当月值）
            for category, data in month_data.items():
                # 充电网络收入
                trends['充电网络收入'][category]['months'].append(month_display)
                trends['充电网络收入'][category]['values'].append(data['revenue'])

                # 总充电量
                trends['总充电量'][category]['months'].append(month_display)
                trends['总充电量'][category]['values'].append(data['power'])

                # ✓ 新增：从已处理的数据中获取单枪日趋势
                if hasattr(self, 'daily_metrics_data') and self.daily_metrics_data:
                    if '单枪日服务费收入' in self.daily_metrics_data['trends']:
                        trends['单枪日服务费收入'] = self.daily_metrics_data['trends']['单枪日服务费收入']
                    if '单枪日电量' in self.daily_metrics_data['trends']:
                        trends['单枪日电量'] = self.daily_metrics_data['trends']['单枪日电量']

        # 添加渗透率趋势
        penetration_trends = self._calculate_penetration_trends()
        trends['理想车主公充电量渗透率'] = penetration_trends

        # ✅ 在这里添加用户分层趋势
        trends['用户分层'] = self._calculate_user_layer_trends()

        self._add_interconnect_trends(trends, sorted_months)

        print(f"\n=== 趋势数据调试 ===")
        print(f"数据中包含的所有月份: {sorted(list(all_months))}")
        print(f"选择的最近6个月: {sorted_months}")
        print(f"月份数量: {len(sorted_months)}")

        # 打印互联互通趋势数据以便调试
        if '充电网络收入' in trends and '互联互通' in trends['充电网络收入']:
            print(f"互联互通收入趋势: {trends['充电网络收入']['互联互通']}")
        if '总充电量' in trends and '互联互通' in trends['总充电量']:
            print(f"互联互通电量趋势: {trends['总充电量']['互联互通']}")

        return dict(trends)

    def _add_interconnect_trends(self, trends: Dict, sorted_months: List[str]):
        """添加互联互通趋势数据"""
        interconnect_data = self.raw_data.get('互联互通指标达成', [])

        if not interconnect_data or len(interconnect_data) <= 1:
            print("警告：互联互通指标达成表为空")
            return

        headers = interconnect_data[0]
        print(f"互联互通表头: {headers}")

        # 根据实际表头获取列索引
        cols = {
            'time': 0,  # 时间（月）
            'total_stations': 1,  # 全量站数
            'total_piles': 2,  # 全量桩数
            'premium_stations': 3,  # 优选站数
            'premium_piles': 4,  # 优选桩数
            'revenue': 5,  # 充电网络收入
            'power': 6,  # 总充电量
        }

        # 初始化趋势数据
        revenue_data = {'months': [], 'values': []}
        power_data = {'months': [], 'values': []}
        stations_data = {'months': [], 'values': []}  # 如果需要站数趋势

        # 处理每个月份
        for month in sorted_months:
            month_display = f"{int(month.split('-')[1])}月"
            found = False

            # 查找该月份的数据
            for row in interconnect_data[1:]:
                try:
                    if len(row) <= cols['power']:
                        continue

                    row_month = self._parse_month(str(row[cols['time']]))
                    if row_month == month:
                        # 收入趋势
                        revenue = float(row[cols['revenue']] or 0)
                        revenue_data['months'].append(month_display)
                        revenue_data['values'].append(revenue)

                        # 电量趋势
                        power = float(row[cols['power']] or 0)
                        power_data['months'].append(month_display)
                        power_data['values'].append(power)

                        # 站数趋势（如果需要）
                        stations = int(row[cols['total_stations']] or 0)
                        stations_data['months'].append(month_display)
                        stations_data['values'].append(stations)

                        found = True
                        break
                except Exception as e:
                    print(f"处理互联互通数据行时出错: {e}")
                    continue

            # 如果没有找到该月份的数据，添加0值保持连续性
            if not found:
                revenue_data['months'].append(month_display)
                revenue_data['values'].append(0)
                power_data['months'].append(month_display)
                power_data['values'].append(0)
                stations_data['months'].append(month_display)
                stations_data['values'].append(0)

        # 将数据添加到趋势中
        if len(revenue_data['values']) > 0 and any(v > 0 for v in revenue_data['values']):
            if '充电网络收入' not in trends:
                trends['充电网络收入'] = {}
            trends['充电网络收入']['互联互通'] = revenue_data
            print(f"添加互联互通收入趋势: {len(revenue_data['values'])}个数据点")

        if len(power_data['values']) > 0 and any(v > 0 for v in power_data['values']):
            if '总充电量' not in trends:
                trends['总充电量'] = {}
            trends['总充电量']['互联互通'] = power_data
            print(f"添加互联互通电量趋势: {len(power_data['values'])}个数据点")

    def _calculate_penetration_trends(self) -> Dict:
        """计算渗透率趋势"""
        penetration_data = self.raw_data.get('理想车主公充渗透率月度', [])

        if not penetration_data or len(penetration_data) <= 1:
            return {}

        headers = penetration_data[0]

        # 修改后的匹配逻辑
        cols = {}
        for i, header in enumerate(headers):
            header_str = str(header)
            if '时间' in header_str or '月' in header_str:
                cols['time'] = i
            elif header_str == '车型':  # 精确匹配
                cols['model'] = i
            elif header_str == '理想快充电量':  # 精确匹配
                cols['fast_charge'] = i
            elif header_str == '理想自建站充电量':  # 精确匹配，避免匹配到高速自建站
                cols['self_station'] = i
            elif header_str == '理想高速自建站充电量':  # 单独处理
                cols['highway_self'] = i
            elif header_str == '理想城市充电量':  # 单独处理
                cols['city_charge'] = i

        # 如果没有找到，使用固定索引
        if 'time' not in cols:
            cols['time'] = 0
        if 'model' not in cols:
            cols['model'] = 4
        if 'fast_charge' not in cols:
            cols['fast_charge'] = 5
        if 'self_station' not in cols:
            cols['self_station'] = 6

        # 获取最近6个月
        all_months = set()
        for row in penetration_data[1:]:
            try:
                if len(row) > cols['time']:
                    month = self._parse_month(str(row[cols['time']]))
                    if month:
                        all_months.add(month)
            except:
                continue

        sorted_months = sorted(all_months)

        # 按月份和车型聚合数据
        monthly_model_data = defaultdict(
            lambda: defaultdict(lambda: {'fast': 0, 'self': 0}))

        for row in penetration_data[1:]:
            try:
                if len(row) <= max(cols.values()):
                    continue

                month = self._parse_month(str(row[cols['time']]))
                if month in sorted_months:
                    model = str(row[cols['model']])
                    fast_charge = float(row[cols['fast_charge']] or 0)
                    self_station = float(row[cols['self_station']] or 0)

                    monthly_model_data[month][model]['fast'] += fast_charge
                    monthly_model_data[month][model]['self'] += self_station
            except Exception as e:
                print(f"处理渗透率趋势数据时出错: {e}")
                continue

        # 生成趋势数据
        trends = {}
        all_models = ['L6', 'L7', 'L8', 'L9', 'ONE', 'MEGA', 'i8', 'i6']

        # 1. 计算各个车型的趋势
        for model in all_models:
            months = []
            values = []

            for month in sorted_months:
                month_display = f"{int(month.split('-')[1])}月"
                months.append(month_display)

                if model in monthly_model_data[month]:
                    data = monthly_model_data[month][model]
                    if data['fast'] > 0:
                        rate = round((data['self'] / data['fast']) * 100, 1)
                        values.append(rate)
                    else:
                        values.append(0)
                else:
                    values.append(0)

            if any(v > 0 for v in values):
                trends[model] = {'months': months, 'values': values}

        # 2. 计算增程整体和纯电整体趋势
        for group_name, models in [('增程整体', ['L6', 'L7', 'L8', 'L9', 'ONE']),
                                   ('纯电整体', ['MEGA', 'i8', 'i6'])]:
            months = []
            values = []

            for month in sorted_months:
                month_display = f"{int(month.split('-')[1])}月"
                months.append(month_display)

                total_fast = sum(monthly_model_data[month][m]['fast']
                                 for m in models if m in monthly_model_data[month])
                total_self = sum(monthly_model_data[month][m]['self']
                                 for m in models if m in monthly_model_data[month])

                if total_fast > 0:
                    rate = round((total_self / total_fast) * 100, 1)
                    values.append(rate)
                else:
                    values.append(0)

            if any(v > 0 for v in values):
                trends[group_name] = {'months': months, 'values': values}

        # 3. 计算全部车型整体趋势
        months = []
        values = []

        for month in sorted_months:
            month_display = f"{int(month.split('-')[1])}月"
            months.append(month_display)

            # 计算所有车型的总和
            total_fast = sum(data['fast'] for model_data in monthly_model_data[month].values(
            ) for data in [model_data])
            total_self = sum(data['self'] for model_data in monthly_model_data[month].values(
            ) for data in [model_data])

            if total_fast > 0:
                rate = round((total_self / total_fast) * 100, 1)
                values.append(rate)
            else:
                values.append(0)

        if any(v > 0 for v in values):
            trends['全部车型整体'] = {'months': months, 'values': values}

        return trends

    def _calculate_user_layer(self) -> Dict:
        """计算用户分层数据"""
        user_data = self.raw_data.get('用户分层', [])
        print("\n=== 用户分层计算诊断 ===")
        print(f"1. 原始数据行数: {len(user_data)}")
        print(f"2. 最新月份: {self.latest_month}")

        if user_data and len(user_data) > 0:
            print(f"3. 表头: {user_data[0]}")
            if len(user_data) > 1:
                print(f"4. 第一行数据示例: {user_data[1]}")
                # 打印前5行的时间列
                print("5. 前5行的时间数据:")
                for i in range(1, min(6, len(user_data))):
                    print(
                        f"   行{i}: {user_data[i][0] if len(user_data[i]) > 0 else 'None'}")

            # 测试月份解析
            if user_data and len(user_data) > 1:
                print("\n6. 月份解析测试:")
                for i in range(1, min(4, len(user_data))):
                    if len(user_data[i]) > 0:
                        raw_time = user_data[i][0]
                        parsed = self._parse_month(str(raw_time))
                        print(
                            f"   原始: '{raw_time}' -> 解析后: '{parsed}' -> 匹配最新月份: {parsed == self.latest_month}")

        third_party_data = self.raw_data.get('三方车辆充电量', [])

        result = {
            "分层数据": {},
            "三方车辆充电量": []
        }

        # 处理用户分层
        if user_data and len(user_data) > 1:
            headers = user_data[0]

            # 动态获取列索引
            try:
                cols = {
                    'time': headers.index('时间（月）'),
                    'energy_type': headers.index('能源形式'),
                    'model': headers.index('车型'),
                    'active': headers.index('活跃车主数'),
                    'potential': headers.index('潜在流失车主数'),
                    'lost': headers.index('流失车主数'),
                    'never': headers.index('从未使用车主数'),
                    'new': headers.index('新车主数'),
                    'first_charge': headers.index('完成首单车主数'),
                    'total': headers.index('车主保有量')
                }
            except ValueError as e:
                print(f"用户分层表列名错误: {e}")
                print(f"实际列名: {headers}")
                # 尝试使用备选列名
                try:
                    # 可能的备选列名映射
                    cols = {}
                    for i, header in enumerate(headers):
                        if '时间' in str(header) or '月' in str(header):
                            cols['time'] = i
                        elif '能源' in str(header):
                            cols['energy_type'] = i
                        elif '车型' in str(header):
                            cols['model'] = i
                        elif '活跃' in str(header):
                            cols['active'] = i
                        elif '潜在' in str(header):
                            cols['potential'] = i
                        elif '流失' in str(header) and '潜在' not in str(header):
                            cols['lost'] = i
                        elif '从未' in str(header):
                            cols['never'] = i
                        elif '新车主' in str(header) or '新用户' in str(header):
                            cols['new'] = i
                        elif '首充' in str(header) or '首单' in str(header):
                            cols['first_charge'] = i
                        elif '保有' in str(header) or '总量' in str(header):
                            cols['total'] = i

                    print(f"使用模糊匹配的列索引: {cols}")
                except Exception as e2:
                    print(f"列名模糊匹配也失败: {e2}")
                    return result

            # 检查是否找到所有必需的列
            required_cols = ['time', 'energy_type', 'model', 'active',
                             'potential', 'lost', 'never', 'new', 'first_charge', 'total']
            missing_cols = [col for col in required_cols if col not in cols]
            if missing_cols:
                print(f"警告：缺少列: {missing_cols}")

            # 处理数据的计数器
            processed_count = 0

            # 只处理最新月份
            for i, row in enumerate(user_data[1:], 1):
                try:
                    # 确保行有足够的列
                    if len(row) <= max(cols.values()):
                        print(f"行{i}数据不完整，跳过")
                        continue

                    month = self._parse_month(str(row[cols['time']]))
                    print(
                        f"行{i}: 原始时间='{row[cols['time']]}', 解析月份='{month}', 最新月份='{self.latest_month}'")

                    if month == self.latest_month:
                        energy_type = row[cols['energy_type']]
                        model = row[cols['model']]

                        # 安全地获取数值，处理可能的空值
                        def safe_int(value, default=0):
                            try:
                                return int(value) if value else default
                            except:
                                return default

                        total = safe_int(row[cols['total']], 1)  # 避免除零
                        new_owners = safe_int(row[cols['new']], 1)  # 避免除零

                        key = f"{energy_type}_{model}"
                        result["分层数据"][key] = {
                            "活跃用户占比": round((safe_int(row[cols['active']]) / total * 100), 1),
                            "潜在流失用户占比": round((safe_int(row[cols['potential']]) / total * 100), 1),
                            "流失用户占比": round((safe_int(row[cols['lost']]) / total * 100), 1),
                            "从未使用用户占比": round((safe_int(row[cols['never']]) / total * 100), 1),
                            "新用户首充率": round((safe_int(row[cols['first_charge']]) / new_owners * 100), 1) if new_owners > 0 else 0
                        }
                        processed_count += 1
                        print(f"成功处理: {key}")

                except Exception as e:
                    print(f"处理行{i}时出错: {e}")
                    continue

            print(f"共处理了{processed_count}行用户分层数据")

        # 处理三方车辆数据
        if third_party_data and len(third_party_data) > 1:
            headers = third_party_data[0]

            # 获取列索引
            try:
                cols = {
                    'time': headers.index('时间'),
                    'model': headers.index('车型'),
                    'charge_cars': headers.index('充电车数'),
                    'charge_power': headers.index('充电量'),
                    'charge_revenue': headers.index('充电收入'),
                    'charge_frequency': headers.index('充电频次'),
                    'total_cars': headers.index('全量三方充电车数'),
                    'total_power': headers.index('全量充电量')
                }
            except ValueError as e:
                print(f"三方车辆充电量表列名错误: {e}")
                print(f"实际列名: {headers}")
                return result

            # 先获取全量数据（从第一行数据中读取）
            total_cars = 0
            total_power = 0
            if len(third_party_data) > 1 and len(third_party_data[1]) > cols['total_power']:
                try:
                    total_cars = int(
                        third_party_data[1][cols['total_cars']] or 0)
                    total_power = float(
                        third_party_data[1][cols['total_power']] or 0)
                    print(f"全量三方充电车数: {total_cars}, 全量充电量: {total_power}")
                except Exception as e:
                    print(f"获取全量数据失败: {e}")

            # 处理每个车型的数据
            vehicles = []
            for row in third_party_data[1:]:
                try:
                    if len(row) > cols['charge_power']:
                        vehicle_name = row[cols['model']]
                        charge_cars = int(row[cols['charge_cars']] or 0)
                        charge_power = float(row[cols['charge_power']] or 0)

                        # 计算单车月均充电量
                        avg_power_per_car = charge_power / charge_cars if charge_cars > 0 else 0

                        vehicles.append({
                            "车型": vehicle_name,
                            "充电车数": charge_cars,
                            "充电量": charge_power,
                            "单车月均充电量": avg_power_per_car
                        })
                except Exception as e:
                    print(f"处理车型数据时出错: {e}")
                    continue

            # 按充电量排序
            vehicles.sort(key=lambda x: x['充电量'], reverse=True)

            # 取前15名，使用全量数据计算占比
            for vehicle in vehicles[:18]:
                result["三方车辆充电量"].append({
                    "车型": vehicle["车型"],
                    "充电车数": vehicle["充电车数"],
                    "充电车数%": round(vehicle["充电车数"] / total_cars * 100, 1) if total_cars > 0 else 0,
                    "充电量": vehicle["充电量"],
                    "充电量%": round(vehicle["充电量"] / total_power * 100, 1) if total_power > 0 else 0,
                    "单车月均充电量": round(vehicle["单车月均充电量"], 1)
                })

        return result

    def _calculate_user_layer_trends(self) -> Dict:
        """计算用户分层趋势数据"""
        user_data = self.raw_data.get('用户分层', [])

        if not user_data or len(user_data) <= 1:
            return {}

        headers = user_data[0]

        # 获取列索引
        cols = {
            'time': headers.index('时间（月）'),
            'energy': headers.index('能源形式'),
            'model': headers.index('车型'),
            'active': headers.index('活跃车主数'),
            'potential': headers.index('潜在流失车主数'),
            'lost': headers.index('流失车主数'),
            'never': headers.index('从未使用车主数'),
            'new': headers.index('新车主数'),
            'first_charge': headers.index('完成首单车主数'),
            'total': headers.index('车主保有量')
        }

        # 获取最近6个月
        all_months = set()
        for row in user_data[1:]:
            month = self._parse_month(str(row[cols['time']]))
            if month:
                all_months.add(month)

        sorted_months = sorted(all_months)

        # 按维度聚合数据
        trends = defaultdict(lambda: {
            'months': [],
            '活跃用户占比': [],
            '潜在流失用户占比': [],
            '流失用户占比': [],
            '从未使用用户占比': [],
            '新用户首充率': []
        })

        for month in sorted_months:
            month_display = f"{int(month.split('-')[1])}月"

            for row in user_data[1:]:
                if self._parse_month(str(row[cols['time']])) != month:
                    continue

                energy = row[cols['energy']]
                model = row[cols['model']]

                # 确定维度key
                if energy == '全部' and model == '全量':
                    key = '全量_全量'
                elif model == '整体':
                    key = f"{energy}_整体"
                else:
                    key = f"{energy}_{model}"

                # 计算各项占比
                total = float(row[cols['total']] or 1)  # 避免除零
                new_count = float(row[cols['new']] or 1)  # 避免除零

                if key not in trends:
                    trends[key]['months'] = []

                # 只在第一次添加月份
                if month_display not in trends[key]['months']:
                    trends[key]['months'].append(month_display)

                    # 计算占比
                    active_rate = round(
                        (float(row[cols['active']] or 0) / total) * 100, 1)
                    potential_rate = round(
                        (float(row[cols['potential']] or 0) / total) * 100, 1)
                    lost_rate = round(
                        (float(row[cols['lost']] or 0) / total) * 100, 1)
                    never_rate = round(
                        (float(row[cols['never']] or 0) / total) * 100, 1)
                    first_charge_rate = round(
                        (float(row[cols['first_charge']] or 0) / new_count) * 100, 1)

                    trends[key]['活跃用户占比'].append(active_rate)
                    trends[key]['潜在流失用户占比'].append(potential_rate)
                    trends[key]['流失用户占比'].append(lost_rate)
                    trends[key]['从未使用用户占比'].append(never_rate)
                    trends[key]['新用户首充率'].append(first_charge_rate)

        return dict(trends)

    def _calculate_region_data(self) -> Dict:
        """计算区域数据 - 城市区域KPI页面专用"""
        monthly_data = self.raw_data.get('分月指标达成情况', [])

        result = {
            "区域列表": ['东一区', '东二区', '南区', '西区', '北区', '中区'],
            "区域统计数据": {},
            "区域时长统计数据": {}
        }

        if not monthly_data or len(monthly_data) <= 1:
            print("警告：分月指标达成情况表为空")
            return result

        headers = monthly_data[0]
        print(f"表头列: {headers}")

        cols = {
            'time': 0,
            'region': 1,
            'province': 2,
            'city': 3,
            'main_type': 4,
            'sub_type': 5,
            'duration': 6,
            'revenue': 7,
            'power': 8,
            'piles': 9,
            'days': 10
        }

        regions = result["区域列表"]

        # 1. 初始化统计数据
        region_stats = {}

        # 初始化全国统计
        region_stats['全国'] = {
            '区域整体': {'revenue': 0, 'power': 0, 'latest_piles': 0},
            '城市自建': {'revenue': 0, 'power': 0, 'latest_piles': 0},
            '城市加盟': {'revenue': 0, 'power': 0, 'latest_piles': 0},
            '城市门店': {'revenue': 0, 'power': 0, 'latest_piles': 0}
        }

        # 初始化六大区域
        for region in regions:
            region_stats[region] = {
                '区域整体': {'revenue': 0, 'power': 0, 'latest_piles': 0},
                '城市自建': {'revenue': 0, 'power': 0, 'latest_piles': 0},
                '城市加盟': {'revenue': 0, 'power': 0, 'latest_piles': 0},
                '城市门店': {'revenue': 0, 'power': 0, 'latest_piles': 0}
            }

        # 2. 初始化按时长分组的统计
        duration_stats = {}

        # 收集所有时长标签
        all_durations = set()
        for row in monthly_data[1:]:
            if len(row) > cols['duration'] and row[cols['duration']]:
                duration_label = str(row[cols['duration']]).strip()
                if duration_label and duration_label != '全部':  # 排除"全部"
                    all_durations.add(duration_label)

        print(f"发现的时长标签: {all_durations}")

        # 为每个时长初始化数据结构
        for duration in all_durations:
            duration_stats[duration] = {}
            # 初始化全国
            duration_stats[duration]['全国'] = {
                '区域整体': {'revenue': 0, 'power': 0, 'latest_piles': 0},
                '城市自建': {'revenue': 0, 'power': 0, 'latest_piles': 0},
                '城市加盟': {'revenue': 0, 'power': 0, 'latest_piles': 0},
                '城市门店': {'revenue': 0, 'power': 0, 'latest_piles': 0}
            }
            # 初始化六大区域
            for region in regions:
                duration_stats[duration][region] = {
                    '区域整体': {'revenue': 0, 'power': 0, 'latest_piles': 0},
                    '城市自建': {'revenue': 0, 'power': 0, 'latest_piles': 0},
                    '城市加盟': {'revenue': 0, 'power': 0, 'latest_piles': 0},
                    '城市门店': {'revenue': 0, 'power': 0, 'latest_piles': 0}
                }

        # 3. 处理数据
        processed_count = 0
        duration_processed_count = 0

        for i, row in enumerate(monthly_data[1:], 1):
            try:
                # 确保行有足够的列
                if len(row) < 11:
                    print(f"跳过第{i}行：列数不足")
                    continue

                month = self._parse_month(str(row[cols['time']]))
                if not month or not month.startswith(self.current_year):
                    continue

                region_raw = str(row[cols['region']])

                # 处理区域名称，匹配六大区域
                matched_region = None
                for r in regions:
                    if region_raw == r or region_raw == f"{r}城市" or region_raw.startswith(r):
                        matched_region = r
                        break

                if not matched_region:
                    continue

                region = matched_region
                main_type = str(row[cols['main_type']])
                sub_type = str(row[cols['sub_type']])

                # 排除高速线上
                if main_type == '高速' and sub_type == '线上':
                    continue

                # 确定场站类型
                station_type = None
                if main_type == '城市':
                    if sub_type in ['自建', '自营']:
                        station_type = '城市自建'
                    elif sub_type == '加盟':
                        station_type = '城市加盟'
                    elif sub_type == '门店':
                        station_type = '城市门店'

                if not station_type:
                    continue

                # 获取数值
                revenue = float(row[cols['revenue']] or 0)
                power = float(row[cols['power']] or 0)
                piles = int(row[cols['piles']] or 0)

                # 获取时长标签
                duration = str(row[cols['duration']]).strip(
                ) if cols['duration'] < len(row) else ""

                # ✅ 关键修改：对于"全部场站"的统计，累计所有数据（不管时长标签）
                # 累计到区域
                region_stats[region][station_type]['revenue'] += revenue
                region_stats[region][station_type]['power'] += power
                region_stats[region]['区域整体']['revenue'] += revenue
                region_stats[region]['区域整体']['power'] += power

                # 累计到全国
                region_stats['全国'][station_type]['revenue'] += revenue
                region_stats['全国'][station_type]['power'] += power
                region_stats['全国']['区域整体']['revenue'] += revenue
                region_stats['全国']['区域整体']['power'] += power

                # 记录最新月份的桩数
                if month == self.latest_month:
                    region_stats[region][station_type]['latest_piles'] += piles
                    region_stats[region]['区域整体']['latest_piles'] += piles
                    region_stats['全国'][station_type]['latest_piles'] += piles
                    region_stats['全国']['区域整体']['latest_piles'] += piles

                processed_count += 1

                # ✅ 如果有具体时长标签，也累计到对应的时长统计
                if duration and duration in duration_stats:
                    # 累计到区域
                    duration_stats[duration][region][station_type]['revenue'] += revenue
                    duration_stats[duration][region][station_type]['power'] += power
                    duration_stats[duration][region]['区域整体']['revenue'] += revenue
                    duration_stats[duration][region]['区域整体']['power'] += power

                    # 累计到全国
                    duration_stats[duration]['全国'][station_type]['revenue'] += revenue
                    duration_stats[duration]['全国'][station_type]['power'] += power
                    duration_stats[duration]['全国']['区域整体']['revenue'] += revenue
                    duration_stats[duration]['全国']['区域整体']['power'] += power

                    # 记录最新月份的桩数
                    if month == self.latest_month:
                        duration_stats[duration][region][station_type]['latest_piles'] += piles
                        duration_stats[duration][region]['区域整体']['latest_piles'] += piles
                        duration_stats[duration]['全国'][station_type]['latest_piles'] += piles
                        duration_stats[duration]['全国']['区域整体']['latest_piles'] += piles

                    duration_processed_count += 1

            except Exception as e:
                print(f"处理第{i}行数据时出错: {e}")
                continue

        print(f"成功处理了 {processed_count} 行全部场站数据")
        print(f"成功处理了 {duration_processed_count} 行时长分类数据")

        # 打印时长统计结果
        for duration, stats in duration_stats.items():
            print(f"时长 {duration}: {len(stats)} 个区域")

        # 从"分月单枪日指标表"读取单枪日数据（保持不变）
        daily_data = self.raw_data.get('分月单枪日指标表', [])
        if daily_data and len(daily_data) > 1:
            self._merge_daily_metrics_to_region_data(
                region_stats, duration_stats, daily_data, regions
            )

        result["区域统计数据"] = region_stats
        result["区域时长统计数据"] = duration_stats

        return result

    def _calculate_province_metrics(self) -> Dict:
        """计算省份指标 - 新增方法"""
        monthly_data = self.raw_data.get('分月指标达成情况', [])
        weekly_data = self.raw_data.get('上周城市省份核心指标', [])
        weekly_penetration = self.raw_data.get('理想车主公充渗透率周度', [])
        monthly_penetration = self.raw_data.get('理想车主公充渗透率月度', [])
        daily_metrics_monthly = self.raw_data.get('分月单枪日指标表', [])  # ✅ 新增数据源

        result = {
            "年度合计": {},
            "上周表现": {},
            "趋势数据": {
                "充电总收入": {},
                "单枪日服务费收入": {},
                "单枪日充电量": {},
                "车主公充渗透率": {}
            }
        }

        if not monthly_data or len(monthly_data) <= 1:
            return result

        # 1. 计算年度合计
        result["年度合计"] = self._calculate_province_yearly_summary(monthly_data)

        # 2. 计算上周表现
        result["上周表现"] = self._calculate_province_weekly_performance(
            weekly_data, weekly_penetration)

        # 3. 计算趋势数据 - 修改：传入daily_metrics_monthly
        result["趋势数据"] = self._calculate_province_trends(
            monthly_data, monthly_penetration, daily_metrics_monthly)

        return result

    def _merge_daily_metrics_to_region_data(self, region_stats: Dict, duration_stats: Dict,
                                            daily_data: List, regions: List):
        """将分月单枪日指标表的数据合并到区域统计中"""
        headers = daily_data[0]
        cols = {
            'time': headers.index('时间（月）'),
            'region': headers.index('区域'),
            'province': headers.index('省份'),
            'city': headers.index('城市'),
            'main_type': headers.index('场站主类型'),
            'sub_type': headers.index('场站子类型'),
            'duration': headers.index('上线时长标签'),
            'daily_revenue': headers.index('单枪日服务费'),
            'daily_power': headers.index('单枪日电量')
        }

        # 处理每行数据
        for row in daily_data[1:]:
            time_value = str(row[cols['time']])
            if time_value != '全年累计':
                continue

            region = row[cols['region']]
            province = row[cols['province']]
            city = row[cols['city']]
            main_type = row[cols['main_type']]
            sub_type = row[cols['sub_type']]
            duration = row[cols['duration']]

            if main_type == '高速' and sub_type == '线上':
                continue

            # ✅ 核心修改：同时支持 region='全国' 和 region='全国城市'
            if region in ['全国城市']:
                display_region = '全国'

                # 根据场站类型确定station_type
                station_type = None

                # 区域整体：主类型和子类型都是'整体' 或者 都是'全国'
                if (main_type == '整体' and sub_type == '整体') or (main_type == '全国' and sub_type == '全国') or (main_type == '全部' and sub_type == '全部'):
                    station_type = '区域整体'
                # 城市自建
                elif main_type == '城市' and sub_type in ['自建', '自营']:
                    station_type = '城市自建'
                # 城市加盟
                elif main_type == '城市' and sub_type == '加盟':
                    station_type = '城市加盟'
                # 城市门店
                elif main_type == '城市' and sub_type == '门店':
                    station_type = '城市门店'

                # 如果识别出了场站类型，处理数据
                if station_type:
                    daily_revenue = float(row[cols['daily_revenue']] or 0)
                    daily_power = float(row[cols['daily_power']] or 0)

                    if duration == '全部':
                        # 确保字典结构存在
                        if display_region not in region_stats:
                            region_stats[display_region] = {}
                        if station_type not in region_stats[display_region]:
                            region_stats[display_region][station_type] = {
                                'revenue': 0, 'power': 0, 'latest_piles': 0,
                                'daily_revenue': 0, 'daily_power': 0
                            }

                        # 更新单枪日数据
                        region_stats[display_region][station_type]['daily_revenue'] = daily_revenue
                        region_stats[display_region][station_type]['daily_power'] = daily_power

                        print(f"✅ 更新全国-{station_type}: 区域={region}, 省份={province}, 城市={city}, "
                              f"主类型={main_type}, 子类型={sub_type}, "
                              f"单枪日收入={daily_revenue}, 单枪日电量={daily_power}")

                    else:
                        # 处理按时长分类的数据
                        if duration in duration_stats:
                            if display_region not in duration_stats[duration]:
                                duration_stats[duration][display_region] = {}
                            if station_type not in duration_stats[duration][display_region]:
                                duration_stats[duration][display_region][station_type] = {
                                    'revenue': 0, 'power': 0, 'latest_piles': 0,
                                    'daily_revenue': 0, 'daily_power': 0
                                }

                            duration_stats[duration][display_region][station_type]['daily_revenue'] = daily_revenue
                            duration_stats[duration][display_region][station_type]['daily_power'] = daily_power

                            print(f"✅ 更新全国-{station_type}[{duration}]: "
                                  f"单枪日收入={daily_revenue}, 单枪日电量={daily_power}")

                # 处理完全国数据后继续下一条
                continue

            # ✅ 处理六大区域的数据
            display_region = None

            # 判断是否为六大区域
            for r in regions:
                if region == f"{r}城市":
                    display_region = r
                    break

            if not display_region:
                continue

            # 判断是否为区域汇总数据
            is_region_summary = (province == region and city == region)

            # 只处理区域汇总数据（非城市明细）
            if not is_region_summary:
                continue

            # 确定场站类型
            station_type = self._determine_station_type_for_region(
                main_type, sub_type)
            if not station_type:
                continue

            daily_revenue = float(row[cols['daily_revenue']] or 0)
            daily_power = float(row[cols['daily_power']] or 0)

            # 更新数据
            if duration == '全部':
                # 更新全部场站的单枪日数据
                if display_region in region_stats and station_type in region_stats[display_region]:
                    region_stats[display_region][station_type]['daily_revenue'] = daily_revenue
                    region_stats[display_region][station_type]['daily_power'] = daily_power

                    print(
                        f"✅ 更新{display_region}-{station_type}: 单枪日收入={daily_revenue}, 单枪日电量={daily_power}")
            else:
                # 更新按时长分类的单枪日数据
                if duration in duration_stats and display_region in duration_stats[duration]:
                    if station_type in duration_stats[duration][display_region]:
                        duration_stats[duration][display_region][station_type]['daily_revenue'] = daily_revenue
                        duration_stats[duration][display_region][station_type]['daily_power'] = daily_power

                        print(f"✅ 更新{display_region}-{station_type}[{duration}]: "
                              f"单枪日收入={daily_revenue}, 单枪日电量={daily_power}")

    def _determine_station_type_for_region(self, main_type: str, sub_type: str) -> str:
        """根据场站主类型和子类型确定区域展示的场站类型"""
        # 区域整体
        if main_type == '全部' and sub_type == '全部':
            return '区域整体'
        elif main_type == '全国' and sub_type == '全国':  # 全国数据
            return '区域整体'
        elif main_type in ['东一区城市', '东二区城市', '南区城市', '西区城市', '北区城市', '中区城市'] and sub_type == '整体':
            return '区域整体'

        # 城市类型
        elif main_type == '城市':
            if sub_type == '自营':
                return '城市自建'
            elif sub_type == '加盟':
                return '城市加盟'
            elif sub_type == '门店':
                return '城市门店'

        return None

    def _calculate_province_yearly_summary(self, monthly_data: List) -> Dict:
        """计算省份年度合计指标"""
        headers = monthly_data[0]

        # 获取列索引
        cols = {
            'time': headers.index('时间（月）'),
            'province': headers.index('省份'),
            'city': headers.index('城市'),
            'owners': headers.index('车主保有量'),
            'stations': headers.index('自建站数量'),
            'recent_users': headers.index('近30天充电车主数'),
            'recent_revenue': headers.index('近30天充电收入')
        }

        # 初始化省份数据
        province_data = {}
        # 用于记录每个省份下已经计算过的城市
        province_city_owners = {}

        # 找到最新月份的数据，累加所有行
        for row in monthly_data[1:]:
            month = self._parse_month(str(row[cols['time']]))
            if month == self.latest_month:
                province = row[cols['province']]
                city = row[cols['city']]

                if province not in province_data:
                    province_data[province] = {
                        'owners': 0,
                        'stations': 0,
                        'recent_users': 0,
                        'recent_revenue': 0.0
                    }
                    province_city_owners[province] = set()

                # 车主保有量按城市去重，每个城市只计算一次
                if city not in province_city_owners[province]:
                    province_city_owners[province].add(city)
                    province_data[province]['owners'] += int(
                        row[cols['owners']] or 0)

                # 其他指标继续累加（每个城市、每个类型、每种标签）
                province_data[province]['stations'] += int(
                    row[cols['stations']] or 0)
                province_data[province]['recent_users'] += int(
                    row[cols['recent_users']] or 0)
                province_data[province]['recent_revenue'] += float(
                    row[cols['recent_revenue']] or 0)

        # 计算总计
        total_owners = sum(d['owners'] for d in province_data.values())
        total_stations = sum(d['stations'] for d in province_data.values())
        total_users = sum(d['recent_users'] for d in province_data.values())
        total_revenue = sum(d['recent_revenue']
                            for d in province_data.values())

        # 构建最终结果并计算占比
        province_summary = {}
        for province, data in province_data.items():
            province_summary[province] = {
                '车主保有量': data['owners'],
                '车主保有量%': round((data['owners'] / total_owners * 100) if total_owners > 0 else 0, 1),
                '累计自建场站数': data['stations'],
                '累计自建场站数%': round((data['stations'] / total_stations * 100) if total_stations > 0 else 0, 1),
                '最近30天充电车主数': data['recent_users'],
                '最近30天充电车主数%': round((data['recent_users'] / total_users * 100) if total_users > 0 else 0, 1),
                '最近30天充电收入': data['recent_revenue'],
                '最近30天充电收入%': round((data['recent_revenue'] / total_revenue * 100) if total_revenue > 0 else 0, 1)
            }

        return province_summary

    def _calculate_province_weekly_performance(self, weekly_data: List, weekly_penetration: List) -> Dict:
        """计算省份上周表现 - ✅ 完全重写"""
        if not weekly_data or len(weekly_data) <= 1:
            return {}

        headers = weekly_data[0]

        # ✅ 修改：新的列索引定义
        cols = {
            'province': headers.index('省份'),
            'city': headers.index('城市'),  # 新增
            'main_type': headers.index('场站主类型'),
            'sub_type': headers.index('场站子类型'),
            'daily_revenue': headers.index('单枪日服务费'),  # 直接读取
            'daily_power': headers.index('单枪日电量')  # 直接读取
        }

        # 按省份存储结果
        province_stats = {}

        # ✅ 新逻辑：直接读取预计算的单枪日数据
        for row in weekly_data[1:]:
            province = row[cols['province']]
            city = row[cols['city']]
            main_type = row[cols['main_type']]
            sub_type = row[cols['sub_type']]

            # 只处理城市=全部的汇总数据
            if city != '全部':
                continue

            if province not in province_stats:
                province_stats[province] = {}

            # 根据场站类型映射到对应字段
            if main_type == '全部':  # 总计
                province_stats[province]['单枪日收入_总计'] = float(
                    row[cols['daily_revenue']] or 0)
                province_stats[province]['单枪日电量_总计'] = float(
                    row[cols['daily_power']] or 0)
            elif sub_type == '线上':
                province_stats[province]['单枪日收入_高速线上'] = float(
                    row[cols['daily_revenue']] or 0)
                province_stats[province]['单枪日电量_高速线上'] = float(
                    row[cols['daily_power']] or 0)
            elif sub_type == '线下':
                province_stats[province]['单枪日收入_高速线下'] = float(
                    row[cols['daily_revenue']] or 0)
                province_stats[province]['单枪日电量_高速线下'] = float(
                    row[cols['daily_power']] or 0)
            elif main_type == '城市' and sub_type in ['自建', '自营']:
                province_stats[province]['单枪日收入_城市自建'] = float(
                    row[cols['daily_revenue']] or 0)
                province_stats[province]['单枪日电量_城市自建'] = float(
                    row[cols['daily_power']] or 0)
            elif main_type == '城市' and sub_type == '加盟':
                province_stats[province]['单枪日收入_城市加盟'] = float(
                    row[cols['daily_revenue']] or 0)
                province_stats[province]['单枪日电量_城市加盟'] = float(
                    row[cols['daily_power']] or 0)
            elif main_type == '城市' and sub_type == '门店':
                province_stats[province]['单枪日收入_城市门店'] = float(
                    row[cols['daily_revenue']] or 0)
                province_stats[province]['单枪日电量_城市门店'] = float(
                    row[cols['daily_power']] or 0)
            elif main_type == '旅游':
                province_stats[province]['单枪日收入_旅游线路'] = float(
                    row[cols['daily_revenue']] or 0)
                province_stats[province]['单枪日电量_旅游线路'] = float(
                    row[cols['daily_power']] or 0)

        # 计算渗透率（保持原逻辑不变）
        penetration_rates = self._calculate_weekly_penetration_by_province(
            weekly_penetration)

        # 合并渗透率数据
        for province in province_stats:
            if province in penetration_rates:
                province_stats[province].update(penetration_rates[province])
            else:
                province_stats[province]['渗透率_总计'] = 0
                province_stats[province]['渗透率_城市'] = 0
                province_stats[province]['渗透率_高速'] = 0

        return province_stats

    def _calculate_weekly_penetration_by_province(self, weekly_data: List) -> Dict:
        """计算上周各省份的渗透率"""
        if not weekly_data or len(weekly_data) <= 1:
            return {}

        headers = weekly_data[0]
        cols = {
            'time': headers.index('时间（周）'),
            'province': headers.index('省份'),
            'fast_charge': headers.index('理想快充电量'),
            'self_station': headers.index('理想自建站充电量'),
            'highway_self': headers.index('理想高速自建站充电量'),
            'city_charge': headers.index('理想城市充电量')
        }

        # 找到最新周
        latest_week = None
        for row in weekly_data[1:]:
            week = row[cols['time']]
            if not latest_week or week > latest_week:
                latest_week = week

        # 按省份聚合
        province_stats = defaultdict(lambda: {
            'fast_total': 0,
            'self_total': 0,
            'highway_total': 0,
            'city_total': 0
        })

        for row in weekly_data[1:]:
            if row[cols['time']] != latest_week:
                continue

            province = row[cols['province']]
            province_stats[province]['fast_total'] += float(
                row[cols['fast_charge']] or 0)
            province_stats[province]['self_total'] += float(
                row[cols['self_station']] or 0)
            province_stats[province]['highway_total'] += float(
                row[cols['highway_self']] or 0)
            province_stats[province]['city_total'] += float(
                row[cols['city_charge']] or 0)

        # 计算渗透率
        result = {}
        for province, stats in province_stats.items():
            result[province] = {
                '渗透率_总计': 0,
                '渗透率_城市': 0,
                '渗透率_高速': 0
            }

            if stats['fast_total'] > 0:
                result[province]['渗透率_总计'] = round(
                    (stats['self_total'] / stats['fast_total']) * 100, 1)
                result[province]['渗透率_城市'] = round(
                    (stats['city_total'] / stats['fast_total']) * 100, 1)
                result[province]['渗透率_高速'] = round(
                    (stats['highway_total'] / stats['fast_total']) * 100, 1)

        return result

    def _calculate_province_trends(self, monthly_data: List, monthly_penetration: List,
                                   daily_metrics_monthly: List) -> Dict:
        """计算省份趋势数据 - ✅ 修改：新增daily_metrics_monthly参数"""
        # 获取最近6个月
        all_months = set()
        headers = monthly_data[0]
        time_col = headers.index('时间（月）')

        for row in monthly_data[1:]:
            month = self._parse_month(str(row[time_col]))
            if month:
                all_months.add(month)

        sorted_months = sorted(all_months)

        # 各指标的趋势数据
        trends = {
            "充电总收入": {},
            "单枪日服务费收入": {},
            "单枪日充电量": {},
            "车主公充渗透率": {}
        }

        # 1. 计算充电收入趋势（保持原逻辑）
        self._calculate_province_revenue_trends(
            monthly_data, sorted_months, trends)

        # 2. ✅ 修改：计算单枪日指标趋势 - 从分月单枪日指标表读取
        self._calculate_province_daily_metrics_trends(
            daily_metrics_monthly, sorted_months, trends)

        # 3. 计算渗透率趋势（保持原逻辑）
        self._calculate_province_penetration_trends(
            monthly_penetration, sorted_months, trends)

        return trends

    def _calculate_province_revenue_trends(self, monthly_data: List, months: List, trends: Dict):
        """计算省份收入相关趋势 - ✅ 修改：只计算充电总收入"""
        headers = monthly_data[0]
        cols = {
            'time': headers.index('时间（月）'),
            'province': headers.index('省份'),
            'main_type': headers.index('场站主类型'),
            'revenue': headers.index('充电收入')
        }

        # ✅ 新增：定义有效的省份列表（复用上面的列表）
        valid_provinces = {
            # 省份
            '河北省', '山西省', '辽宁省', '吉林省', '黑龙江省', '江苏省', '浙江省',
            '安徽省', '福建省', '江西省', '山东省', '河南省', '湖北省', '湖南省',
            '广东省', '海南省', '四川省', '贵州省', '云南省', '陕西省', '甘肃省',
            '青海省', '台湾省',
            # 自治区
            '内蒙古自治区', '广西壮族自治区', '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区',
            # 直辖市
            '北京市', '天津市', '上海市', '重庆市',
            # 简称形式
            '河北', '山西', '辽宁', '吉林', '黑龙江', '江苏', '浙江', '安徽', '福建',
            '江西', '山东', '河南', '湖北', '湖南', '广东', '海南', '四川', '贵州',
            '云南', '陕西', '甘肃', '青海', '台湾', '内蒙古', '广西', '西藏', '宁夏',
            '新疆', '北京', '天津', '上海', '重庆', '香港', '澳门'
        }

        # 按月份和省份聚合数据
        for month in months:
            month_data = defaultdict(lambda: {
                '全部': {'revenue': 0},
                '高速': {'revenue': 0},
                '城市': {'revenue': 0}
            })

            for row in monthly_data[1:]:
                if self._parse_month(str(row[cols['time']])) == month:
                    province = row[cols['province']]

                    # ✅ 关键过滤：只处理有效省份
                    if province not in valid_provinces:
                        continue

                    main_type = row[cols['main_type']]
                    revenue = float(row[cols['revenue']] or 0)

                    # 全部
                    month_data[province]['全部']['revenue'] += revenue

                    # 按主类型
                    if main_type in ['高速', '城市']:
                        month_data[province][main_type]['revenue'] += revenue

            # 保存趋势数据
            month_display = f"{int(month.split('-')[1])}月"

            for province, data in month_data.items():
                # 只处理充电总收入
                for scene in ['全部', '高速', '城市']:
                    key = f"{province}_{scene}"
                    if key not in trends["充电总收入"]:
                        trends["充电总收入"][key] = {'months': [], 'values': []}
                    trends["充电总收入"][key]['months'].append(month_display)
                    trends["充电总收入"][key]['values'].append(
                        data[scene]['revenue'])

    def _calculate_province_daily_metrics_trends(self, daily_metrics_data: List,
                                                 months: List, trends: Dict):
        """计算省份单枪日指标趋势"""
        if not daily_metrics_data or len(daily_metrics_data) <= 1:
            return

        headers = daily_metrics_data[0]
        cols = {
            'time': headers.index('时间（月）'),
            'region': headers.index('区域'),
            'province': headers.index('省份'),
            'city': headers.index('城市'),
            'duration': headers.index('上线时长标签'),
            'main_type': headers.index('场站主类型'),
            'sub_type': headers.index('场站子类型'),
            'daily_revenue': headers.index('单枪日服务费'),
            'daily_power': headers.index('单枪日电量')
        }

        # 定义有效的省份列表
        valid_provinces = {
            # 省份
            '河北省', '山西省', '辽宁省', '吉林省', '黑龙江省', '江苏省', '浙江省',
            '安徽省', '福建省', '江西省', '山东省', '河南省', '湖北省', '湖南省',
            '广东省', '海南省', '四川省', '贵州省', '云南省', '陕西省', '甘肃省',
            '青海省', '台湾省',
            # 自治区
            '内蒙古自治区', '广西壮族自治区', '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区',
            # 直辖市
            '北京市', '天津市', '上海市', '重庆市',
            # 简称形式
            '河北', '山西', '辽宁', '吉林', '黑龙江', '江苏', '浙江', '安徽', '福建',
            '江西', '山东', '河南', '湖北', '湖南', '广东', '海南', '四川', '贵州',
            '云南', '陕西', '甘肃', '青海', '台湾', '内蒙古', '广西', '西藏', '宁夏',
            '新疆', '北京', '天津', '上海', '重庆', '香港', '澳门'
        }

        print(f"\n=== 省份单枪日趋势计算 ===")
        print(f"要处理的月份: {months}")

        # 遍历每个月份
        for month in months:
            month_display = f"{int(month.split('-')[1])}月"

            for row in daily_metrics_data[1:]:
                # 基础筛选
                if row[cols['city']] != '全部':
                    continue
                if row[cols['duration']] != '全部':
                    continue
                if row[cols['sub_type']] not in ['整体', '全部']:
                    continue

                province = row[cols['province']]
                if province not in valid_provinces:
                    continue

                # 时间匹配
                row_time = self._parse_month(str(row[cols['time']]))
                if not row_time:
                    continue

                # ✅ 关键修改：比较年月，而不是只比较月份
                if row_time != month:
                    continue

                main_type = row[cols['main_type']]
                daily_revenue = float(row[cols['daily_revenue']] or 0)
                daily_power = float(row[cols['daily_power']] or 0)

                # ✅ 修改场景映射，确保"全部"场景被正确处理
                if main_type == '全部':
                    # "全部"场景
                    key = f"{province}_全部"

                    # 单枪日服务费收入
                    if key not in trends["单枪日服务费收入"]:
                        trends["单枪日服务费收入"][key] = {'months': [], 'values': []}
                    trends["单枪日服务费收入"][key]['months'].append(month_display)
                    trends["单枪日服务费收入"][key]['values'].append(daily_revenue)

                    # 单枪日电量
                    if key not in trends["单枪日充电量"]:
                        trends["单枪日充电量"][key] = {'months': [], 'values': []}
                    trends["单枪日充电量"][key]['months'].append(month_display)
                    trends["单枪日充电量"][key]['values'].append(daily_power)

                    print(
                        f"✅ 添加全部场景: {province}_全部 - {month_display}: 收入={daily_revenue}, 电量={daily_power}")

                elif main_type == '高速':
                    # "高速"场景
                    key = f"{province}_高速"

                    if key not in trends["单枪日服务费收入"]:
                        trends["单枪日服务费收入"][key] = {'months': [], 'values': []}
                    trends["单枪日服务费收入"][key]['months'].append(month_display)
                    trends["单枪日服务费收入"][key]['values'].append(daily_revenue)

                    if key not in trends["单枪日充电量"]:
                        trends["单枪日充电量"][key] = {'months': [], 'values': []}
                    trends["单枪日充电量"][key]['months'].append(month_display)
                    trends["单枪日充电量"][key]['values'].append(daily_power)

                elif main_type == '城市':
                    # "城市"场景
                    key = f"{province}_城市"

                    if key not in trends["单枪日服务费收入"]:
                        trends["单枪日服务费收入"][key] = {'months': [], 'values': []}
                    trends["单枪日服务费收入"][key]['months'].append(month_display)
                    trends["单枪日服务费收入"][key]['values'].append(daily_revenue)

                    if key not in trends["单枪日充电量"]:
                        trends["单枪日充电量"][key] = {'months': [], 'values': []}
                    trends["单枪日充电量"][key]['months'].append(month_display)
                    trends["单枪日充电量"][key]['values'].append(daily_power)

    def _calculate_province_penetration_trends(self, monthly_penetration: List, months: List, trends: Dict):
        """计算省份渗透率趋势"""
        if not monthly_penetration or len(monthly_penetration) <= 1:
            return

        headers = monthly_penetration[0]

        # 灵活匹配列名
        cols = {
            'time': 0,
            'province': 2,
            'model': 4,
            'fast_charge': 5,
            'self_station': 6
        }
        print("\n=== 省份渗透率列索引匹配结果 ===")
        print(f"表头: {headers}")
        print(f"匹配的列索引: {cols}")
        print(f"快充电量列(应该是5): {cols.get('fast_charge')}")
        print(f"自建站充电量列(应该是6): {cols.get('self_station')}")

        valid_provinces = {
            # 省份
            '河北省', '山西省', '辽宁省', '吉林省', '黑龙江省', '江苏省', '浙江省',
            '安徽省', '福建省', '江西省', '山东省', '河南省', '湖北省', '湖南省',
            '广东省', '海南省', '四川省', '贵州省', '云南省', '陕西省', '甘肃省',
            '青海省', '台湾省',
            # 自治区
            '内蒙古自治区', '广西壮族自治区', '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区',
            # 直辖市
            '北京市', '天津市', '上海市', '重庆市',
            # 简称形式
            '河北', '山西', '辽宁', '吉林', '黑龙江', '江苏', '浙江', '安徽', '福建',
            '江西', '山东', '河南', '湖北', '湖南', '广东', '海南', '四川', '贵州',
            '云南', '陕西', '甘肃', '青海', '台湾', '内蒙古', '广西', '西藏', '宁夏',
            '新疆', '北京', '天津', '上海', '重庆', '香港', '澳门'
        }

        phev_models = ['L6', 'L7', 'L8', 'L9', 'ONE']
        bev_models = ['MEGA', 'i8', 'i6']

        # 按月份、省份、车型聚合数据
        for month in months:
            month_data = defaultdict(lambda: defaultdict(
                lambda: {'fast': 0, 'self': 0}))

            for row in monthly_penetration[1:]:
                # 确保行有足够的列
                if len(row) <= max(cols.values()):
                    continue

                # 解析月份
                row_month = self._parse_month(str(row[cols['time']]))
                if row_month != month:
                    continue

                province = str(row[cols['province']]).strip()  # ✅ 获取省份
                model = str(row[cols['model']])
                fast_charge = float(row[cols['fast_charge']] or 0)
                self_station = float(row[cols['self_station']] or 0)

                # 只处理在可用省份列表中的省份
                if province not in valid_provinces:
                    continue

                month_data[province][model]['fast'] += fast_charge
                month_data[province][model]['self'] += self_station

            # 计算渗透率
            month_display = f"{int(month.split('-')[1])}月"

            for province, models in month_data.items():
                # 1. 全部车型
                total_fast = sum(data['fast'] for data in models.values())
                total_self = sum(data['self'] for data in models.values())

                key = f"{province}_全部车型"
                if key not in trends["车主公充渗透率"]:
                    trends["车主公充渗透率"][key] = {'months': [], 'values': []}

                trends["车主公充渗透率"][key]['months'].append(month_display)
                if total_fast > 0:
                    trends["车主公充渗透率"][key]['values'].append(
                        round((total_self / total_fast) * 100, 1))
                else:
                    trends["车主公充渗透率"][key]['values'].append(0)

                # 2. ✅ 增程整体
                phev_fast = sum(models[m]['fast']
                                for m in models if m in phev_models)
                phev_self = sum(models[m]['self']
                                for m in models if m in phev_models)

                key = f"{province}_增程整体"
                if key not in trends["车主公充渗透率"]:
                    trends["车主公充渗透率"][key] = {'months': [], 'values': []}

                trends["车主公充渗透率"][key]['months'].append(month_display)
                if phev_fast > 0:
                    trends["车主公充渗透率"][key]['values'].append(
                        round((phev_self / phev_fast) * 100, 1))
                else:
                    trends["车主公充渗透率"][key]['values'].append(0)

                # 3. ✅ 纯电整体
                bev_fast = sum(models[m]['fast']
                               for m in models if m in bev_models)
                bev_self = sum(models[m]['self']
                               for m in models if m in bev_models)

                key = f"{province}_纯电整体"
                if key not in trends["车主公充渗透率"]:
                    trends["车主公充渗透率"][key] = {'months': [], 'values': []}

                trends["车主公充渗透率"][key]['months'].append(month_display)
                if bev_fast > 0:
                    trends["车主公充渗透率"][key]['values'].append(
                        round((bev_self / bev_fast) * 100, 1))
                else:
                    trends["车主公充渗透率"][key]['values'].append(0)

                # 4. 各车型
                for model, data in models.items():
                    key = f"{province}_{model}"
                    if key not in trends["车主公充渗透率"]:
                        trends["车主公充渗透率"][key] = {'months': [], 'values': []}

                    trends["车主公充渗透率"][key]['months'].append(month_display)
                    if data['fast'] > 0:
                        trends["车主公充渗透率"][key]['values'].append(
                            round((data['self'] / data['fast']) * 100, 1))
                    else:
                        trends["车主公充渗透率"][key]['values'].append(0)

    def _calculate_city_metrics(self) -> Dict:
        """计算城市指标（TOP20）"""
        monthly_data = self.raw_data.get('分月指标达成情况', [])

        # 先获取TOP20城市列表
        top20_cities = self._get_top20_cities(monthly_data)

        # 使用相同的计算逻辑，但限定在TOP20城市
        result = {
            "TOP20城市": top20_cities,
            "年度合计": self._calculate_city_yearly_summary(monthly_data, top20_cities),
            "上周表现": self._calculate_city_weekly_performance(top20_cities),
            "趋势数据": self._calculate_city_trends(monthly_data, top20_cities)
        }

        return result

    def _get_top20_cities(self, monthly_data: List) -> List:
        """返回固定的TOP20城市列表"""
        # 固定的TOP20城市列表
        return ['苏州', '无锡', '南京', '武汉', '合肥', '上海', '杭州',
                '宁波', '金华', '福州', '长沙', '厦门', '深圳', '广州',
                '成都', '西安', '北京', '济南', '青岛', '郑州', '重庆']

    def _calculate_city_yearly_summary(self, monthly_data: List, top20_cities: List) -> Dict:
        """计算城市年度合计指标"""
        headers = monthly_data[0]

        cols = {
            'time': headers.index('时间（月）'),
            'province': headers.index('省份'),
            'city': headers.index('城市'),
            'owners': headers.index('车主保有量'),
            'stations': headers.index('自建站数量'),
            'recent_users': headers.index('近30天充电车主数'),
            'recent_revenue': headers.index('近30天充电收入')
        }

        # 初始化所有TOP20城市的数据结构
        city_data = {}
        for city in top20_cities:
            city_data[city] = {
                'owners': 0,  # 将存储最大值
                'stations': 0,
                'recent_users': 0,
                'recent_revenue': 0.0
            }

        # 找到最新月份的数据，累加所有行
        for row in monthly_data[1:]:
            month = self._parse_month(str(row[cols['time']]))
            if month == self.latest_month:
                city = row[cols['city']]
                province = row[cols['province']]

                if city in top20_cities:
                    # 车主保有量取最大值
                    current_owners = int(row[cols['owners']] or 0)
                    city_data[city]['owners'] = max(
                        city_data[city]['owners'], current_owners)

                    # 其他指标继续累加（所有类型、所有标签）
                    city_data[city]['stations'] += int(
                        row[cols['stations']] or 0)
                    city_data[city]['recent_users'] += int(
                        row[cols['recent_users']] or 0)
                    city_data[city]['recent_revenue'] += float(
                        row[cols['recent_revenue']] or 0)

        # 计算总计
        total_owners = sum(d['owners'] for d in city_data.values())
        total_stations = sum(d['stations'] for d in city_data.values())
        total_users = sum(d['recent_users'] for d in city_data.values())
        total_revenue = sum(d['recent_revenue'] for d in city_data.values())

        # 计算占比
        city_summary = {}
        for city, data in city_data.items():
            city_summary[city] = {
                '车主保有量': data['owners'],
                '车主保有量%': round((data['owners'] / total_owners * 100) if total_owners > 0 else 0, 1),
                '累计自建场站数': data['stations'],
                '累计自建场站数%': round((data['stations'] / total_stations * 100) if total_stations > 0 else 0, 1),
                '最近30天充电车主数': data['recent_users'],
                '最近30天充电车主数%': round((data['recent_users'] / total_users * 100) if total_users > 0 else 0, 1),
                '最近30天充电收入': data['recent_revenue'],
                '最近30天充电收入%': round((data['recent_revenue'] / total_revenue * 100) if total_revenue > 0 else 0, 1)
            }

        return city_summary

    def _calculate_city_weekly_performance(self, top20_cities: List) -> Dict:
        """计算城市上周表现"""
        weekly_data = self.raw_data.get('上周城市省份核心指标', [])
        weekly_penetration = self.raw_data.get('理想车主公充渗透率周度', [])

        if not weekly_data or len(weekly_data) <= 1:
            return {}

        headers = weekly_data[0]
        cols = {
            'province': headers.index('省份'),
            'city': headers.index('城市'),
            'main_type': headers.index('场站主类型'),
            'sub_type': headers.index('场站子类型'),
            'daily_revenue': headers.index('单枪日服务费'),
            'daily_power': headers.index('单枪日电量')
        }

        # 初始化所有TOP20城市的数据结构
        city_stats = {}
        for city in top20_cities:
            city_stats[city] = {
                '单枪日收入_总计': 0,
                '单枪日收入_高速线上': 0,
                '单枪日收入_高速线下': 0,
                '单枪日收入_城市自建': 0,
                '单枪日收入_城市加盟': 0,
                '单枪日收入_城市门店': 0,
                '单枪日收入_旅游线路': 0,
                '单枪日电量_总计': 0,
                '单枪日电量_高速线上': 0,
                '单枪日电量_高速线下': 0,
                '单枪日电量_城市自建': 0,
                '单枪日电量_城市加盟': 0,
                '单枪日电量_城市门店': 0,
                '单枪日电量_旅游线路': 0,
                '渗透率_总计': 0,
                '渗透率_城市': 0,
                '渗透率_高速': 0
            }

        # 处理单枪日数据
        for row in weekly_data[1:]:
            province = row[cols['province']]
            city = row[cols['city']]
            main_type = row[cols['main_type']]
            sub_type = row[cols['sub_type']]

            # 只处理在TOP20列表中的城市
            if city in top20_cities:
                if main_type == '全部':
                    city_stats[city]['单枪日收入_总计'] = float(
                        row[cols['daily_revenue']] or 0)
                    city_stats[city]['单枪日电量_总计'] = float(
                        row[cols['daily_power']] or 0)
                elif sub_type == '线上':
                    city_stats[city]['单枪日收入_高速线上'] = float(
                        row[cols['daily_revenue']] or 0)
                    city_stats[city]['单枪日电量_高速线上'] = float(
                        row[cols['daily_power']] or 0)
                elif sub_type == '线下':
                    city_stats[city]['单枪日收入_高速线下'] = float(
                        row[cols['daily_revenue']] or 0)
                    city_stats[city]['单枪日电量_高速线下'] = float(
                        row[cols['daily_power']] or 0)
                elif sub_type in ['自建', '自营']:
                    city_stats[city]['单枪日收入_城市自建'] = float(
                        row[cols['daily_revenue']] or 0)
                    city_stats[city]['单枪日电量_城市自建'] = float(
                        row[cols['daily_power']] or 0)
                elif sub_type == '门店':
                    city_stats[city]['单枪日收入_城市门店'] = float(
                        row[cols['daily_revenue']] or 0)
                    city_stats[city]['单枪日电量_城市门店'] = float(
                        row[cols['daily_power']] or 0)
                elif sub_type == '加盟':
                    city_stats[city]['单枪日收入_城市加盟'] = float(
                        row[cols['daily_revenue']] or 0)
                    city_stats[city]['单枪日电量_城市加盟'] = float(
                        row[cols['daily_power']] or 0)
                elif sub_type == '旅游':
                    city_stats[city]['单枪日收入_旅游线路'] = float(
                        row[cols['daily_revenue']] or 0)
                    city_stats[city]['单枪日电量_旅游线路'] = float(
                        row[cols['daily_power']] or 0)

        # 计算渗透率
        penetration_rates = self._calculate_weekly_penetration_by_city(
            weekly_penetration, top20_cities)

        # 合并渗透率数据
        for city in city_stats:
            if city in penetration_rates:
                city_stats[city].update(penetration_rates[city])

        return city_stats

    def _calculate_weekly_penetration_by_city(self, weekly_data: List, top20_cities: List) -> Dict:
        """计算上周各城市的渗透率"""
        if not weekly_data or len(weekly_data) <= 1:
            return {}

        headers = weekly_data[0]
        cols = {
            'time': headers.index('时间（周）'),
            'province': headers.index('省份'),  # ✅ 新增
            'city': headers.index('城市'),
            'model': headers.index('车型'),     # ✅ 新增
            'fast_charge': headers.index('理想快充电量'),
            'self_station': headers.index('理想自建站充电量'),
            'highway_self': headers.index('理想高速自建站充电量'),
            'city_charge': headers.index('理想城市充电量')
        }

        # ✅ 定义重庆相关城市
        chongqing_cities = {'重庆市', '重庆', '重庆郊县', '重庆城区'}

        # 找到最新周
        latest_week = None
        for row in weekly_data[1:]:
            week = row[cols['time']]
            if not latest_week or week > latest_week:
                latest_week = week

        # 按城市聚合
        city_stats = defaultdict(lambda: {
            'fast_total': 0,
            'self_total': 0,
            'highway_total': 0,
            'city_total': 0
        })

        for row in weekly_data[1:]:
            if row[cols['time']] != latest_week:
                continue

            province = row[cols['province']]
            city = row[cols['city']]

            # ✅ 特殊处理重庆
            for top_city in top20_cities:
                if city == top_city:
                    city_stats[city]['fast_total'] += float(
                        row[cols['fast_charge']] or 0)
                    city_stats[city]['self_total'] += float(
                        row[cols['self_station']] or 0)
                    city_stats[city]['highway_total'] += float(
                        row[cols['highway_self']] or 0)
                    city_stats[city]['city_total'] += float(
                        row[cols['city_charge']] or 0)

        # 计算渗透率
        result = {}
        for city, stats in city_stats.items():
            result[city] = {
                '渗透率_总计': 0,
                '渗透率_城市': 0,
                '渗透率_高速': 0
            }

            if stats['fast_total'] > 0:
                result[city]['渗透率_总计'] = round(
                    (stats['self_total'] / stats['fast_total']) * 100, 1)
                result[city]['渗透率_城市'] = round(
                    (stats['city_total'] / stats['fast_total']) * 100, 1)
                result[city]['渗透率_高速'] = round(
                    (stats['highway_total'] / stats['fast_total']) * 100, 1)

        return result

    def _calculate_city_trends(self, monthly_data: List, top20_cities: List) -> Dict:
        """计算城市趋势数据"""
        # 获取额外数据源
        monthly_penetration = self.raw_data.get('理想车主公充渗透率月度', [])
        daily_metrics_monthly = self.raw_data.get('分月单枪日指标表', [])

        # 获取最近6个月
        all_months = set()
        headers = monthly_data[0]
        time_col = headers.index('时间（月）')

        for row in monthly_data[1:]:
            month = self._parse_month(str(row[time_col]))
            if month:
                all_months.add(month)

        sorted_months = sorted(all_months)

        # 各指标的趋势数据
        trends = {
            "充电总收入": {},
            "单枪日服务费收入": {},
            "单枪日充电量": {},
            "车主公充渗透率": {}
        }

        # 1. 计算充电收入趋势
        self._calculate_city_revenue_trends(
            monthly_data, sorted_months, trends, top20_cities)

        # 2. 计算单枪日指标趋势
        self._calculate_city_daily_metrics_trends(
            daily_metrics_monthly, sorted_months, trends, top20_cities)

        # 3. 计算渗透率趋势
        self._calculate_city_penetration_trends(
            monthly_penetration, sorted_months, trends, top20_cities)

        return trends

    def _calculate_city_revenue_trends(self, monthly_data: List, months: List,
                                       trends: Dict, top20_cities: List):
        """计算城市收入趋势"""
        headers = monthly_data[0]
        cols = {
            'time': headers.index('时间（月）'),
            'province': headers.index('省份'),
            'city': headers.index('城市'),
            'main_type': headers.index('场站主类型'),
            'revenue': headers.index('充电收入')
        }

        # 按月份和城市聚合数据
        for month in months:
            month_data = defaultdict(lambda: {
                '全部': {'revenue': 0},
                '高速': {'revenue': 0},
                '城市': {'revenue': 0}
            })

            for row in monthly_data[1:]:
                if self._parse_month(str(row[cols['time']])) == month:
                    province = row[cols['province']]
                    city = row[cols['city']]
                    main_type = row[cols['main_type']]
                    revenue = float(row[cols['revenue']] or 0)

                    # 只处理TOP20城市
                    if '重庆' in top20_cities:
                        # 重庆：省份='重庆市'的所有数据都聚合到重庆
                        if province == '重庆市':
                            # 全部
                            month_data['重庆']['全部']['revenue'] += revenue

                            # 按主类型
                            if main_type in ['高速', '城市']:
                                month_data['重庆'][main_type]['revenue'] += revenue

                    # ✅ 处理其他TOP20城市（排除已经在重庆处理过的数据）
                    if city in top20_cities and city != '重庆' and province != '重庆市':
                        # 全部
                        month_data[city]['全部']['revenue'] += revenue

                        # 按主类型
                        if main_type in ['高速', '城市']:
                            month_data[city][main_type]['revenue'] += revenue

            # 保存趋势数据
            month_display = f"{int(month.split('-')[1])}月"

            for city, data in month_data.items():
                for scene in ['全部', '高速', '城市']:
                    key = f"{city}_{scene}"
                    if key not in trends["充电总收入"]:
                        trends["充电总收入"][key] = {'months': [], 'values': []}
                    trends["充电总收入"][key]['months'].append(month_display)
                    trends["充电总收入"][key]['values'].append(
                        data[scene]['revenue'])

    def _calculate_city_daily_metrics_trends(self, daily_metrics_data: List,
                                             months: List, trends: Dict, top20_cities: List):
        """计算城市单枪日指标趋势"""
        if not daily_metrics_data or len(daily_metrics_data) <= 1:
            return

        headers = daily_metrics_data[0]
        cols = {
            'time': headers.index('时间（月）'),
            'province': headers.index('省份'),
            'city': headers.index('城市'),
            'duration': headers.index('上线时长标签'),
            'main_type': headers.index('场站主类型'),
            'sub_type': headers.index('场站子类型'),
            'daily_revenue': headers.index('单枪日服务费'),
            'daily_power': headers.index('单枪日电量')
        }

        # ✅ 定义重庆相关城市
        chongqing_cities = {'重庆市', '重庆', '重庆郊县', '重庆城区'}

        # 遍历每个月份
        for month in months:
            month_display = f"{int(month.split('-')[1])}月"

            for row in daily_metrics_data[1:]:
                if self._parse_month(str(row[cols['time']])) != month:
                    continue

                # 只处理上线时长=全部的数据
                if row[cols['duration']] != '全部':
                    continue

                province = row[cols['province']]
                city = row[cols['city']]
                main_type = row[cols['main_type']]
                sub_type = row[cols['sub_type']]

                # ✅ 新增：场站子类型必须是'整体'
                if sub_type not in ['全部', '整体']:
                    continue

                daily_revenue = float(row[cols['daily_revenue']] or 0)
                daily_power = float(row[cols['daily_power']] or 0)

                # 处理每个TOP20城市
                for top_city in top20_cities:
                    if top_city in chongqing_cities:
                        # ✅ 重庆特殊处理：省份='重庆'，城市='全部'
                        if province == '重庆市' and city == '全部':
                            scene_map = {
                                '全部': '全部',
                                '高速': '高速',
                                '城市': '城市'
                            }

                            if main_type in scene_map:
                                scene = scene_map[main_type]
                                key = f"{top_city}_{scene}"

                                # 存储趋势数据
                                if key not in trends["单枪日服务费收入"]:
                                    trends["单枪日服务费收入"][key] = {
                                        'months': [], 'values': []}
                                if month_display not in trends["单枪日服务费收入"][key]['months']:
                                    trends["单枪日服务费收入"][key]['months'].append(
                                        month_display)
                                    trends["单枪日服务费收入"][key]['values'].append(
                                        daily_revenue)

                                # 单枪日电量
                                if key not in trends["单枪日充电量"]:
                                    trends["单枪日充电量"][key] = {
                                        'months': [], 'values': []}
                                if month_display not in trends["单枪日充电量"][key]['months']:
                                    trends["单枪日充电量"][key]['months'].append(
                                        month_display)
                                    trends["单枪日充电量"][key]['values'].append(
                                        daily_power)
                    else:
                        # ✅ 其他城市正常处理：直接匹配城市名
                        if city == top_city:
                            scene_map = {
                                '全部': '全部',
                                '高速': '高速',
                                '城市': '城市'
                            }

                            if main_type in scene_map:
                                scene = scene_map[main_type]
                                key = f"{top_city}_{scene}"

                                # 单枪日服务费收入
                                if key not in trends["单枪日服务费收入"]:
                                    trends["单枪日服务费收入"][key] = {
                                        'months': [], 'values': []}
                                if month_display not in trends["单枪日服务费收入"][key]['months']:
                                    trends["单枪日服务费收入"][key]['months'].append(
                                        month_display)
                                    trends["单枪日服务费收入"][key]['values'].append(
                                        daily_revenue)

                                # 单枪日电量
                                if key not in trends["单枪日充电量"]:
                                    trends["单枪日充电量"][key] = {
                                        'months': [], 'values': []}
                                if month_display not in trends["单枪日充电量"][key]['months']:
                                    trends["单枪日充电量"][key]['months'].append(
                                        month_display)
                                    trends["单枪日充电量"][key]['values'].append(
                                        daily_power)

    def _calculate_city_penetration_trends(self, monthly_penetration: List, months: List,
                                           trends: Dict, top20_cities: List):
        """计算城市渗透率趋势"""
        if not monthly_penetration or len(monthly_penetration) <= 1:
            print("警告：理想车主公充渗透率月度表为空")
            return

        headers = monthly_penetration[0]
        print(f"渗透率表头: {headers}")

        # ✅ 修复：使用正确的列索引
        cols = {}
        for i, header in enumerate(headers):
            header_str = str(header)
            if '时间' in header_str or '月' in header_str:
                cols['time'] = i
            elif header_str == '城市':  # 精确匹配
                cols['city'] = i
            elif header_str == '省份':  # ✅ 新增省份列
                cols['province'] = i
            elif header_str == '车型':  # 精确匹配
                cols['model'] = i
            elif '理想快充电量' in header_str:  # 更精确的匹配
                cols['fast_charge'] = i
            elif '理想自建站充电量' in header_str:  # 更精确的匹配
                cols['self_station'] = i

        # 如果灵活匹配失败，使用固定索引（基于你提供的表头）
        if 'city' not in cols or cols['city'] != 3:
            print("使用固定列索引")
            cols = {
                'time': 0,          # 时间（月）
                'region': 1,        # 区域
                'province': 2,      # 省份
                'city': 3,          # 城市
                'model': 4,         # 车型
                'fast_charge': 5,   # 理想快充电量
                'self_station': 6,  # 理想自建站充电量
                'highway': 7,       # 理想高速自建站充电量
                'city_charge': 8    # 理想城市充电量
            }

        print(f"渗透率表列索引: {cols}")
        print(f"TOP20城市: {top20_cities}")
        print(f"要处理的月份: {months}")

        # 打印前几行数据进行验证
        if len(monthly_penetration) > 1:
            for i in range(1, min(4, len(monthly_penetration))):
                row = monthly_penetration[i]
                if len(row) > cols['city']:
                    print(
                        f"第{i}行数据 - 城市: {row[cols['city']]}, 车型: {row[cols['model']]}")

        # 按月份、城市、车型聚合数据
        processed_count = 0
        phev_models = {'L6', 'L7', 'L8', 'L9', 'ONE'}
        bev_models = {'MEGA', 'i8', 'i6'}

        for month in months:
            month_data = defaultdict(lambda: defaultdict(
                lambda: {'fast': 0, 'self': 0}))

            for row in monthly_penetration[1:]:
                # 确保行有足够的列
                if len(row) <= max(cols.values()):
                    continue

                # 解析月份
                row_month = self._parse_month(str(row[cols['time']]))
                if row_month != month:
                    continue

                province = str(row[cols['province']]).strip()  # ✅ 获取省份
                city = str(row[cols['city']]).strip()
                model = str(row[cols['model']])
                fast_charge = float(row[cols['fast_charge']] or 0)
                self_station = float(row[cols['self_station']] or 0)

                # ✅ 处理其他TOP20城市（排除已经在重庆处理过的数据）
                if city in top20_cities:
                    month_data[city][model]['fast'] += fast_charge
                    month_data[city][model]['self'] += self_station
                    processed_count += 1

                    # 调试信息
                    if processed_count <= 5:
                        print(
                            f"处理数据: 月份={row_month}, 城市={city}, 车型={model}, 快充={fast_charge}, 自建={self_station}")

            # 计算渗透率并添加到趋势数据
            month_display = f"{int(month.split('-')[1])}月"

            # 处理每个城市的数据
            for city in month_data:
                total_fast = sum(data['fast']
                                 for data in month_data[city].values())
                total_self = sum(data['self']
                                 for data in month_data[city].values())

                key = f"{city}_全部车型"
                if key not in trends["车主公充渗透率"]:
                    trends["车主公充渗透率"][key] = {'months': [], 'values': []}

                trends["车主公充渗透率"][key]['months'].append(month_display)
                if total_fast > 0:
                    trends["车主公充渗透率"][key]['values'].append(
                        round((total_self / total_fast) * 100, 1))
                else:
                    trends["车主公充渗透率"][key]['values'].append(0)
                # 增程整体
                phev_fast = sum(month_data[city][m]['fast']
                                for m in month_data[city] if m in phev_models)
                phev_self = sum(month_data[city][m]['self']
                                for m in month_data[city] if m in phev_models)

                key = f"{city}_增程整体"
                if key not in trends["车主公充渗透率"]:
                    trends["车主公充渗透率"][key] = {'months': [], 'values': []}

                trends["车主公充渗透率"][key]['months'].append(month_display)
                if phev_fast > 0:
                    trends["车主公充渗透率"][key]['values'].append(
                        round((phev_self / phev_fast) * 100, 1))
                else:
                    trends["车主公充渗透率"][key]['values'].append(0)

                # 3. ✅ 纯电整体
                bev_fast = sum(month_data[city][m]['fast']
                               for m in month_data[city] if m in bev_models)
                bev_self = sum(month_data[city][m]['self']
                               for m in month_data[city] if m in bev_models)

                key = f"{city}_纯电整体"
                if key not in trends["车主公充渗透率"]:
                    trends["车主公充渗透率"][key] = {'months': [], 'values': []}

                trends["车主公充渗透率"][key]['months'].append(month_display)
                if bev_fast > 0:
                    trends["车主公充渗透率"][key]['values'].append(
                        round((bev_self / bev_fast) * 100, 1))
                else:
                    trends["车主公充渗透率"][key]['values'].append(0)
                # 各车型
                for model, data in month_data[city].items():
                    key = f"{city}_{model}"
                    if key not in trends["车主公充渗透率"]:
                        trends["车主公充渗透率"][key] = {'months': [], 'values': []}

                    trends["车主公充渗透率"][key]['months'].append(month_display)
                    if data['fast'] > 0:
                        trends["车主公充渗透率"][key]['values'].append(
                            round((data['self'] / data['fast']) * 100, 1))
                    else:
                        trends["车主公充渗透率"][key]['values'].append(0)

        print(f"处理了 {processed_count} 条城市渗透率数据")
        print(f"生成的趋势数据条数: {len(trends.get('车主公充渗透率', {}))}")

        # 打印一些样例数据用于验证
        if trends.get('车主公充渗透率'):
            sample_keys = list(trends['车主公充渗透率'].keys())[:3]
            for key in sample_keys:
                print(f"样例数据 - {key}: {trends['车主公充渗透率'][key]}")

            # 打印一些样例数据用于验证
            if trends.get('车主公充渗透率'):
                sample_keys = list(trends['车主公充渗透率'].keys())[:3]
                for key in sample_keys:
                    print(f"样例数据 - {key}: {trends['车主公充渗透率'][key]}")

    def calculate_all_metrics(self) -> Dict:
        """计算所有指标"""
        monthly_data = self.raw_data.get('分月指标达成情况', [])
        core_kpi = self._calculate_core_kpi()  # 这里会调用_process_daily_metrics_table

        return {
            "核心KPI": self._calculate_core_kpi(),
            "趋势数据": self._calculate_trends(),
            "用户分层": self._calculate_user_layer(),
            "区域数据": self._calculate_region_data(),
            "目标": self._process_target_data(),
            "按时长统计": self._calculate_duration_stats(monthly_data),
            "省份指标": self._calculate_province_metrics(),  # ✅ 新增这行
            "城市指标": self._calculate_city_metrics(),
            "最新月份": self.latest_month,
            "数据日期": self.current_date.strftime("%Y-%m-%d"),
            "时间进度": self._calculate_time_progress(),
            "目标周期": self.target_period,  # 新增
            "周期名称": self.period_name,     # 新增
            "YTD天数": self.ytd_days
        }
        # 逐个计算，捕获具体错误
        print("\n=== 开始计算各项指标 ===")

        try:
            print("1. 计算核心KPI...")
            result["核心KPI"] = self._calculate_core_kpi()
            print("   ✓ 核心KPI计算成功")
        except Exception as e:
            print(f"   ✗ 核心KPI计算失败: {e}")
            import traceback
            traceback.print_exc()
            result["核心KPI"] = {}

        try:
            print("2. 计算趋势数据...")
            result["趋势数据"] = self._calculate_trends()
            print("   ✓ 趋势数据计算成功")
        except Exception as e:
            print(f"   ✗ 趋势数据计算失败: {e}")
            import traceback
            traceback.print_exc()
            result["趋势数据"] = {}

        try:
            print("3. 计算用户分层...")
            result["用户分层"] = self._calculate_user_layer()
            print("   ✓ 用户分层计算成功")
        except Exception as e:
            print(f"   ✗ 用户分层计算失败: {e}")
            import traceback
            traceback.print_exc()
            result["用户分层"] = {}

        try:
            print("4. 计算区域数据...")
            result["区域数据"] = self._calculate_region_data()
            print("   ✓ 区域数据计算成功")
        except Exception as e:
            print(f"   ✗ 区域数据计算失败: {e}")
            import traceback
            traceback.print_exc()
            result["区域数据"] = {}

        try:
            print("5. 处理目标数据...")
            result["目标"] = self._process_target_data()
            print("   ✓ 目标数据处理成功")
        except Exception as e:
            print(f"   ✗ 目标数据处理失败: {e}")
            import traceback
            traceback.print_exc()
            result["目标"] = {}

        try:
            print("6. 计算按时长统计...")
            result["按时长统计"] = self._calculate_duration_stats(monthly_data)
            print("   ✓ 按时长统计计算成功")
        except Exception as e:
            print(f"   ✗ 按时长统计计算失败: {e}")
            import traceback
            traceback.print_exc()
            result["按时长统计"] = {}

        try:
            print("7. 计算省份指标...")
            result["省份指标"] = self._calculate_province_metrics()
            print("   ✓ 省份指标计算成功")
        except Exception as e:
            print(f"   ✗ 省份指标计算失败: {e}")
            import traceback
            traceback.print_exc()
            result["省份指标"] = {}

        try:
            print("8. 计算城市指标...")
            result["城市指标"] = self._calculate_city_metrics()
            print("   ✓ 城市指标计算成功")
        except Exception as e:
            print(f"   ✗ 城市指标计算失败: {e}")
            import traceback
            traceback.print_exc()
            result["城市指标"] = {}

        print("=== 计算完成 ===\n")

        return result

# 创建简单的内存缓存

class SimpleCache:
    def __init__(self):
        self._cache = {}
        self._timestamps = {}

    def get(self, key: str) -> Optional[any]:
        if key in self._cache:
            if key in self._timestamps:
                if datetime.utcnow() > self._timestamps[key]:
                    del self._cache[key]
                    del self._timestamps[key]
                    return None
            return self._cache[key]
        return None

    def set(self, key: str, value: any, expire_minutes: int = 60):
        self._cache[key] = value
        if expire_minutes:
            self._timestamps[key] = datetime.utcnow(
            ) + timedelta(minutes=expire_minutes)

    def clear_pattern(self, pattern: str):
        keys_to_delete = [
            k for k in self._cache.keys() if k.startswith(pattern)]
        for key in keys_to_delete:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)


try:
    from cache import cache
except ImportError:
    cache = SimpleCache()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ✨自动注册所有数据处理器
    DataRegistry.auto_register_handlers()

    yield

    # 清理资源
    await async_engine.dispose()

app = FastAPI(title="星驰平台", lifespan=lifespan)

# 全局异常处理器


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global exception: {type(exc).__name__}: {str(exc)}")
    logger.error(f"Request URL: {request.url}")
    logger.error(f"Traceback:\n{traceback.format_exc()}")

    return JSONResponse(
        status_code=500,
        content={
            "detail": str(exc),
            "type": type(exc).__name__,
            "traceback": traceback.format_exc() if os.getenv("DEBUG") else None
        }
    )

# CORS配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 静态文件
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
# 专门为pxxdash挂载
if os.path.exists("src/dashboard/pxxdash"):
    app.mount("/pxxdash/files",
              StaticFiles(directory="src/dashboard/pxxdash"), name="pxxdash_files")


# 路由管理

# HTMLmanage路由
app.include_router(html_manage_router, tags=["HTML文件管理"])


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/travel", response_class=HTMLResponse)
async def travel_system():
    if os.path.exists("static/index.html"):
        with open("static/index.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return html_content
    else:
        return HTMLResponse(content="<h1>Travel System Not Found</h1>", status_code=404)

# 测试数据库连接


@app.get("/api/test-db")
async def test_database(db: AsyncSession = Depends(get_db)):
    try:
        from sqlalchemy import text
        result = await db.execute(text("SELECT 1"))
        return {"status": "success", "result": result.scalar()}
    except Exception as e:
        logger.error(f"Database test failed: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "failed",
                "error": str(e),
                "type": type(e).__name__
            }
        )


async def check_editor_mode(x_user_mode: Optional[str] = Header(None)):
    if x_user_mode != "editor":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="需要编辑模式才能执行此操作"
        )
    return True


@app.get("/api/routes", response_model=List[schemas.Route])
async def get_routes(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    try:
        logger.info(f"Getting routes: skip={skip}, limit={limit}")

        # 尝试从缓存获取
        cache_key = f"routes_{skip}_{limit}"
        cached_routes = cache.get(cache_key)
        if cached_routes:
            return cached_routes

        routes = await crud.get_routes(db, skip=skip, limit=limit)
        logger.info(f"Found {len(routes)} routes")

        # routes 已经是字典列表，直接缓存和返回
        cache.set(cache_key, routes, expire_minutes=5)
        return routes
    except Exception as e:
        logger.error(f"Error in get_routes: {e}")
        logger.error(traceback.format_exc())
        raise


@app.get("/api/routes/{route_id}", response_model=schemas.Route)
async def get_route(
    route_id: int,
    db: AsyncSession = Depends(get_db)
):
    try:
        route = await crud.get_route(db, route_id=route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="Route not found")
        return route
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_route: {e}")
        logger.error(traceback.format_exc())
        raise


@app.post("/api/routes", response_model=schemas.Route)
async def create_route(
    route: schemas.RouteCreate,
    db: AsyncSession = Depends(get_db),
    _: bool = Depends(check_editor_mode)
):
    try:
        logger.info(f"Creating route: {route.name}")
        logger.info(
            f"Route data: waypoints={len(route.waypoints)}, attractions={len(route.attractions)}, stations={len(route.stations) if route.stations else 0}")

        cache.clear_pattern("routes_")
        result = await crud.create_route(db=db, route=route)
        logger.info(f"Route created successfully")
        return result
    except Exception as e:
        logger.error(f"Error in create_route: {type(e).__name__}: {str(e)}")
        logger.error(traceback.format_exc())
        raise


@app.put("/api/routes/{route_id}", response_model=schemas.Route)
async def update_route(
    route_id: int,
    route: schemas.RouteUpdate,
    db: AsyncSession = Depends(get_db),
    _: bool = Depends(check_editor_mode)
):
    try:
        cache.clear_pattern("routes_")
        db_route = await crud.update_route(db=db, route_id=route_id, route=route)
        if db_route is None:
            raise HTTPException(status_code=404, detail="Route not found")
        return db_route
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in update_route: {e}")
        logger.error(traceback.format_exc())
        raise


@app.delete("/api/routes/{route_id}")
async def delete_route(
    route_id: int,
    db: AsyncSession = Depends(get_db),
    _: bool = Depends(check_editor_mode)
):
    try:
        cache.clear_pattern("routes_")
        success = await crud.delete_route(db=db, route_id=route_id)
        if not success:
            raise HTTPException(status_code=404, detail="Route not found")
        return {"message": "Route deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in delete_route: {e}")
        logger.error(traceback.format_exc())
        raise


@app.get("/api/map-config", response_model=schemas.MapConfig)
async def get_map_config(
    db: AsyncSession = Depends(get_db)
):
    try:
        logger.info("Getting map config")
        config = await crud.get_map_config(db)
        if config is None:
            return schemas.MapConfig(
                id=0,
                amap_key="",
                amap_security_code="",
                updated_at=datetime.utcnow()
            )
        return config
    except Exception as e:
        logger.error(f"Error in get_map_config: {e}")
        logger.error(traceback.format_exc())
        raise


@app.put("/api/map-config", response_model=schemas.MapConfig)
async def update_map_config(
    config: schemas.MapConfigUpdate,
    db: AsyncSession = Depends(get_db),
    _: bool = Depends(check_editor_mode)
):
    try:
        return await crud.update_map_config(db=db, config=config)
    except Exception as e:
        logger.error(f"Error in update_map_config: {e}")
        logger.error(traceback.format_exc())
        raise

# ==========区域看板路由===========


@app.get("/RegionUp", response_class=HTMLResponse)
async def region_upload_page():
    """充电站数据上传页面"""
    try:
        with open("static/RegionUp.html", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="RegionUp.html文件未找到")

# 各大区页面路由


@app.get("/RegionEast1", response_class=HTMLResponse)
async def region_east1_page():
    """东一区页面"""
    try:
        with open("static/RegionEast1.html", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="RegionEast1.html文件未找到")


@app.get("/RegionEast2", response_class=HTMLResponse)
async def region_east2_page():
    """东二区页面"""
    try:
        with open("static/RegionEast2.html", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="RegionEast2.html文件未找到")


@app.get("/RegionNorth", response_class=HTMLResponse)
async def region_north_page():
    """北区页面"""
    try:
        with open("static/RegionNorth.html", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="RegionNorth.html文件未找到")


@app.get("/RegionSouth", response_class=HTMLResponse)
async def region_south_page():
    """南区页面"""
    try:
        with open("static/RegionSouth.html", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="RegionSouth.html文件未找到")


@app.get("/RegionWest", response_class=HTMLResponse)
async def region_west_page():
    """西区页面"""
    try:
        with open("static/RegionWest.html", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="RegionWest.html文件未找到")


@app.get("/RegionCenter", response_class=HTMLResponse)
async def region_center_page():
    """中区页面"""
    try:
        with open("static/RegionCenter.html", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="RegionCenter.html文件未找到")

# ========== 充电站数据上传API ==========


@app.post("/upload")
async def upload_charging_data(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    """上传充电站Excel数据文件 - 异步处理版本"""
    logger.info(f"接收到文件: {file.filename}")

    # 1. 基础验证
    if not file.filename.endswith('.xlsx'):
        logger.error("文件格式错误：不是.xlsx文件")
        raise HTTPException(status_code=400, detail="只支持.xlsx格式的文件")

    # 2. 读取文件内容到内存
    try:
        contents = await file.read()
        file_size = len(contents)
        logger.info(f"文件大小: {file_size} bytes")

        # 检查文件大小
        if file_size > 100 * 1024 * 1024:  # 100MB
            raise HTTPException(status_code=400, detail="文件大小不能超过100MB")

        # 生成任务ID
        task_id = datetime.now().strftime("%Y%m%d%H%M%S") + \
            "_" + str(uuid.uuid4())[:8]

        # 初始化任务状态
        upload_tasks[task_id] = {
            "status": "processing",
            "progress": 0,
            "message": "开始处理文件",
            "filename": file.filename,
            "start_time": datetime.now().isoformat(),
            "result": None,
            "error": None
        }

        # 3. 添加后台任务处理Excel
        background_tasks.add_task(
            process_excel_async,
            task_id,
            contents,
            file.filename
        )

        # 4. 立即返回任务ID
        return {
            "message": "文件已接收，正在后台处理",
            "task_id": task_id,
            "filename": file.filename,
            "file_size": file_size
        }

    except Exception as e:
        logger.error(f"接收文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"接收文件失败: {str(e)}")


async def process_excel_async(task_id: str, contents: bytes, filename: str):
    """异步处理Excel文件"""
    try:
        logger.info(f"开始异步处理任务: {task_id}")

        # 更新任务状态
        upload_tasks[task_id]["status"] = "processing"
        upload_tasks[task_id]["progress"] = 10
        upload_tasks[task_id]["message"] = "正在读取Excel文件..."

        # 1. 读取Excel sheets
        try:
            upload_tasks[task_id]["message"] = "读取单站数据..."
            upload_tasks[task_id]["progress"] = 20
            station_df = pd.read_excel(io.BytesIO(contents), sheet_name="单站数据")
            logger.info(f"单站数据: {len(station_df)} 行")

            upload_tasks[task_id]["message"] = "读取公充渗透率数据..."
            upload_tasks[task_id]["progress"] = 30
            penetration_df = pd.read_excel(
                io.BytesIO(contents), sheet_name="公充渗透率")
            logger.info(f"公充渗透率数据: {len(penetration_df)} 行")

            upload_tasks[task_id]["message"] = "读取目标数据..."
            upload_tasks[task_id]["progress"] = 40
            target_df = pd.read_excel(io.BytesIO(contents), sheet_name="目标")
            logger.info(f"目标数据: {len(target_df)} 行")

        except Exception as e:
            raise Exception(f"读取Excel失败: {str(e)}")

        # 2. 数据预处理
        upload_tasks[task_id]["message"] = "预处理数据..."
        upload_tasks[task_id]["progress"] = 50

        station_df = preprocess_station_data(station_df)
        penetration_df = preprocess_penetration_data(penetration_df)
        target_df = preprocess_target_data(target_df)

        # 3. 创建新版本
        upload_tasks[task_id]["message"] = "创建数据版本..."
        upload_tasks[task_id]["progress"] = 60

        version_id = await db_manager.create_new_version(
            description=f"上传于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info(f"新版本ID: {version_id}")

        # 4. 批量插入数据
        upload_tasks[task_id]["message"] = "保存单站数据..."
        upload_tasks[task_id]["progress"] = 70
        await db_manager.insert_station_daily_batch(version_id, station_df)

        upload_tasks[task_id]["message"] = "保存渗透率数据..."
        upload_tasks[task_id]["progress"] = 80
        await db_manager.insert_penetration_daily_batch(version_id, penetration_df)

        upload_tasks[task_id]["message"] = "保存目标数据..."
        upload_tasks[task_id]["progress"] = 85
        await db_manager.insert_target_batch(version_id, target_df)

        # 5. 生成预聚合数据
        upload_tasks[task_id]["message"] = "生成聚合数据..."
        upload_tasks[task_id]["progress"] = 90
        await aggregation_service.save_aggregated_data(version_id, station_df, penetration_df)

        # 6. 更新缓存
        upload_tasks[task_id]["message"] = "更新缓存..."
        upload_tasks[task_id]["progress"] = 95
        await aggregation_service.update_filter_cache(version_id, station_df, penetration_df)

        # 清空内存缓存
        data_service.clear_cache()

        # 7. 完成
        upload_tasks[task_id]["status"] = "completed"
        upload_tasks[task_id]["progress"] = 100
        upload_tasks[task_id]["message"] = "处理完成"
        upload_tasks[task_id]["end_time"] = datetime.now().isoformat()
        upload_tasks[task_id]["result"] = {
            "version_id": version_id,
            "station_rows": len(station_df),
            "penetration_rows": len(penetration_df),
            "target_rows": len(target_df),
            "regions": list(station_df['区域'].unique()) if '区域' in station_df.columns else []
        }

        logger.info(f"任务 {task_id} 处理完成")

    except Exception as e:
        logger.error(f"处理任务 {task_id} 时出错: {str(e)}")
        upload_tasks[task_id]["status"] = "failed"
        upload_tasks[task_id]["progress"] = 0
        upload_tasks[task_id]["message"] = "处理失败"
        upload_tasks[task_id]["error"] = str(e)
        upload_tasks[task_id]["end_time"] = datetime.now().isoformat()

# 添加查询任务状态的接口


@app.get("/upload/status/{task_id}")
async def get_upload_status(task_id: str):
    """查询上传任务状态"""
    if task_id not in upload_tasks:
        raise HTTPException(status_code=404, detail="任务不存在")

    task_status = upload_tasks[task_id].copy()

    # 如果任务已完成或失败，可以考虑清理
    if task_status["status"] in ["completed", "failed"]:
        # 保留一段时间后清理（比如5分钟）
        if "end_time" in task_status:
            end_time = datetime.fromisoformat(task_status["end_time"])
            if datetime.now() - end_time > timedelta(minutes=5):
                del upload_tasks[task_id]

    return task_status

# 清理过期任务的定时任务（可选）


async def cleanup_old_tasks():
    """清理过期的任务记录"""
    while True:
        await asyncio.sleep(300)  # 每5分钟清理一次

        current_time = datetime.now()
        tasks_to_remove = []

        for task_id, task_info in upload_tasks.items():
            if task_info["status"] in ["completed", "failed"]:
                if "end_time" in task_info:
                    end_time = datetime.fromisoformat(task_info["end_time"])
                    if current_time - end_time > timedelta(minutes=10):
                        tasks_to_remove.append(task_id)

        for task_id in tasks_to_remove:
            del upload_tasks[task_id]
            logger.info(f"清理过期任务: {task_id}")
# ========== 充电站数据查询API ==========
# 修改 get_region_data 函数（约第3760行）


@app.get("/data/{region}")
async def get_region_data(region: str):
    """获取特定大区的初始数据"""
    # 获取最新版本ID
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")
    data_upload_time = await db_manager.get_version_upload_time(version_id)
    # 大区映射
    region_map = {
        "east1": "东一区",
        "east2": "东二区",
        "north": "北区",
        "south": "南区",
        "west": "西区",
        "center": "中区"
    }

    if region not in region_map:
        raise HTTPException(status_code=400, detail="无效的大区")

    region_name = region_map[region]

    try:
        # ========== 新增：获取当前季度 ==========
        current_date = datetime.now()
        current_year = current_date.year
        current_quarter = (current_date.month - 1) // 3 + 1
        quarter_key = f"{current_year}-Q{current_quarter}"

        # ========== 修改：获取季度聚合数据用于计算当前指标 ==========
        # 获取季度数据
        station_quarterly_sql = f"""
            SELECT * FROM charging_aggregated_station_data
            WHERE version_id = {version_id} 
            AND region = '{region_name}' 
            AND aggregation_type = 'quarterly'
            AND period_key = '{quarter_key}'
        """
        station_quarterly = await db_manager.execute_query_df(station_quarterly_sql, {})

        penetration_quarterly_sql = f"""
            SELECT * FROM charging_aggregated_penetration_data
            WHERE version_id = {version_id}
            AND region = '{region_name}'
            AND aggregation_type = 'quarterly'
            AND period_key = '{quarter_key}'
        """
        penetration_quarterly = await db_manager.execute_query_df(penetration_quarterly_sql, {})

        # 转换列名为Excel格式
        station_quarterly = convert_db_columns_to_excel_format(
            station_quarterly, 'station')
        penetration_quarterly = convert_db_columns_to_excel_format(
            penetration_quarterly, 'penetration')

        # ========== 修改：使用季度数据计算当前指标 ==========
        metrics = calculate_current_metrics(
            station_quarterly, penetration_quarterly)

        # ========== 以下保持原有逻辑（用于其他数据） ==========
        # 1. 获取月度数据（用于趋势图等）
        station_df = await data_service.get_region_data(region_name, 'monthly', version_id)
        penetration_df = await data_service.get_region_data(region_name, 'penetration_monthly', version_id)

        # 2. 获取目标数据
        target_df = await data_service.get_region_data(region_name, 'target', version_id)

        # 3. 转换列名为Excel格式
        station_df = convert_db_columns_to_excel_format(station_df, 'station')
        penetration_df = convert_db_columns_to_excel_format(
            penetration_df, 'penetration')
        target_df = convert_db_columns_to_excel_format(target_df, 'target')

        # 5. 获取全国数据和目标
        national_data = await calculate_national_metrics_with_targets(version_id)

        # 6. 生成所有大区卡片数据（需要修改）
        region_cards = await generate_all_region_cards_data(region, version_id)

        # 7. 生成月度和周度数据（保持原样）
        monthly_data = await generate_monthly_data_async(region_name, None, version_id)
        weekly_data = await generate_weekly_data_async(region_name, None, version_id)

        # 8. 生成表格数据（使用月度数据）
        table_data = generate_table_data(station_df, penetration_df)

        # 9. 获取省份城市数据（使用月度数据）
        province_city_data = get_province_city_data(station_df)

        # 10. 获取筛选选项（使用月度数据）
        filter_options = get_filter_options(station_df, penetration_df)

        # 11. 生成拆解数据（使用季度数据）
        breakdown_data = generate_breakdown_data(
            station_quarterly, penetration_quarterly)

        # 12. 生成贡献度数据（使用季度数据）
        contribution_data = generate_contribution_data(
            station_quarterly, penetration_quarterly, None)

        # 13. 生成组件数据（使用季度数据）
        component_data = generate_component_data(
            station_quarterly, penetration_quarterly, None, region_name)

        # 构建完整响应
        result = {
            "region": region_name,
            "lastUpdate": data_upload_time.isoformat() if data_upload_time else datetime.now().isoformat(),
            "metrics": metrics,  # 现在是季度数据
            "nationalData": national_data,
            "regionCards": region_cards,
            "monthlyData": monthly_data,
            "weeklyData": weekly_data,
            "tableData": table_data,
            "provinceCityData": province_city_data,
            "filterOptions": filter_options,
            "breakdownData": breakdown_data,
            "contributionData": contribution_data,
            "componentData": component_data
        }

        return JSONResponse(content=convert_decimals(result))

    except Exception as e:
        print(f"计算指标时出错: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"计算指标失败: {str(e)}")


@app.post("/data/{region}/filter")
async def get_filtered_data(region: str, filters: FilterRequest):
    """根据筛选条件获取数据"""
    # 获取最新版本ID
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")
    data_upload_time = await db_manager.get_version_upload_time(version_id)
    region_map = {
        "east1": "东一区",
        "east2": "东二区",
        "north": "北区",
        "south": "南区",
        "west": "西区",
        "center": "中区"
    }

    if region not in region_map:
        raise HTTPException(status_code=400, detail="无效的大区")

    region_name = region_map[region]

    try:
        # 获取数据
        station_df = await data_service.get_region_data(region_name, 'monthly', version_id)
        penetration_df = await data_service.get_region_data(region_name, 'penetration_monthly', version_id)

        # 应用筛选
        filter_dict = filters.model_dump()
        if filter_dict:
            station_df = data_service.apply_filters(
                station_df, filter_dict, 'station')
            penetration_df = data_service.apply_filters(
                penetration_df, filter_dict, 'penetration')

        # 转换列名
        station_df = convert_db_columns_to_excel_format(station_df, 'station')
        penetration_df = convert_db_columns_to_excel_format(
            penetration_df, 'penetration')

        # 计算筛选后的数据
        metrics = calculate_current_metrics(station_df, penetration_df)
        monthly_data = await generate_monthly_data_async(region_name, filter_dict, version_id)
        weekly_data = await generate_weekly_data_async(region_name, filter_dict, version_id)
        table_data = generate_table_data(station_df, penetration_df)
        breakdown_data = generate_breakdown_data(station_df, penetration_df)
        contribution_data = generate_contribution_data(
            station_df, penetration_df, filter_dict)

        result = {
            "region": region_name,
            "lastUpdate": data_upload_time.isoformat() if data_upload_time else datetime.now().isoformat(),
            "metrics": metrics,
            "monthlyData": monthly_data,
            "weeklyData": weekly_data,
            "tableData": table_data,
            "breakdownData": breakdown_data,
            "contributionData": contribution_data
        }

        return JSONResponse(content=convert_decimals(result))

    except Exception as e:
        print(f"计算筛选数据时出错: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"计算筛选数据失败: {str(e)}")
# ========== 月度详细数据 ==========


@app.post("/data/{region}/month/{month}")
async def get_month_detail_data(
    region: str,
    month: str,
    filters: FilterRequest
):
    """获取特定月份的详细数据"""
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")

    region_map = {
        "east1": "东一区",
        "east2": "东二区",
        "north": "北区",
        "south": "南区",
        "west": "西区",
        "center": "中区",
        "national": "全国"
    }

    if region not in region_map:
        raise HTTPException(status_code=400, detail="无效的大区")

    region_name = region_map[region]

    try:
        # 获取数据 - 不需要db参数
        if region == "national":
            # 获取全国数据（所有区域的聚合）
            all_regions = ["北区", "中区", "东一区", "东二区", "西区", "南区"]
            station_list = []
            penetration_list = []

            for r in all_regions:
                r_station = await data_service.get_region_data(r, 'monthly', version_id)
                r_pen = await data_service.get_region_data(r, 'penetration_monthly', version_id)
                if not r_station.empty:
                    station_list.append(r_station)
                if not r_pen.empty:
                    penetration_list.append(r_pen)

            station_df = pd.concat(
                station_list, ignore_index=True) if station_list else pd.DataFrame()
            penetration_df = pd.concat(
                penetration_list, ignore_index=True) if penetration_list else pd.DataFrame()
        else:
            station_df = await data_service.get_region_data(region_name, 'monthly', version_id)
            penetration_df = await data_service.get_region_data(region_name, 'penetration_monthly', version_id)

        # 应用筛选
        filter_dict = filters.model_dump()
        if filter_dict:
            if region == "national":
                # 全国数据不筛选省份城市
                filter_dict['provinces'] = []
                filter_dict['cities'] = []
            station_df = data_service.apply_filters(
                station_df, filter_dict, 'station')
            penetration_df = data_service.apply_filters(
                penetration_df, filter_dict, 'penetration')

        # 转换列名
        station_df = convert_db_columns_to_excel_format(station_df, 'station')
        penetration_df = convert_db_columns_to_excel_format(
            penetration_df, 'penetration')

        # 过滤月份
        if "Q" in month and len(month.split("-")) == 2:  # 季度数据
            year, quarter = month.split("-")
            quarter_num = int(quarter.replace("Q", ""))
            start_month = (quarter_num - 1) * 3 + 1
            end_month = quarter_num * 3

            station_df = station_df[
                (station_df["统计日期"].dt.year == int(year)) &
                (station_df["统计日期"].dt.month >= start_month) &
                (station_df["统计日期"].dt.month <= end_month)
            ]
            penetration_df = penetration_df[
                (penetration_df["统计日期"].dt.year == int(year)) &
                (penetration_df["统计日期"].dt.month >= start_month) &
                (penetration_df["统计日期"].dt.month <= end_month)
            ]
        else:  # 月度数据
            station_df = station_df[station_df["统计日期"].dt.strftime(
                '%Y-%m') == month]
            penetration_df = penetration_df[penetration_df["统计日期"].dt.strftime(
                '%Y-%m') == month]

        result = {
            "region": region_name,
            "month": month,
            "metrics": calculate_metrics_for_period(station_df, penetration_df),
            "breakdownData": calculate_breakdown_for_period(station_df, penetration_df),
            "contributionData": calculate_contribution_for_period(station_df, penetration_df, filter_dict)
        }

        return JSONResponse(content=convert_decimals(result))
    except Exception as e:
        logger.error(f"计算月度详细数据时出错: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"计算月度详细数据失败: {str(e)}")


@app.post("/data/{region}/week/{week}")
async def get_week_detail_data(
    region: str,
    week: str,
    filters: FilterRequest
):
    """获取特定周的详细数据"""
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")

    region_map = {
        "east1": "东一区",
        "east2": "东二区",
        "north": "北区",
        "south": "南区",
        "west": "西区",
        "center": "中区",
        "national": "全国"
    }

    if region not in region_map:
        raise HTTPException(status_code=400, detail="无效的大区")

    region_name = region_map[region]

    try:
        # 从数据服务获取周度聚合数据
        if region == "national":
            # 获取全国数据
            all_regions = ["北区", "中区", "东一区", "东二区", "西区", "南区"]
            station_list = []
            penetration_list = []

            for r in all_regions:
                r_station = await data_service.get_region_data(r, 'weekly', version_id)
                r_pen = await data_service.get_region_data(r, 'penetration_weekly', version_id)
                if not r_station.empty:
                    station_list.append(r_station)
                if not r_pen.empty:
                    penetration_list.append(r_pen)

            weekly_station = pd.concat(
                station_list, ignore_index=True) if station_list else pd.DataFrame()
            weekly_penetration = pd.concat(
                penetration_list, ignore_index=True) if penetration_list else pd.DataFrame()
        else:
            weekly_station = await data_service.get_region_data(region_name, 'weekly', version_id)
            weekly_penetration = await data_service.get_region_data(region_name, 'penetration_weekly', version_id)

        # 应用筛选
        filter_dict = filters.model_dump()
        if filter_dict:
            if region == "national":
                filter_dict['provinces'] = []
                filter_dict['cities'] = []
            weekly_station = data_service.apply_filters(
                weekly_station, filter_dict, 'station')
            weekly_penetration = data_service.apply_filters(
                weekly_penetration, filter_dict, 'penetration')

        # 转换列名
        weekly_station = convert_db_columns_to_excel_format(
            weekly_station, 'station')
        weekly_penetration = convert_db_columns_to_excel_format(
            weekly_penetration, 'penetration')

        # 过滤指定周
        weekly_station = weekly_station[weekly_station['年周'] == week]
        weekly_penetration = weekly_penetration[weekly_penetration['年周'] == week]

        result = {
            "region": region_name,
            "week": week,
            "metrics": calculate_metrics_for_period(weekly_station, weekly_penetration),
            "breakdownData": calculate_breakdown_for_period(weekly_station, weekly_penetration),
            "contributionData": calculate_contribution_for_period(weekly_station, weekly_penetration, filter_dict)
        }

        return JSONResponse(content=convert_decimals(result))
    except Exception as e:
        logger.error(f"计算周度详细数据时出错: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"计算周度详细数据失败: {str(e)}")

# ========== 季度数据接口 ==========


@app.post("/data/{region}/quarter/breakdown")
async def get_quarter_breakdown_data(
    region: str,
    request: Request  # 移除 db 参数
):
    """获取季度拆解数据"""
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")

    try:
        body = await request.json()
        metric = body.get("metric")
        start_date = body.get("startDate")
        end_date = body.get("endDate")
        filters = {k: v for k, v in body.items() if k not in [
            "metric", "startDate", "endDate"]}

        region_map = {
            "east1": "东一区",
            "east2": "东二区",
            "north": "北区",
            "south": "南区",
            "west": "西区",
            "center": "中区",
            "national": "全国"
        }

        region_name = region_map.get(region, region)

        # 获取日数据进行季度计算
        station_df = await data_service.get_region_data(region_name, 'daily', version_id)
        penetration_df = await data_service.get_region_data(region_name, 'penetration_daily', version_id)

        # 应用筛选
        if filters:
            if region == "national":
                filters['provinces'] = []
                filters['cities'] = []
            station_df = data_service.apply_filters(
                station_df, filters, 'station')
            penetration_df = data_service.apply_filters(
                penetration_df, filters, 'penetration')

        # 转换列名
        station_df = convert_db_columns_to_excel_format(station_df, 'station')
        penetration_df = convert_db_columns_to_excel_format(
            penetration_df, 'penetration')

        # 日期过滤
        station_df['统计日期'] = pd.to_datetime(station_df['统计日期'])
        penetration_df['统计日期'] = pd.to_datetime(penetration_df['统计日期'])

        station_df = station_df[
            (station_df['统计日期'] >= start_date) &
            (station_df['统计日期'] <= end_date)
        ]
        penetration_df = penetration_df[
            (penetration_df['统计日期'] >= start_date) &
            (penetration_df['统计日期'] <= end_date)
        ]

        # 聚合为季度数据
        station_df = aggregate_data_by_quarter(station_df, 'station')
        penetration_df = aggregate_data_by_quarter(
            penetration_df, 'penetration')

        # 计算拆解数据
        breakdown_data = calculate_quarter_breakdown(
            metric, station_df, penetration_df)

        return JSONResponse(content=convert_decimals({"breakdown": breakdown_data}))

    except Exception as e:
        logger.error(f"计算季度拆解数据时出错: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"计算季度拆解数据失败: {str(e)}")


@app.post("/data/{region}/quarter/component")
async def get_quarter_component_data(
    region: str,
    request: Request
):  # 移除 db: AsyncSession = Depends(get_db) 参数
    """获取季度组件贡献度数据"""
    # 获取最新版本ID
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")

    try:
        body = await request.json()
        component = body.get("component")
        metric = body.get("metric")
        quarter = body.get("quarter")
        year = body.get("year")
        filters = {
            "provinces": body.get("provinces", []),
            "cities": body.get("cities", []),
            "carModels": body.get("carModels", []),
            "carTypes": body.get("carTypes", []),
            "stationTypes": body.get("stationTypes", []),
            "stationAges": body.get("stationAges", [])
        }

        region_map = {
            "east1": "东一区",
            "east2": "东二区",
            "north": "北区",
            "south": "南区",
            "west": "西区",
            "center": "中区",
            "national": "全国"
        }

        region_name = region_map.get(region, region)

        # 计算季度日期范围
        start_month = (quarter - 1) * 3 + 1
        end_month = quarter * 3
        start_date = pd.Timestamp(year, start_month, 1)
        end_date = pd.Timestamp(year, end_month, 1) + \
            pd.DateOffset(months=1) - pd.DateOffset(days=1)

        # 获取数据 - 使用正确的方法
        if region == "national":
            # 获取全国数据（所有区域的聚合）
            all_regions = ["北区", "中区", "东一区", "东二区", "西区", "南区"]
            station_list = []
            penetration_list = []

            for r in all_regions:
                # 获取日数据进行季度计算
                r_station = await data_service.get_region_data(r, 'daily', version_id)
                r_pen = await data_service.get_region_data(r, 'penetration_daily', version_id)
                if not r_station.empty:
                    station_list.append(r_station)
                if not r_pen.empty:
                    penetration_list.append(r_pen)

            station_df = pd.concat(
                station_list, ignore_index=True) if station_list else pd.DataFrame()
            penetration_df = pd.concat(
                penetration_list, ignore_index=True) if penetration_list else pd.DataFrame()
        else:
            # 获取日数据进行季度计算
            station_df = await data_service.get_region_data(region_name, 'daily', version_id)
            penetration_df = await data_service.get_region_data(region_name, 'penetration_daily', version_id)

        # 应用筛选
        if filters:
            if region == "national":
                # 全国数据不筛选省份城市
                filters_copy = filters.copy()
                filters_copy['provinces'] = []
                filters_copy['cities'] = []
                station_df = data_service.apply_filters(
                    station_df, filters_copy, 'station')
                penetration_df = data_service.apply_filters(
                    penetration_df, filters_copy, 'penetration')
            else:
                station_df = data_service.apply_filters(
                    station_df, filters, 'station')
                penetration_df = data_service.apply_filters(
                    penetration_df, filters, 'penetration')

        # 转换列名为Excel格式
        station_df = convert_db_columns_to_excel_format(station_df, 'station')
        penetration_df = convert_db_columns_to_excel_format(
            penetration_df, 'penetration')

        # 确保日期列是datetime类型
        if '统计日期' in station_df.columns:
            station_df['统计日期'] = pd.to_datetime(station_df['统计日期'])
        if '统计日期' in penetration_df.columns:
            penetration_df['统计日期'] = pd.to_datetime(penetration_df['统计日期'])

        # 日期过滤
        station_df = station_df[
            (station_df['统计日期'] >= start_date) &
            (station_df['统计日期'] <= end_date)
        ]
        penetration_df = penetration_df[
            (penetration_df['统计日期'] >= start_date) &
            (penetration_df['统计日期'] <= end_date)
        ]

        # 生成组件数据
        component_data = generate_quarter_component_data(
            component, metric, station_df, penetration_df, region_name, filters
        )

        return JSONResponse(content=convert_decimals({"componentData": component_data}))

    except Exception as e:
        logger.error(f"计算季度组件数据时出错: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"计算季度组件数据失败: {str(e)}")


@app.post("/data/{region}/quarter/component/{component}")
async def get_specific_quarter_component_data(
    region: str,
    component: str,
    request: Request
):
    """获取特定季度组件数据"""
    # 获取最新版本ID
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")

    try:
        body = await request.json()
        year = body.get("year")
        quarter = body.get("quarter")
        metric = body.get("metric")
        filters = body.get("filters", {})

        region_map = {
            "east1": "东一区",
            "east2": "东二区",
            "north": "北区",
            "south": "南区",
            "west": "西区",
            "center": "中区",
            "national": "全国"
        }

        region_name = region_map.get(region, region)

        # 计算季度日期范围
        start_month = (quarter - 1) * 3 + 1
        end_month = quarter * 3
        start_date = pd.Timestamp(year, start_month, 1)
        end_date = pd.Timestamp(year, end_month, 1) + \
            pd.DateOffset(months=1) - pd.DateOffset(days=1)

        # 获取数据
        if region == "national":
            # 获取全国数据
            all_regions = ["北区", "中区", "东一区", "东二区", "西区", "南区"]
            station_list = []
            penetration_list = []

            for r in all_regions:
                r_station = await data_service.get_region_data(r, 'daily', version_id)
                r_pen = await data_service.get_region_data(r, 'penetration_daily', version_id)
                if not r_station.empty:
                    station_list.append(r_station)
                if not r_pen.empty:
                    penetration_list.append(r_pen)

            station_df = pd.concat(
                station_list, ignore_index=True) if station_list else pd.DataFrame()
            penetration_df = pd.concat(
                penetration_list, ignore_index=True) if penetration_list else pd.DataFrame()
        else:
            station_df = await data_service.get_region_data(region_name, 'daily', version_id)
            penetration_df = await data_service.get_region_data(region_name, 'penetration_daily', version_id)

        # 应用筛选
        if filters:
            if region == "national":
                filters_copy = filters.copy()
                filters_copy['provinces'] = []
                filters_copy['cities'] = []
                station_df = data_service.apply_filters(
                    station_df, filters_copy, 'station')
                penetration_df = data_service.apply_filters(
                    penetration_df, filters_copy, 'penetration')
            else:
                station_df = data_service.apply_filters(
                    station_df, filters, 'station')
                penetration_df = data_service.apply_filters(
                    penetration_df, filters, 'penetration')

        # 转换列名
        station_df = convert_db_columns_to_excel_format(station_df, 'station')
        penetration_df = convert_db_columns_to_excel_format(
            penetration_df, 'penetration')

        # 确保日期列是datetime类型
        if '统计日期' in station_df.columns:
            station_df['统计日期'] = pd.to_datetime(station_df['统计日期'])
        if '统计日期' in penetration_df.columns:
            penetration_df['统计日期'] = pd.to_datetime(penetration_df['统计日期'])

        # 日期过滤
        station_df = station_df[
            (station_df['统计日期'] >= start_date) &
            (station_df['统计日期'] <= end_date)
        ]
        penetration_df = penetration_df[
            (penetration_df['统计日期'] >= start_date) &
            (penetration_df['统计日期'] <= end_date)
        ]

        # 根据组件类型生成相应的贡献度数据
        component_data = {}

        if component == 'numerator':
            # 理想车主自建站充电量（分子）
            component_data = generate_numerator_contribution(
                penetration_df, filters)

        elif component == 'denominator':
            # 理想车主公充电量（分母）
            component_data = generate_denominator_contribution(
                penetration_df, filters)

        elif component == 'service-numerator':
            # 订单服务费收入
            component_data = generate_service_numerator_contribution(
                station_df, filters)

        elif component == 'power-numerator':
            # 直营站充电量
            component_data = generate_power_numerator_contribution(
                station_df, filters)

        elif component.startswith('revenue-'):
            # Net收入组件
            revenue_component = component.replace('revenue-', '')
            component_data = generate_revenue_component_contribution(
                station_df, revenue_component, filters, region_name
            )

        return JSONResponse(content=convert_decimals({
            "componentData": {component: component_data} if component_data else {}
        }))

    except Exception as e:
        logger.error(f"计算季度组件数据失败: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"计算季度组件数据失败: {str(e)}")
# ========== 月度/周度组件数据 ==========


@app.post("/data/{region}/month/{month}/component")
async def get_month_component_data(
    region: str,
    month: str,
    request: Request
):  # 移除 db 参数
    """获取特定月份的组件贡献度数据"""
    region_map = {
        "east1": "东一区",
        "east2": "东二区",
        "north": "北区",
        "south": "南区",
        "west": "西区",
        "center": "中区",
        "national": "全国"
    }

    if region not in region_map:
        raise HTTPException(status_code=400, detail="无效的大区")

    region_name = region_map[region]

    # 获取最新版本ID
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")

    try:
        body = await request.json()
        filters = {
            "provinces": body.get("provinces", []),
            "cities": body.get("cities", []),
            "carModels": body.get("carModels", []),
            "carTypes": body.get("carTypes", []),
            "stationTypes": body.get("stationTypes", []),
            "stationAges": body.get("stationAges", [])
        }
        component = body.get("component")
        metric = body.get("metric")

        # 获取数据 - 使用正确的方法
        if region == "national":
            # 获取全国数据（所有区域的聚合）
            all_regions = ["北区", "中区", "东一区", "东二区", "西区", "南区"]
            station_list = []
            penetration_list = []

            for r in all_regions:
                r_station = await data_service.get_region_data(r, 'monthly', version_id)
                r_pen = await data_service.get_region_data(r, 'penetration_monthly', version_id)
                if not r_station.empty:
                    station_list.append(r_station)
                if not r_pen.empty:
                    penetration_list.append(r_pen)

            station_df = pd.concat(
                station_list, ignore_index=True) if station_list else pd.DataFrame()
            penetration_df = pd.concat(
                penetration_list, ignore_index=True) if penetration_list else pd.DataFrame()
        else:
            station_df = await data_service.get_region_data(region_name, 'monthly', version_id)
            penetration_df = await data_service.get_region_data(region_name, 'penetration_monthly', version_id)

        # 应用筛选
        if filters:
            station_df = data_service.apply_filters(
                station_df, filters, 'station')
            penetration_df = data_service.apply_filters(
                penetration_df, filters, 'penetration')

        # 转换列名
        station_df = convert_db_columns_to_excel_format(station_df, 'station')
        penetration_df = convert_db_columns_to_excel_format(
            penetration_df, 'penetration')

        # 过滤月份
        station_df = station_df[station_df["统计日期"].dt.strftime(
            '%Y-%m') == month]
        penetration_df = penetration_df[penetration_df["统计日期"].dt.strftime(
            '%Y-%m') == month]

        # 生成组件数据
        component_data = generate_component_data_for_period(
            station_df, penetration_df, filters, region_name
        )

        result = {
            "region": region_name,
            "month": month,
            "component": component,
            "componentData": component_data
        }

        if component and component.startswith('revenue-'):
            if component_data and component not in component_data:
                component_data = {component: component_data}

        return JSONResponse(content=convert_decimals(result))

    except Exception as e:
        logger.error(f"计算组件数据时出错: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"计算组件数据失败: {str(e)}")


@app.post("/data/{region}/week/{week}/component")
async def get_week_component_data(
    region: str,
    week: str,
    request: Request
):  # 移除 db 参数
    """获取特定周的组件贡献度数据"""
    region_map = {
        "east1": "东一区",
        "east2": "东二区",
        "north": "北区",
        "south": "南区",
        "west": "西区",
        "center": "中区",
        "national": "全国"
    }

    if region not in region_map:
        raise HTTPException(status_code=400, detail="无效的大区")

    region_name = region_map[region]

    # 获取最新版本ID
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")

    try:
        body = await request.json()
        filters = {
            "provinces": body.get("provinces", []),
            "cities": body.get("cities", []),
            "carModels": body.get("carModels", []),
            "carTypes": body.get("carTypes", []),
            "stationTypes": body.get("stationTypes", []),
            "stationAges": body.get("stationAges", [])
        }
        component = body.get("component")
        metric = body.get("metric")

        # 获取周度聚合数据
        if region == "national":
            # 获取全国数据
            all_regions = ["北区", "中区", "东一区", "东二区", "西区", "南区"]
            station_list = []
            penetration_list = []

            for r in all_regions:
                r_station = await data_service.get_region_data(r, 'weekly', version_id)
                r_pen = await data_service.get_region_data(r, 'penetration_weekly', version_id)
                if not r_station.empty:
                    station_list.append(r_station)
                if not r_pen.empty:
                    penetration_list.append(r_pen)

            weekly_station = pd.concat(
                station_list, ignore_index=True) if station_list else pd.DataFrame()
            weekly_penetration = pd.concat(
                penetration_list, ignore_index=True) if penetration_list else pd.DataFrame()
        else:
            weekly_station = await data_service.get_region_data(region_name, 'weekly', version_id)
            weekly_penetration = await data_service.get_region_data(region_name, 'penetration_weekly', version_id)

        # 应用筛选
        if filters:
            weekly_station = data_service.apply_filters(
                weekly_station, filters, 'station')
            weekly_penetration = data_service.apply_filters(
                weekly_penetration, filters, 'penetration')

        # 转换列名
        weekly_station = convert_db_columns_to_excel_format(
            weekly_station, 'station')
        weekly_penetration = convert_db_columns_to_excel_format(
            weekly_penetration, 'penetration')

        # 过滤指定周
        weekly_station = weekly_station[weekly_station['年周'] == week]
        weekly_penetration = weekly_penetration[weekly_penetration['年周'] == week]

        # 生成组件数据
        component_data = generate_component_data_for_period(
            weekly_station, weekly_penetration, filters, region_name
        )

        result = {
            "region": region_name,
            "week": week,
            "component": component,
            "componentData": component_data
        }

        return JSONResponse(content=convert_decimals(result))

    except Exception as e:
        logger.error(f"计算组件数据时出错: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"计算组件数据失败: {str(e)}")

# ========== 导出功能 ==========


@app.get("/data/{region}/export")
async def export_region_data(
    region: str,
    request: Request  # 移除 db 参数
):
    """导出大区明细数据"""
    version_id = await db_manager.get_latest_version_id()
    if not version_id:
        raise HTTPException(status_code=404, detail="请先上传数据")

    region_map = {
        "east1": "东一区",
        "east2": "东二区",
        "north": "北区",
        "south": "南区",
        "west": "西区",
        "center": "中区"
    }

    if region not in region_map:
        raise HTTPException(status_code=400, detail="无效的大区")

    region_name = region_map[region]

    try:
        # 获取查询参数中的筛选条件
        filters = {
            "provinces": request.query_params.getlist("provinces[]"),
            "cities": request.query_params.getlist("cities[]"),
            "carModels": request.query_params.getlist("carModels[]"),
            "carTypes": request.query_params.getlist("carTypes[]"),
            "stationTypes": request.query_params.getlist("stationTypes[]"),
            "stationAges": request.query_params.getlist("stationAges[]")
        }

        # 获取日数据
        station_df = await data_service.get_region_data(region_name, 'daily', version_id)
        penetration_df = await data_service.get_region_data(region_name, 'penetration_daily', version_id)

        # 应用筛选
        if any(filters.values()):
            station_df = data_service.apply_filters(
                station_df, filters, 'station')
            penetration_df = data_service.apply_filters(
                penetration_df, filters, 'penetration')

        # 转换列名为Excel格式
        station_df = convert_db_columns_to_excel_format(station_df, 'station')
        penetration_df = convert_db_columns_to_excel_format(
            penetration_df, 'penetration')

        # 创建Excel文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # 导出站点数据
            station_df.to_excel(writer, sheet_name='站点明细数据', index=False)

            # 导出渗透率数据
            penetration_df.to_excel(writer, sheet_name='渗透率明细数据', index=False)

            # 获取workbook对象进行格式化
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#002D28',
                'font_color': 'white',
                'align': 'center',
                'valign': 'vcenter',
                'border': 1
            })

            # 格式化表头
            for sheet_name in ['站点明细数据', '渗透率明细数据']:
                worksheet = writer.sheets[sheet_name]
                worksheet.set_column('A:Z', 15)

                # 应用表头格式
                if sheet_name == '站点明细数据':
                    for col_num, value in enumerate(station_df.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                else:
                    for col_num, value in enumerate(penetration_df.columns.values):
                        worksheet.write(0, col_num, value, header_format)

        output.seek(0)

        # 生成文件名
        from urllib.parse import quote
        filename = f"{region_name}_明细数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return StreamingResponse(
            io.BytesIO(output.read()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"}
        )

    except Exception as e:
        logger.error(f"导出数据时出错: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"导出数据失败: {str(e)}")

# ========== 看板功能路由 ==========

# 看板展示页面


@app.get("/ThorBI", response_class=HTMLResponse)
async def dashboard_page():
    """返回看板页面"""
    try:
        with open("static/ThorBI.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return html_content
    except FileNotFoundError:
        return HTMLResponse(content="<h1>看板页面未找到</h1>", status_code=404)
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        return HTMLResponse(content="<h1>加载看板时出错</h1>", status_code=500)

# 看板上传页面


@app.get("/ThorBIup", response_class=HTMLResponse)
async def upload_page():
    """返回上传页面"""
    try:
        with open("static/ThorBIup.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return html_content
    except FileNotFoundError:
        return HTMLResponse(content="<h1>上传页面未找到</h1>", status_code=404)
    except Exception as e:
        logger.error(f"Error loading upload page: {e}")
        return HTMLResponse(content="<h1>加载上传页面时出错</h1>", status_code=500)

# ====== lcpcode2:数据上传管理(dataking) ===========
# 负责人：乔岳
# =============Start===============================

# 用于存储DataKing异步上传任务状态
dataking_tasks = {}


@app.post("/dataking/upload/{project_id}", response_model=UploadResponse)
async def dataking_upload(
    project_id: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user: Optional[str] = Header(None, alias="X-User")
):
    """
    DataKing统一数据上传接口

    - **project_id**: 项目标识符
    - **file**: 上传的文件
    - **user**: 用户信息（从请求头获取）
    """
    logger.info(
        f"DataKing上传请求 - 项目: {project_id}, 文件: {file.filename}, 用户: {user}")

    # 1. 检查项目是否注册
    handler_class = DataRegistry.get_handler(project_id)
    if not handler_class:
        logger.error(f"未知的项目ID: {project_id}")
        return UploadResponse(
            success=False,
            project_id=project_id,
            message=f"项目 '{project_id}' 未注册",
            errors=[
                f"未知的项目ID: {project_id}。可用项目: {', '.join(DataRegistry.list_projects())}"]
        )

    # 2. 获取项目配置
    config = DataRegistry.get_config(project_id) or {}

    # 3. 检查文件类型
    file_ext = file.filename.split('.')[-1].lower()
    allowed_types = config.get('allowed_file_types', ['xlsx', 'xls', 'csv'])
    if file_ext not in allowed_types:
        return UploadResponse(
            success=False,
            project_id=project_id,
            message="文件类型不支持",
            errors=[f"只支持以下文件类型: {', '.join(allowed_types)}"]
        )

    # 4. 读取文件内容
    try:
        contents = await file.read()
        filename = file.filename

        # 检查文件大小
        max_size = config.get('max_file_size', 150 * 1024 * 1024)  # 默认150MB
        if len(contents) > max_size:
            return UploadResponse(
                success=False,
                project_id=project_id,
                message="文件太大",
                errors=[f"文件大小不能超过 {max_size // (1024*1024)}MB"]
            )

        # 5. 创建处理器实例
        handler = handler_class(project_id)

        # 6. 决定是否异步处理
        async_threshold = config.get(
            'async_threshold', 5 * 1024 * 1024)  # 默认5MB

        if len(contents) > async_threshold:
            # 异步处理大文件
            task_id = f"{project_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{str(uuid.uuid4())[:8]}"

            # 初始化任务状态
            dataking_tasks[task_id] = {
                "status": "processing",
                "progress": 0,
                "message": "开始处理文件",
                "project_id": project_id,
                "filename": filename,
                "start_time": datetime.now().isoformat(),
                "user": user or "anonymous"
            }

            # 添加后台任务
            background_tasks.add_task(
                async_dataking_process,
                handler,
                contents,
                filename,
                task_id,
                user
            )

            return UploadResponse(
                success=True,
                project_id=project_id,
                task_id=task_id,
                message="文件已接收，正在后台处理",
                upload_time=datetime.now()
            )
        else:
            # 直接处理小文件
            result = await handler.handle_upload(contents, filename, user=user)

            return UploadResponse(
                success=result["success"],
                project_id=project_id,
                version_id=result.get("version_id"),
                message="上传成功" if result["success"] else "上传失败",
                errors=result.get("errors"),
                upload_time=datetime.now(),
                data=result.get("summary")  # 可选：返回处理摘要
            )

    except Exception as e:
        logger.error(f"处理上传文件时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return UploadResponse(
            success=False,
            project_id=project_id,
            message=f"处理文件时出错: {str(e)}",
            errors=[str(e)]
        )


async def async_dataking_process(handler, contents, filename, task_id, user):
    """DataKing异步处理上传文件"""
    try:
        logger.info(f"开始DataKing异步处理任务: {task_id}")

        # 更新进度
        dataking_tasks[task_id]["status"] = "processing"
        dataking_tasks[task_id]["progress"] = 10
        dataking_tasks[task_id]["message"] = "正在验证文件..."

        # 处理上传
        result = await handler.handle_upload(contents, filename, user=user)

        # 更新任务状态
        if result["success"]:
            dataking_tasks[task_id]["status"] = "completed"
            dataking_tasks[task_id]["progress"] = 100
            dataking_tasks[task_id]["message"] = "处理完成"
            dataking_tasks[task_id]["result"] = result
        else:
            dataking_tasks[task_id]["status"] = "failed"
            dataking_tasks[task_id]["progress"] = 0
            dataking_tasks[task_id]["message"] = "处理失败"
            dataking_tasks[task_id]["errors"] = result.get("errors", [])

        dataking_tasks[task_id]["end_time"] = datetime.now().isoformat()
        logger.info(f"DataKing任务 {task_id} 处理完成")

    except Exception as e:
        logger.error(f"处理DataKing任务 {task_id} 时出错: {str(e)}")
        dataking_tasks[task_id]["status"] = "failed"
        dataking_tasks[task_id]["progress"] = 0
        dataking_tasks[task_id]["message"] = "处理失败"
        dataking_tasks[task_id]["error"] = str(e)
        dataking_tasks[task_id]["end_time"] = datetime.now().isoformat()


@app.get("/dataking/upload/status/{task_id}")
async def get_dataking_status(task_id: str):
    """查询DataKing上传任务状态"""
    if task_id not in dataking_tasks:
        raise HTTPException(status_code=404, detail="任务不存在")

    task_status = dataking_tasks[task_id].copy()

    # 如果任务已完成或失败，可以在一段时间后清理
    if task_status["status"] in ["completed", "failed"]:
        if "end_time" in task_status:
            end_time = datetime.fromisoformat(task_status["end_time"])
            if datetime.now() - end_time > timedelta(minutes=30):  # 30分钟后清理
                del dataking_tasks[task_id]

    return task_status


@app.get("/dataking/projects")
async def list_dataking_projects():
    """获取所有DataKing已注册的数据上传项目"""
    projects = []
    for project_id in DataRegistry.list_projects():
        config = DataRegistry.get_config(project_id) or {}
        projects.append({
            "id": project_id,
            "name": config.get("name", project_id),
            "description": config.get("description", ""),
            "allowedFileTypes": config.get("allowed_file_types", ["xlsx", "xls"]),
            "maxFileSize": config.get("max_file_size", 100 * 1024 * 1024)
        })
    return {"projects": projects}

# ====== lcpcode2:数据上传管理(dataking) ===========
# 负责人：乔岳
# =============End===============================


# ====== lcpcode3:城市选址财务PXX看板(pxxdash) ===========
# 负责人：乔岳
# =============Start=====================================

# 导入PXX Dashboard API
app.include_router(pxx_router)

# 配置静态文件服务（用于CSS、JS等）
static_path = Path("src/dashboard/pxxdash")
if static_path.exists():
    app.mount("/pxxdash/static",
              StaticFiles(directory=str(static_path)), name="pxxdash-static")

# PXX Dashboard 主页路由
@app.get("/pxxdash")
async def pxxdash_index():
    """PXX Dashboard 主页"""
    index_path = Path("src/dashboard/pxxdash/index.html")
    if index_path.exists():
        # 读取并修改HTML内容，使路径正确
        with open(index_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        # 替换相对路径为正确的路径
        html_content = html_content.replace(
            'href="styles.css"', 'href="/pxxdash/styles.css"')
        html_content = html_content.replace(
            'src="App.jsx"', 'src="/pxxdash/App.jsx"')

        return HTMLResponse(content=html_content)
    return HTMLResponse("PXX Dashboard index.html not found", status_code=404)

# 处理 styles.css 路由
@app.get("/pxxdash/styles.css")
async def pxxdash_styles():
    file_path = Path("src/dashboard/pxxdash/styles.css")
    if file_path.exists():
        return FileResponse(
            str(file_path), 
            media_type="text/css",
            headers={"Content-Type": "text/css; charset=utf-8"}
        )
    return HTMLResponse("styles.css not found", status_code=404)

# 处理 App.jsx 路由
@app.get("/pxxdash/App.jsx")
async def pxxdash_app():
    file_path = Path("src/dashboard/pxxdash/App.jsx")
    if file_path.exists():
        return FileResponse(
            str(file_path), 
            media_type="application/javascript",
            headers={"Content-Type": "application/javascript; charset=utf-8"}
        )
    return HTMLResponse("App.jsx not found", status_code=404)

# 为了让相对路径的资源文件正常加载，添加额外的路由
@app.get("/pxxdash/{filename}")
async def pxxdash_static_files(filename: str):
    """处理其他静态文件请求"""
    # 安全检查，防止路径遍历
    if ".." in filename:
        return HTMLResponse("Invalid path", status_code=400)
    
    file_path = Path(f"src/dashboard/pxxdash/{filename}")
    if file_path.exists() and file_path.is_file():
        # 根据文件扩展名设置正确的 MIME 类型
        content_type = "application/octet-stream"
        if filename.endswith('.js') or filename.endswith('.jsx'):
            content_type = "application/javascript"
        elif filename.endswith('.css'):
            content_type = "text/css"
        elif filename.endswith('.html'):
            content_type = "text/html"
        elif filename.endswith('.json'):
            content_type = "application/json"
        
        return FileResponse(
            str(file_path),
            media_type=content_type,
            headers={"Content-Type": f"{content_type}; charset=utf-8"}
        )
    return HTMLResponse(f"File {filename} not found", status_code=404)

# PxxDash 上传页面
@app.get("/pxxdashupload", response_class=HTMLResponse)
async def pxxdash_upload_page():
    """PXX Dashboard 上传页面"""
    try:
        with open("src/dashboard/pxxdash/pxxdashupload.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>PxxDash上传页面未找到</h1>", status_code=404)


# ====== lcpcode3:城市选址财务PXX看板(pxxdash) ===========
# 负责人：乔岳
# =============End=====================================


# 修改同步数据的函数为异步
@app.post("/api/sync-data")
async def sync_data(data: DashboardData):
    """同步看板数据到数据库"""
    try:
        from database import AsyncSessionLocal
        from sqlalchemy import text

        async with AsyncSessionLocal() as session:
            # 获取当前日期和版本信息
            today = datetime.now().date()
            dt = datetime.now()

            # 查询今天的最大版本号 - 使用更简单的查询
            result = await session.execute(
                text("""
                    SELECT IFNULL(MAX(version), 0) as max_version 
                    FROM dashboard_raw_data 
                    WHERE DATE(upload_date) = DATE(:today)
                """),
                {"today": str(today)}
            )
            row = result.fetchone()
            next_version = (row.max_version +
                            1) if row and row.max_version is not None else 1

            # 将今天之前版本的is_latest设为0（而不是FALSE）
            await session.execute(
                text("""
                    UPDATE dashboard_raw_data 
                    SET is_latest = 0 
                    WHERE DATE(upload_date) = DATE(:today) 
                    AND is_latest = 1
                """),
                {"today": str(today)}
            )

            batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
            upload_user = data.user or "system"

            # 存储原始数据
            insert_count = 0
            for sheet_name, sheet_data in data.data.items():
                if not sheet_data or len(sheet_data) == 0:
                    continue

                for row in sheet_data:
                    if row and any(cell for cell in row if cell):
                        await session.execute(
                            text("""
                                INSERT INTO dashboard_raw_data 
                                (sheet_name, row_data, upload_batch, upload_date, version, is_latest, upload_user, dt)
                                VALUES (:sheet_name, :row_data, :upload_batch, :upload_date, :version, 1, :upload_user, :dt)
                            """),
                            {
                                "sheet_name": sheet_name,
                                "row_data": json.dumps(row, ensure_ascii=False),
                                "upload_batch": batch_id,
                                "upload_date": dt,
                                "version": next_version,
                                "upload_user": upload_user,
                                "dt": dt
                            }
                        )
                        insert_count += 1

            await session.commit()

        return JSONResponse(content={
            "status": "success",
            "message": f"数据同步成功，版本号：{next_version}",
            "timestamp": data.timestamp or datetime.now().isoformat(),
            "batch_id": batch_id,
            "version": next_version,
            "date": str(today),
            "rows_inserted": insert_count
        })

    except Exception as e:
        logger.error(f"同步失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# 获取数据库中的数据


@app.get("/api/dashboard-data")
async def get_dashboard_data(date: Optional[str] = None, compress: bool = True):
    """获取看板数据"""
    try:
        from database import AsyncSessionLocal
        from sqlalchemy import text

        async with AsyncSessionLocal() as session:
            # 确定查询日期
            if date:
                query_date = datetime.strptime(date, "%Y-%m-%d").date()
            else:
                # 获取最新有数据的日期
                result = await session.execute(
                    text("""
                        SELECT DATE(upload_date) as upload_date
                        FROM dashboard_raw_data
                        WHERE is_latest = 1
                        ORDER BY upload_date DESC
                        LIMIT 1
                    """)
                )
                row = result.fetchone()
                if row and row.upload_date:
                    query_date = row.upload_date
                else:
                    query_date = datetime.now().date()

            # 使用更简单的查询
            result = await session.execute(
                text("""
                    SELECT sheet_name, row_data, upload_batch, version, upload_date, upload_user, dt
                    FROM dashboard_raw_data
                    WHERE DATE(upload_date) = DATE(:query_date) 
                    AND is_latest = 1
                    ORDER BY id
                """),
                {"query_date": str(query_date)}
            )

            rows = result.fetchall()

            if not rows:
                # 查找可用日期
                result = await session.execute(
                    text("""
                        SELECT DISTINCT DATE(upload_date) as upload_date
                        FROM dashboard_raw_data
                        WHERE is_latest = 1
                        ORDER BY upload_date DESC
                        LIMIT 5
                    """)
                )
                available_dates = result.fetchall()

                return JSONResponse(content={
                    "data": {
                        "核心KPI": {},
                        "趋势数据": {},
                        "用户分层": {},
                        "区域数据": {},
                        "最新月份": datetime.now().strftime("%Y-%m"),
                        "数据日期": datetime.now().strftime("%Y-%m-%d"),
                        "时间进度": 0
                    },
                    "message": f"指定日期 {query_date} 没有数据",
                    "available_dates": [str(d.upload_date) for d in available_dates]
                })

            # 获取版本信息
            first_row = rows[0]
            version_info = {
                'date': str(first_row.upload_date),
                'version': first_row.version,
                'upload_time': first_row.dt.isoformat() if first_row.dt else None,
                'upload_user': first_row.upload_user
            }

            # 处理数据
            raw_data = {}
            for row in rows:
                sheet_name = row.sheet_name
                if sheet_name not in raw_data:
                    raw_data[sheet_name] = []
                try:
                    row_data = json.loads(row.row_data)
                    if row_data and any(cell for cell in row_data if cell):
                        raw_data[sheet_name].append(row_data)
                except Exception as e:
                    logger.error(f"解析数据行时出错: {e}")
                    continue

            # 使用计算器处理数据
            try:
                calculator = MetricsCalculator(raw_data)
                calculated_data = calculator.calculate_all_metrics()
            except Exception as e:
                logger.error(f"计算数据时出错: {str(e)}")
                calculated_data = {
                    "核心KPI": {},
                    "趋势数据": {},
                    "用户分层": {},
                    "区域数据": {},
                    "最新月份": datetime.now().strftime("%Y-%m"),
                    "数据日期": datetime.now().strftime("%Y-%m-%d"),
                    "时间进度": 0
                }

            # 合并原始数据和计算结果
            response_data = {
                **raw_data,
                **calculated_data
            }

            # 添加最后更新时间
            response_data['最后更新时间'] = version_info['upload_time'] if version_info['upload_time'] else datetime.now(
            ).isoformat()

            # ============ 新增的压缩逻辑 ============
            # 在返回之前，可以选择性地删除一些大的原始数据
            if compress:
                # 移除原始的Excel数据，只保留计算后的结果
                keys_to_remove = ['分月指标达成情况', '理想车主公充渗透率月度',
                                  '互联互通指标达成', '分月单枪日指标表',
                                  '上周城市省份核心指标',
                                  '理想车主公充渗透率周度']
                for key in keys_to_remove:
                    if key in response_data:
                        del response_data[key]

                print(f"压缩前数据大小: {len(str(raw_data))} 字符")
                print(f"压缩后数据大小: {len(str(response_data))} 字符")
            # ============ 压缩逻辑结束 ============

            return JSONResponse(content={
                "data": response_data,
                "version_info": version_info,
                "update_date": version_info['upload_time']
            })

    except Exception as e:
        logger.error(f"获取数据失败: {str(e)}")
        return JSONResponse(content={
            "data": {
                "核心KPI": {},
                "趋势数据": {},
                "用户分层": {},
                "区域数据": {},
                "最新月份": datetime.now().strftime("%Y-%m"),
                "数据日期": datetime.now().strftime("%Y-%m-%d"),
                "时间进度": 0
            },
            "message": f"处理数据时出错: {str(e)}"
        })


@app.get("/api/versions")
async def get_versions(date: Optional[str] = None):
    """获取版本列表"""
    try:
        from database import AsyncSessionLocal
        from sqlalchemy import text

        async with AsyncSessionLocal() as session:
            if date:
                # 获取指定日期的所有版本
                result = await session.execute(
                    text("""
                        SELECT DISTINCT version, upload_batch, dt, upload_user,
                               COUNT(*) as row_count
                        FROM dashboard_raw_data
                        WHERE upload_date = :upload_date
                        GROUP BY version, upload_batch, dt, upload_user
                        ORDER BY version DESC
                    """),
                    {"upload_date": date}
                )
            else:
                # 获取最近7天的版本信息
                result = await session.execute(
                    text("""
                        SELECT upload_date, version, upload_batch, dt, upload_user,
                               COUNT(*) as row_count, is_latest
                        FROM dashboard_raw_data
                        WHERE upload_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                        GROUP BY upload_date, version, upload_batch, dt, upload_user, is_latest
                        ORDER BY upload_date DESC, version DESC
                    """)
                )

            versions = result.fetchall()

            # 转换为字典列表
            version_list = []
            for v in versions:
                version_list.append({
                    "upload_date": str(v.upload_date) if hasattr(v, 'upload_date') else None,
                    "version": v.version,
                    "upload_batch": v.upload_batch,
                    "dt": v.dt.isoformat() if v.dt else None,
                    "upload_user": v.upload_user,
                    "row_count": v.row_count,
                    "is_latest": v.is_latest if hasattr(v, 'is_latest') else None
                })

            return JSONResponse(content={"versions": version_list})

    except Exception as e:
        logger.error(f"获取版本列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def preprocess_station_data(df: pd.DataFrame) -> pd.DataFrame:
    """预处理站点数据"""
    # 确保日期列是datetime类型
    df['统计日期'] = pd.to_datetime(df['统计日期'])

    # 数值列转换为数字类型
    numeric_cols = [
        "订单服务费收入（扣除分成）", "电卡销售收入", "占位费收入",
        "优惠券优惠金额", "车主优惠金额", "电卡优惠金额",
        "充电量", "订单服务费收入（不扣除分成）", "场站枪数"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 确保必需的列存在
    required_cols = [
        "统计日期", "场站ID", "区域", "省份", "城市",
        "场站类型", "新老站", "订单服务费收入（扣除分成）",
        "电卡销售收入", "占位费收入", "优惠券优惠金额",
        "车主优惠金额", "电卡优惠金额", "充电量",
        "订单服务费收入（不扣除分成）", "场站枪数"
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"站点数据缺少必需的列: {', '.join(missing_cols)}")

    return df


def preprocess_penetration_data(df: pd.DataFrame) -> pd.DataFrame:
    """预处理渗透率数据"""
    # 确保日期列是datetime类型
    df['统计日期'] = pd.to_datetime(df['统计日期'])

    # 数值列转换为数字类型
    numeric_cols = ["自建站充电量", "公充电量"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 确保必需的列存在
    required_cols = [
        "统计日期", "区域", "省份", "城市",
        "车型", "自建站充电量", "公充电量"
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"渗透率数据缺少必需的列: {', '.join(missing_cols)}")

    return df


def preprocess_target_data(df: pd.DataFrame) -> pd.DataFrame:
    """预处理目标数据"""
    # ✅ 修改：更灵活地处理列名
    # 尝试多种可能的列名
    column_mappings = {
        '区域': ['区域', 'region', 'Region'],
        '指标': ['指标', 'metric_name', 'Metric'],
        '目标周期': ['目标周期', 'target_period', 'Period'],
        '目标': ['目标', '目标值', 'target_value', 'Target']
    }

    # 重命名列以统一格式
    for target_col, possible_cols in column_mappings.items():
        for col in possible_cols:
            if col in df.columns and target_col not in df.columns:
                df = df.rename(columns={col: target_col})
                break

    # 确保必需的列存在
    required_cols = ["区域", "指标", "目标周期", "目标"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"目标数据缺少必需的列: {', '.join(missing_cols)}")

    # 处理目标值，如果是百分比字符串，转换为数值
    def parse_target_value(val):
        if isinstance(val, str) and val.endswith('%'):
            return float(val.rstrip('%'))
        return float(val)

    df['目标值'] = df['目标'].apply(parse_target_value)

    return df


def calculate_current_metrics(station_df: pd.DataFrame, penetration_df: pd.DataFrame) -> Dict[str, Any]:
    """计算当前（最新月份）的各项指标"""
    metrics = {}

    # 获取最新月份
    if len(station_df) > 0:
        latest_date = station_df["统计日期"].max()
        current_month_station = station_df[station_df["统计日期"] == latest_date]
    else:
        current_month_station = pd.DataFrame()

    if len(penetration_df) > 0:
        latest_date_pen = penetration_df["统计日期"].max()
        current_month_pen = penetration_df[penetration_df["统计日期"]
                                           == latest_date_pen]
    else:
        current_month_pen = pd.DataFrame()

    # 1. 公充渗透率
    if len(current_month_pen) > 0:
        total_self_charge = current_month_pen["自建站充电量"].sum()
        total_public_charge = current_month_pen["公充电量"].sum()
        penetration_rate = (
            total_self_charge / total_public_charge * 100) if total_public_charge > 0 else 0
        metrics["penetration"] = {
            "value": f"{penetration_rate:.1f}%",
            "numerator": float(total_self_charge),
            "denominator": float(total_public_charge)
        }
    else:
        metrics["penetration"] = {"value": "0.0%",
                                  "numerator": 0, "denominator": 0}

    # 2. Net收入（单位：万元）
    if len(current_month_station) > 0:
        revenue = (
            current_month_station["订单服务费收入（扣除分成）"].sum() +
            current_month_station["电卡销售收入"].sum() +
            current_month_station["占位费收入"].sum() -
            current_month_station["优惠券优惠金额"].sum() -
            current_month_station["车主优惠金额"].sum() -
            current_month_station["电卡优惠金额"].sum()
        ) / 10000  # 转换为万元
        metrics["revenue"] = {
            "value": f"{revenue:.0f}万", "amount": float(revenue)}
    else:
        metrics["revenue"] = {"value": "0万", "amount": 0}

    # 3. 单枪日服务费收入（使用枪天数）
    if len(current_month_station) > 0 and '枪天数' in current_month_station.columns:
        total_gun_days = current_month_station["枪天数"].sum()
        if total_gun_days > 0:
            # 修改计算公式：扣除各项优惠
            net_service_fee = (
                current_month_station["订单服务费收入（不扣除分成）"].sum() -
                current_month_station["车主优惠金额"].sum() -
                current_month_station["优惠券优惠金额"].sum() -
                current_month_station["电卡优惠金额"].sum()
            )
            service_fee = net_service_fee / total_gun_days
            metrics["service"] = {
                "value": f"{service_fee:.0f}元", "amount": float(service_fee)}
        else:
            metrics["service"] = {"value": "0元", "amount": 0}
    else:
        metrics["service"] = {"value": "0元", "amount": 0}

    # 4. 单枪日电量（使用枪天数）
    if len(current_month_station) > 0 and '枪天数' in current_month_station.columns:
        total_gun_days = current_month_station["枪天数"].sum()
        if total_gun_days > 0:
            power = current_month_station["充电量"].sum() / total_gun_days
            metrics["power"] = {
                "value": f"{power:.0f}度", "amount": float(power)}
        else:
            metrics["power"] = {"value": "0度", "amount": 0}
    else:
        metrics["power"] = {"value": "0度", "amount": 0}

    return metrics


def calculate_metric_value(metric: str, station_data: pd.DataFrame, pen_data: pd.DataFrame) -> Dict[str, Any]:
    """计算单个指标值"""
    if metric == "penetration":
        self_charge = pen_data["自建站充电量"].sum()
        public_charge = pen_data["公充电量"].sum()
        value = (self_charge / public_charge * 100) if public_charge > 0 else 0
        return {"raw": value, "formatted": f"{value:.1f}%"}

    elif metric == "revenue":
        revenue = (
            station_data["订单服务费收入（扣除分成）"].sum() +
            station_data["电卡销售收入"].sum() +
            station_data["占位费收入"].sum() -
            station_data["优惠券优惠金额"].sum() -
            station_data["车主优惠金额"].sum() -
            station_data["电卡优惠金额"].sum()
        ) / 10000
        return {"raw": revenue, "formatted": f"{revenue:.2f}万"}  # 保留2位小数

    elif metric == "service":
        if '枪天数' in station_data.columns:
            total_gun_days = station_data['枪天数'].sum()
        else:
            return {"raw": 0, "formatted": "0元"}

        net_service_fee = (
            station_data['订单服务费收入（不扣除分成）'].sum() -
            station_data['车主优惠金额'].sum() -
            station_data['优惠券优惠金额'].sum() -
            station_data['电卡优惠金额'].sum()
        )
        service = net_service_fee / total_gun_days if total_gun_days > 0 else 0

        return {"raw": service, "formatted": f"{service:.0f}元"}

    elif metric == "power":
        if '枪天数' in station_data.columns:
            total_gun_days = station_data['枪天数'].sum()
        else:
            return {"raw": 0, "formatted": "0度"}

        total_power = station_data['充电量'].sum()
        power = total_power / total_gun_days if total_gun_days > 0 else 0

        return {"raw": power, "formatted": f"{power:.0f}度"}

    return {"raw": 0, "formatted": "--"}


def calculate_quarter_metric_value(metric: str, station_df: pd.DataFrame, penetration_df: pd.DataFrame) -> Dict[str, Any]:
    """计算季度指标值"""
    if metric == "penetration":
        # 公充渗透率：计算季度的加权平均
        total_self = penetration_df["自建站充电量"].sum()
        total_public = penetration_df["公充电量"].sum()
        if total_public > 0:
            quarter_penetration = (total_self / total_public * 100)
            return {"raw": quarter_penetration, "formatted": f"{quarter_penetration:.1f}%"}
        else:
            return {"raw": 0, "formatted": "--"}

    elif metric == "revenue":
        # Net收入：季度累计
        quarter_revenue = (
            station_df["订单服务费收入（扣除分成）"].sum() +
            station_df["电卡销售收入"].sum() +
            station_df["占位费收入"].sum() -
            station_df["优惠券优惠金额"].sum() -
            station_df["车主优惠金额"].sum() -
            station_df["电卡优惠金额"].sum()
        ) / 10000
        if quarter_revenue != 0:  # 允许负值
            return {"raw": quarter_revenue, "formatted": f"{quarter_revenue:.2f}万"}
        else:
            return {"raw": 0, "formatted": "--"}

    elif metric == "service":
        # 单枪日服务费：计算季度加权平均
        if '枪天数' in station_df.columns:
            total_gun_days = station_df["枪天数"].sum()
            if total_gun_days > 0:
                net_service_fee = (
                    station_df["订单服务费收入（不扣除分成）"].sum() -
                    station_df["车主优惠金额"].sum() -
                    station_df["优惠券优惠金额"].sum() -
                    station_df["电卡优惠金额"].sum()
                )
                quarter_service = net_service_fee / total_gun_days
                return {"raw": quarter_service, "formatted": f"{quarter_service:.0f}元"}
            else:
                return {"raw": 0, "formatted": "--"}
        else:
            return {"raw": 0, "formatted": "--"}

    else:  # power
        # 单枪日电量：计算季度加权平均
        if '枪天数' in station_df.columns:
            total_gun_days = station_df["枪天数"].sum()
            if total_gun_days > 0:
                total_power = station_df["充电量"].sum()
                quarter_power = total_power / total_gun_days
                return {"raw": quarter_power, "formatted": f"{quarter_power:.0f}度"}
            else:
                return {"raw": 0, "formatted": "--"}
        else:
            return {"raw": 0, "formatted": "--"}


def get_target_value(region: str, metric: str, period: str, target_df: pd.DataFrame) -> float:
    """获取目标值"""
    # ✅ 添加安全检查
    if target_df is None or target_df.empty:
        return 0

    # ✅ 检查必要的列是否存在
    required_cols = ["区域", "指标", "目标周期", "目标值"]
    for col in required_cols:
        if col not in target_df.columns:
            print(f"警告：目标数据缺少列 '{col}'，可用列：{list(target_df.columns)}")
            return 0

    # 指标名称映射 - 修改映射关系以匹配您的数据
    metric_map = {
        "penetration": "公充渗透率",
        "revenue": "net收入",  # 确保与Excel中的名称一致
        "service": "单枪日服务费收入",  # 修改为与Excel一致
        "power": "单枪日电量"  # 修改为与Excel一致
    }

    metric_name = metric_map.get(metric, metric)

    # 查找对应的目标值
    try:
        target_row = target_df[
            (target_df["区域"] == region) &
            (target_df["指标"] == metric_name) &
            (target_df["目标周期"] == period)
        ]

        if not target_row.empty:
            value = target_row.iloc[0]["目标值"]
            # 处理空值或非数值的情况
            if pd.isna(value) or value == '' or value is None:
                return 0
            return float(value)
    except Exception as e:
        print(f"获取目标值时出错 - 区域:{region}, 指标:{metric_name}, 周期:{period}, 错误:{e}")
        return 0

    return 0


def generate_monthly_data(station_df: pd.DataFrame, penetration_df: pd.DataFrame,
                          national_station_df: pd.DataFrame = None,
                          national_pen_df: pd.DataFrame = None) -> Dict[str, List[Dict]]:
    """生成月度数据（包含完整的环比计算和全国数据对比）"""
    # 获取所有月份并排序
    months = sorted(station_df["统计日期"].unique()) if len(station_df) > 0 else []

    monthly_data = {
        "penetration": [],
        "revenue": [],
        "service": [],
        "power": []
    }

    # 如果没有提供全国数据，使用当前数据作为全国数据
    if national_station_df is None:
        national_station_df = station_df
    if national_pen_df is None:
        national_pen_df = penetration_df

    # 计算上月数据用于环比
    prev_month_data = {metric: {} for metric in [
        "penetration", "revenue", "service", "power"]}

    for i, month in enumerate(months):
        month_station = station_df[station_df["统计日期"] == month]
        month_pen = penetration_df[penetration_df["统计日期"] == month]

        # 全国数据
        national_month_station = national_station_df[national_station_df["统计日期"] == month]
        national_month_pen = national_pen_df[national_pen_df["统计日期"] == month]

        # 计算各项指标
        current_values = {}
        national_values = {}

        # 1. 公充渗透率
        self_charge = month_pen["自建站充电量"].sum()
        public_charge = month_pen["公充电量"].sum()
        penetration = (self_charge / public_charge *
                       100) if public_charge > 0 else 0
        current_values["penetration"] = penetration

        national_self = national_month_pen["自建站充电量"].sum()
        national_public = national_month_pen["公充电量"].sum()
        national_penetration = (
            national_self / national_public * 100) if national_public > 0 else 0
        national_values["penetration"] = national_penetration

        # 2. Net收入
        revenue = (
            month_station["订单服务费收入（扣除分成）"].sum() +
            month_station["电卡销售收入"].sum() +
            month_station["占位费收入"].sum() -
            month_station["优惠券优惠金额"].sum() -
            month_station["车主优惠金额"].sum() -
            month_station["电卡优惠金额"].sum()
        ) / 10000
        current_values["revenue"] = revenue

        national_revenue = (
            national_month_station["订单服务费收入（扣除分成）"].sum() +
            national_month_station["电卡销售收入"].sum() +
            national_month_station["占位费收入"].sum() -
            national_month_station["优惠券优惠金额"].sum() -
            national_month_station["车主优惠金额"].sum() -
            national_month_station["电卡优惠金额"].sum()
        ) / 10000
        national_values["revenue"] = national_revenue

        # 3. 单枪日服务费收入
        if '枪天数' in month_station.columns:
            gun_days = month_station["枪天数"].sum()
            net_service_fee = (
                month_station["订单服务费收入（不扣除分成）"].sum() -
                month_station["车主优惠金额"].sum() -
                month_station["优惠券优惠金额"].sum() -
                month_station["电卡优惠金额"].sum()
            )
            service = net_service_fee / gun_days if gun_days > 0 else 0
        else:
            service = 0
        current_values["service"] = service

        # 全国单枪日服务费收入
        if '枪天数' in national_month_station.columns:
            national_gun_days = national_month_station["枪天数"].sum()
            national_net_service_fee = (
                national_month_station["订单服务费收入（不扣除分成）"].sum() -
                national_month_station["车主优惠金额"].sum() -
                national_month_station["优惠券优惠金额"].sum() -
                national_month_station["电卡优惠金额"].sum()
            )
            national_service = national_net_service_fee / \
                national_gun_days if national_gun_days > 0 else 0
        else:
            national_service = 0
        national_values["service"] = national_service

        # 4. 单枪日电量
        if '枪天数' in month_station.columns:
            gun_days = month_station["枪天数"].sum()
            power = month_station["充电量"].sum(
            ) / gun_days if gun_days > 0 else 0
        else:
            power = 0
        current_values["power"] = power

        if '枪天数' in national_month_station.columns:
            national_gun_days = national_month_station["枪天数"].sum()
            national_power = national_month_station["充电量"].sum(
            ) / national_gun_days if national_gun_days > 0 else 0
        else:
            national_power = 0
        national_values["power"] = national_power

        # 添加到结果
        month_str = month.strftime("%Y-%m")

        # 计算环比趋势
        for metric in ["penetration", "revenue", "service", "power"]:
            # 计算环比
            if i > 0 and prev_month_data[metric]:
                prev_value = prev_month_data[metric].get(months[i-1], 0)
                if prev_value > 0:
                    change = (current_values[metric] -
                              prev_value) / prev_value * 100
                    trend = f"{'↑' if change >= 0 else '↓'} {abs(change):.1f}%"
                else:
                    trend = "-"
            else:
                trend = "-"

            # 格式化值
            if metric == "penetration":
                formatted_value = f"{current_values[metric]:.1f}%"
                formatted_national = f"{national_values[metric]:.1f}%"
            elif metric == "revenue":
                formatted_value = f"{current_values[metric]:.0f}万"
                formatted_national = f"{national_values[metric]:.0f}万"
            elif metric == "service":
                formatted_value = f"{current_values[metric]:.0f}元"
                formatted_national = f"{national_values[metric]:.0f}元"
            else:  # power
                formatted_value = f"{current_values[metric]:.0f}度"
                formatted_national = f"{national_values[metric]:.0f}度"

            monthly_data[metric].append({
                "month": month_str,
                "value": formatted_value,
                "nationalValue": formatted_national,
                "trend": trend
            })

            # 存储当前值用于下月计算
            prev_month_data[metric][month] = current_values[metric]

    return monthly_data


def generate_weekly_data(station_df: pd.DataFrame, penetration_df: pd.DataFrame,
                         national_station_df: pd.DataFrame = None,
                         national_pen_df: pd.DataFrame = None) -> Dict[str, List[Dict]]:
    """生成周度数据（使用预聚合的周度数据，包含完整的环比和全国对比）"""
    # 确保数据有年周列
    if '年周' not in station_df.columns or '年周' not in penetration_df.columns:
        return {
            "penetration": [],
            "revenue": [],
            "service": [],
            "power": []
        }

    # 确保日期列是datetime类型
    station_df = station_df.copy()
    penetration_df = penetration_df.copy()
    station_df['统计日期'] = pd.to_datetime(station_df['统计日期'])
    penetration_df['统计日期'] = pd.to_datetime(penetration_df['统计日期'])

    # 获取最近4周的数据
    if len(station_df) > 0:
        latest_date = station_df['统计日期'].max()
        four_weeks_ago = latest_date - timedelta(weeks=4)

        station_df = station_df[station_df['统计日期'] > four_weeks_ago]
        penetration_df = penetration_df[penetration_df['统计日期']
                                        > four_weeks_ago]

    # 如果没有提供全国数据，使用当前数据作为全国数据
    if national_station_df is None:
        national_station_df = station_df
    else:
        national_station_df = national_station_df.copy()
        national_station_df['统计日期'] = pd.to_datetime(
            national_station_df['统计日期'])
        national_station_df = national_station_df[national_station_df['统计日期']
                                                  > four_weeks_ago]

    if national_pen_df is None:
        national_pen_df = penetration_df
    else:
        national_pen_df = national_pen_df.copy()
        national_pen_df['统计日期'] = pd.to_datetime(national_pen_df['统计日期'])
        national_pen_df = national_pen_df[national_pen_df['统计日期']
                                          > four_weeks_ago]

    # 按周聚合数据
    weekly_data = {
        "penetration": [],
        "revenue": [],
        "service": [],
        "power": []
    }

    # 获取所有周并排序
    weeks = sorted(station_df['年周'].unique()) if len(station_df) > 0 else []

    # 计算上周数据用于环比
    prev_week_data = {metric: {}
                      for metric in ["penetration", "revenue", "service", "power"]}

    for i, week in enumerate(weeks[-4:]):  # 只取最近4周
        # 区域数据
        week_station = station_df[station_df['年周'] == week]
        week_pen = penetration_df[penetration_df['年周'] == week]

        # 全国数据
        national_week_station = national_station_df[national_station_df['年周'] == week]
        national_week_pen = national_pen_df[national_pen_df['年周'] == week]

        # 计算各项指标
        current_values = {}
        national_values = {}

        # 1. 公充渗透率
        self_charge = week_pen["自建站充电量"].sum()
        public_charge = week_pen["公充电量"].sum()
        penetration = (self_charge / public_charge *
                       100) if public_charge > 0 else 0
        current_values["penetration"] = penetration

        national_self = national_week_pen["自建站充电量"].sum()
        national_public = national_week_pen["公充电量"].sum()
        national_penetration = (
            national_self / national_public * 100) if national_public > 0 else 0
        national_values["penetration"] = national_penetration

        # 2. Net收入
        revenue = (
            week_station["订单服务费收入（扣除分成）"].sum() +
            week_station["电卡销售收入"].sum() +
            week_station["占位费收入"].sum() -
            week_station["优惠券优惠金额"].sum() -
            week_station["车主优惠金额"].sum() -
            week_station["电卡优惠金额"].sum()
        ) / 10000
        current_values["revenue"] = revenue

        national_revenue = (
            national_week_station["订单服务费收入（扣除分成）"].sum() +
            national_week_station["电卡销售收入"].sum() +
            national_week_station["占位费收入"].sum() -
            national_week_station["优惠券优惠金额"].sum() -
            national_week_station["车主优惠金额"].sum() -
            national_week_station["电卡优惠金额"].sum()
        ) / 10000
        national_values["revenue"] = national_revenue

        # 3. 单枪日服务费收入
        gun_days = week_station['枪天数'].sum(
        ) if '枪天数' in week_station.columns else 0
        if gun_days > 0:
            net_service_fee = (
                week_station["订单服务费收入（不扣除分成）"].sum() -
                week_station["车主优惠金额"].sum() -
                week_station["优惠券优惠金额"].sum() -
                week_station["电卡优惠金额"].sum()
            )
            service = net_service_fee / gun_days
        else:
            service = 0
        current_values["service"] = service

        national_gun_days = national_week_station['枪天数'].sum(
        ) if '枪天数' in national_week_station.columns else 0
        if national_gun_days > 0:
            national_net_service_fee = (
                national_week_station["订单服务费收入（不扣除分成）"].sum() -
                national_week_station["车主优惠金额"].sum() -
                national_week_station["优惠券优惠金额"].sum() -
                national_week_station["电卡优惠金额"].sum()
            )
            national_service = national_net_service_fee / national_gun_days
        else:
            national_service = 0
        national_values["service"] = national_service

        # 4. 单枪日电量
        power = week_station['充电量'].sum() / gun_days if gun_days > 0 else 0
        current_values["power"] = power

        national_power = national_week_station['充电量'].sum(
        ) / national_gun_days if national_gun_days > 0 else 0
        national_values["power"] = national_power

        # 计算环比趋势
        for metric in ["penetration", "revenue", "service", "power"]:
            # 计算环比
            if i > 0 and prev_week_data[metric]:
                # 获取正确的上周数据索引
                prev_week_index = i - 1
                if prev_week_index >= 0 and prev_week_index < len(weeks[-4:]):
                    prev_week_key = weeks[-4:][prev_week_index]
                    prev_value = prev_week_data[metric].get(prev_week_key, 0)
                    if prev_value > 0:
                        change = (
                            current_values[metric] - prev_value) / prev_value * 100
                        trend = f"{'↑' if change >= 0 else '↓'} {abs(change):.1f}%"
                    else:
                        trend = "-"
                else:
                    trend = "-"
            else:
                trend = "-"

            # 格式化值
            if metric == "penetration":
                formatted_value = f"{current_values[metric]:.1f}%"
                formatted_national = f"{national_values[metric]:.1f}%"
            elif metric == "revenue":
                formatted_value = f"{current_values[metric]:.0f}万"
                formatted_national = f"{national_values[metric]:.0f}万"
            elif metric == "service":
                formatted_value = f"{current_values[metric]:.0f}元"
                formatted_national = f"{national_values[metric]:.0f}元"
            else:  # power
                formatted_value = f"{current_values[metric]:.0f}度"
                formatted_national = f"{national_values[metric]:.0f}度"

            weekly_data[metric].append({
                "week": week,
                "value": formatted_value,
                "nationalValue": formatted_national,
                "trend": trend
            })

            # 存储当前值用于下周计算
            prev_week_data[metric][week] = current_values[metric]

    return weekly_data


def apply_filter_conditions_df(df: pd.DataFrame, filters: dict, data_type: str) -> pd.DataFrame:
    """应用筛选条件到数据框（完整的筛选逻辑）"""
    if not filters:
        return df

    df = df.copy()

    try:
        # 1. 省份筛选
        if filters.get("provinces") and len(filters["provinces"]) > 0:
            print(f"Filtering by provinces: {filters['provinces']}")
            df = df[df["省份"].isin(filters["provinces"])]
            print(f"After province filter, remaining rows: {len(df)}")

        # 2. 城市筛选 - 确保城市筛选生效
        if filters.get("cities") and len(filters["cities"]) > 0:
            print(f"Filtering by cities: {filters['cities']}")
            df = df[df["城市"].isin(filters["cities"])]
            print(f"After city filter, remaining rows: {len(df)}")

        if data_type == "penetration":
            # 3. 车型筛选（仅适用于公充渗透率数据）
            if filters.get("carModels") and len(filters["carModels"]) > 0:
                print(f"Filtering by car models: {filters['carModels']}")
                df = df[df["车型"].isin(filters["carModels"])]
                print(f"After car model filter, remaining rows: {len(df)}")

            # 4. 车型组筛选
            if filters.get("carTypes") and len(filters["carTypes"]) > 0:
                # 根据车型组筛选车型
                selected_models = []
                if "hybrid" in filters["carTypes"]:
                    selected_models.extend(["L9", "L8", "L7", "L6", "ONE"])
                if "electric" in filters["carTypes"]:
                    selected_models.extend(["MEGA", "W01", "i6", "i8"])

                if selected_models:
                    print(f"Filtering by car type models: {selected_models}")
                    df = df[df["车型"].isin(selected_models)]
                    print(f"After car type filter, remaining rows: {len(df)}")

        elif data_type == "station":
            # 5. 场站类型筛选（仅适用于单站数据）
            if filters.get("stationTypes") and len(filters["stationTypes"]) > 0:
                type_map = {
                    "store": "城市门店",
                    "self": "城市自营",
                    "franchise": "城市加盟"
                }
                mapped_types = [type_map.get(t, t)
                                for t in filters["stationTypes"]]
                print(f"Filtering by station types: {mapped_types}")
                df = df[df["场站类型"].isin(mapped_types)]
                print(f"After station type filter, remaining rows: {len(df)}")

            # 6. 新老站筛选
            if filters.get("stationAges") and len(filters["stationAges"]) > 0:
                age_map = {
                    "new": "新站",
                    "old": "老站"
                }
                mapped_ages = [age_map.get(a, a)
                               for a in filters["stationAges"]]
                print(f"Filtering by station ages: {mapped_ages}")
                df = df[df["新老站"].isin(mapped_ages)]
                print(f"After station age filter, remaining rows: {len(df)}")

        print(f"Final filtered dataframe shape: {df.shape}")
        return df

    except Exception as e:
        print(f"Error in apply_filter_conditions_df: {str(e)}")
        print(f"Filters: {filters}")
        print(f"Data type: {data_type}")
        print(f"DataFrame columns: {list(df.columns)}")
        raise


def generate_city_contribution(station_df: pd.DataFrame, penetration_df: pd.DataFrame,
                               metric: str) -> Dict[str, Any]:
    """生成城市维度贡献度（所有指标的完整实现）"""
    result = {}

    if metric == "penetration" and len(penetration_df) > 0:
        # 公充渗透率的城市贡献度
        total_self = penetration_df["自建站充电量"].sum()
        total_public = penetration_df["公充电量"].sum()
        total_penetration = (total_self / total_public *
                             100) if total_public > 0 else 0

        cities = sorted(penetration_df["城市"].unique())
        city_items = []

        for city in cities:
            city_pen = penetration_df[penetration_df["城市"] == city]
            city_self = city_pen["自建站充电量"].sum()
            city_public = city_pen["公充电量"].sum()
            city_penetration = (city_self / city_public *
                                100) if city_public > 0 else 0

            # 贡献度：该城市自建站充电量占总自建站充电量的比例
            contribution = (city_self / total_self *
                            100) if total_self > 0 else 0

            city_items.append({
                "id": city,
                "name": city,
                "value": round(contribution, 1),
                "absolute": f"{city_penetration:.1f}%"
            })

        result = {
            "total": f"{total_penetration:.1f}%",
            "items": sorted(city_items, key=lambda x: x["value"], reverse=True)
        }

    elif metric == "revenue" and len(station_df) > 0:
        # Net收入的城市贡献度
        total_revenue = (
            station_df["订单服务费收入（扣除分成）"].sum() +
            station_df["电卡销售收入"].sum() +
            station_df["占位费收入"].sum() -
            station_df["优惠券优惠金额"].sum() -
            station_df["车主优惠金额"].sum() -
            station_df["电卡优惠金额"].sum()
        ) / 10000

        cities = sorted(station_df["城市"].unique())
        city_items = []

        for city in cities:
            city_station = station_df[station_df["城市"] == city]
            city_revenue = (
                city_station["订单服务费收入（扣除分成）"].sum() +
                city_station["电卡销售收入"].sum() +
                city_station["占位费收入"].sum() -
                city_station["优惠券优惠金额"].sum() -
                city_station["车主优惠金额"].sum() -
                city_station["电卡优惠金额"].sum()
            ) / 10000

            contribution = (city_revenue / total_revenue *
                            100) if total_revenue > 0 else 0

            city_items.append({
                "id": city,
                "name": city,
                "value": round(contribution, 1),
                "absolute": f"{city_revenue:.0f}万"
            })

        result = {
            "total": f"{total_revenue:.0f}万",
            "items": sorted(city_items, key=lambda x: x["value"], reverse=True)
        }

    elif metric == "service" and len(station_df) > 0:
        # 单枪日服务费的城市贡献度（使用加权贡献）
        total_gun_days = station_df["枪天数"].sum(
        ) if '枪天数' in station_df.columns else 0

        if total_gun_days > 0:
            total_net_service = (
                station_df["订单服务费收入（不扣除分成）"].sum() -
                station_df["车主优惠金额"].sum() -
                station_df["优惠券优惠金额"].sum() -
                station_df["电卡优惠金额"].sum()
            )
            total_value = total_net_service / total_gun_days

            cities = sorted(station_df["城市"].unique())
            city_items = []

            for city in cities:
                city_station = station_df[station_df["城市"] == city]
                city_gun_days = city_station["枪天数"].sum(
                ) if '枪天数' in city_station.columns else 0

                if city_gun_days > 0:
                    city_net_service = (
                        city_station["订单服务费收入（不扣除分成）"].sum() -
                        city_station["车主优惠金额"].sum() -
                        city_station["优惠券优惠金额"].sum() -
                        city_station["电卡优惠金额"].sum()
                    )
                    city_value = city_net_service / city_gun_days
                else:
                    city_value = 0

                # 加权贡献度计算
                city_weighted_contribution = (
                    city_value * city_gun_days / total_gun_days) if total_gun_days > 0 else 0
                city_contribution_percent = (
                    city_weighted_contribution / total_value * 100) if total_value > 0 else 0

                city_items.append({
                    "id": city,
                    "name": city,
                    "value": round(city_contribution_percent, 1),
                    "absolute": f"{city_value:.0f}元"
                })

            result = {
                "total": f"{total_value:.0f}元",
                "items": sorted(city_items, key=lambda x: x["value"], reverse=True)
            }
        else:
            result = {
                "total": "0元",
                "items": []
            }

    elif metric == "power" and len(station_df) > 0:
        # 单枪日电量的城市贡献度（使用加权贡献）
        total_gun_days = station_df["枪天数"].sum(
        ) if '枪天数' in station_df.columns else 0

        if total_gun_days > 0:
            total_power = station_df["充电量"].sum()
            total_value = total_power / total_gun_days

            cities = sorted(station_df["城市"].unique())
            city_items = []

            for city in cities:
                city_station = station_df[station_df["城市"] == city]
                city_gun_days = city_station["枪天数"].sum(
                ) if '枪天数' in city_station.columns else 0

                if city_gun_days > 0:
                    city_power = city_station["充电量"].sum()
                    city_value = city_power / city_gun_days
                else:
                    city_value = 0

                # 加权贡献度计算
                city_weighted_contribution = (
                    city_value * city_gun_days / total_gun_days) if total_gun_days > 0 else 0
                city_contribution_percent = (
                    city_weighted_contribution / total_value * 100) if total_value > 0 else 0

                city_items.append({
                    "id": city,
                    "name": city,
                    "value": round(city_contribution_percent, 1),
                    "absolute": f"{city_value:.0f}度"
                })

            result = {
                "total": f"{total_value:.0f}度",
                "items": sorted(city_items, key=lambda x: x["value"], reverse=True)
            }
        else:
            result = {
                "total": "0度",
                "items": []
            }

    return result


def generate_province_contribution(station_df: pd.DataFrame, penetration_df: pd.DataFrame,
                                   metric: str, include_cities: bool = True) -> Dict[str, Any]:
    """生成省份维度贡献度（带城市展开，所有指标的完整实现）"""
    result = {}

    if metric == "penetration" and len(penetration_df) > 0:
        provinces = sorted(penetration_df["省份"].unique())
        geo_items = []

        total_self = penetration_df["自建站充电量"].sum()
        total_public = penetration_df["公充电量"].sum()
        total_penetration = (total_self / total_public *
                             100) if total_public > 0 else 0

        for province in provinces[:10]:
            prov_pen = penetration_df[penetration_df["省份"] == province]
            prov_self = prov_pen["自建站充电量"].sum()
            prov_public = prov_pen["公充电量"].sum()
            prov_penetration = (prov_self / prov_public *
                                100) if prov_public > 0 else 0

            contribution = (prov_self / total_self *
                            100) if total_self > 0 else 0

            province_item = {
                "id": province,
                "name": province,
                "value": round(contribution, 1),
                "absolute": f"{prov_penetration:.1f}%"
            }

            # 获取城市数据
            if include_cities:
                cities = sorted(prov_pen["城市"].unique())
                city_items = []

                for city in cities[:5]:
                    city_pen = prov_pen[prov_pen["城市"] == city]
                    city_self = city_pen["自建站充电量"].sum()
                    city_public = city_pen["公充电量"].sum()
                    city_penetration = (
                        city_self / city_public * 100) if city_public > 0 else 0
                    city_contribution = (
                        city_self / prov_self * 100) if prov_self > 0 else 0

                    city_items.append({
                        "id": city,
                        "name": city,
                        "value": round(city_contribution, 1),
                        "absolute": f"{city_penetration:.1f}%"
                    })

                province_item["children"] = sorted(
                    city_items, key=lambda x: x["value"], reverse=True)

            geo_items.append(province_item)

        result = {
            "total": f"{total_penetration:.1f}%",
            "items": sorted(geo_items, key=lambda x: x["value"], reverse=True)
        }

    elif metric == "revenue" and len(station_df) > 0:
        # Net收入的省份贡献度
        total_value = (
            station_df["订单服务费收入（扣除分成）"].sum() +
            station_df["电卡销售收入"].sum() +
            station_df["占位费收入"].sum() -
            station_df["优惠券优惠金额"].sum() -
            station_df["车主优惠金额"].sum() -
            station_df["电卡优惠金额"].sum()
        ) / 10000

        provinces = sorted(station_df["省份"].unique())
        geo_items = []

        for province in provinces[:10]:
            prov_station = station_df[station_df["省份"] == province]

            prov_value = (
                prov_station["订单服务费收入（扣除分成）"].sum() +
                prov_station["电卡销售收入"].sum() +
                prov_station["占位费收入"].sum() -
                prov_station["优惠券优惠金额"].sum() -
                prov_station["车主优惠金额"].sum() -
                prov_station["电卡优惠金额"].sum()
            ) / 10000

            contribution = (prov_value / total_value *
                            100) if total_value > 0 else 0

            province_item = {
                "id": province,
                "name": province,
                "value": round(contribution, 1),
                "absolute": f"{prov_value:.0f}万"
            }

            # 添加城市数据
            if include_cities:
                cities = sorted(prov_station["城市"].unique())
                city_items = []

                for city in cities[:5]:
                    city_station = prov_station[prov_station["城市"] == city]
                    city_value = (
                        city_station["订单服务费收入（扣除分成）"].sum() +
                        city_station["电卡销售收入"].sum() +
                        city_station["占位费收入"].sum() -
                        city_station["优惠券优惠金额"].sum() -
                        city_station["车主优惠金额"].sum() -
                        city_station["电卡优惠金额"].sum()
                    ) / 10000

                    city_contribution = (
                        city_value / prov_value * 100) if prov_value > 0 else 0

                    city_items.append({
                        "id": city,
                        "name": city,
                        "value": round(city_contribution, 1),
                        "absolute": f"{city_value:.0f}万"
                    })

                province_item["children"] = sorted(
                    city_items, key=lambda x: x["value"], reverse=True)

            geo_items.append(province_item)

        result = {
            "total": f"{total_value:.0f}万",
            "items": sorted(geo_items, key=lambda x: x["value"], reverse=True)
        }

    elif metric == "service" and len(station_df) > 0:
        # 单枪日服务费的省份贡献度（使用加权贡献）
        total_gun_days = station_df["枪天数"].sum(
        ) if '枪天数' in station_df.columns else 0

        if total_gun_days > 0:
            total_net_service = (
                station_df["订单服务费收入（不扣除分成）"].sum() -
                station_df["车主优惠金额"].sum() -
                station_df["优惠券优惠金额"].sum() -
                station_df["电卡优惠金额"].sum()
            )
            total_value = total_net_service / total_gun_days

            provinces = sorted(station_df["省份"].unique())
            geo_items = []

            for province in provinces[:10]:
                prov_station = station_df[station_df["省份"] == province]
                prov_gun_days = prov_station["枪天数"].sum(
                ) if '枪天数' in prov_station.columns else 0

                if prov_gun_days > 0:
                    prov_net_service = (
                        prov_station["订单服务费收入（不扣除分成）"].sum() -
                        prov_station["车主优惠金额"].sum() -
                        prov_station["优惠券优惠金额"].sum() -
                        prov_station["电卡优惠金额"].sum()
                    )
                    prov_value = prov_net_service / prov_gun_days
                else:
                    prov_value = 0

                # 加权贡献度
                weighted_contribution = (
                    prov_value * prov_gun_days / total_gun_days) if total_gun_days > 0 else 0
                contribution_percent = (
                    weighted_contribution / total_value * 100) if total_value > 0 else 0

                province_item = {
                    "id": province,
                    "name": province,
                    "value": round(contribution_percent, 1),
                    "absolute": f"{prov_value:.0f}元"
                }

                # 获取城市数据
                if include_cities:
                    cities = sorted(prov_station["城市"].unique())
                    city_items = []

                    for city in cities[:5]:
                        city_station = prov_station[prov_station["城市"] == city]
                        city_gun_days = city_station["枪天数"].sum(
                        ) if '枪天数' in city_station.columns else 0

                        if city_gun_days > 0:
                            city_net_service = (
                                city_station["订单服务费收入（不扣除分成）"].sum() -
                                city_station["车主优惠金额"].sum() -
                                city_station["优惠券优惠金额"].sum() -
                                city_station["电卡优惠金额"].sum()
                            )
                            city_value = city_net_service / city_gun_days
                        else:
                            city_value = 0

                        # 城市对省份的加权贡献
                        city_weighted_contribution = (
                            city_value * city_gun_days / prov_gun_days) if prov_gun_days > 0 else 0
                        city_contribution_percent = (
                            city_weighted_contribution / prov_value * 100) if prov_value > 0 else 0

                        city_items.append({
                            "id": city,
                            "name": city,
                            "value": round(city_contribution_percent, 1),
                            "absolute": f"{city_value:.0f}元"
                        })

                    province_item["children"] = sorted(
                        city_items, key=lambda x: x["value"], reverse=True)

                geo_items.append(province_item)

            result = {
                "total": f"{total_value:.0f}元",
                "items": sorted(geo_items, key=lambda x: x["value"], reverse=True)
            }
        else:
            result = {"total": "0元", "items": []}

    elif metric == "power" and len(station_df) > 0:
        # 单枪日电量的省份贡献度（使用加权贡献）
        total_gun_days = station_df["枪天数"].sum(
        ) if '枪天数' in station_df.columns else 0

        if total_gun_days > 0:
            total_power = station_df["充电量"].sum()
            total_value = total_power / total_gun_days

            provinces = sorted(station_df["省份"].unique())
            geo_items = []

            for province in provinces[:10]:
                prov_station = station_df[station_df["省份"] == province]
                prov_gun_days = prov_station["枪天数"].sum(
                ) if '枪天数' in prov_station.columns else 0

                if prov_gun_days > 0:
                    prov_power = prov_station["充电量"].sum()
                    prov_value = prov_power / prov_gun_days
                else:
                    prov_value = 0

                # 加权贡献度
                weighted_contribution = (
                    prov_value * prov_gun_days / total_gun_days) if total_gun_days > 0 else 0
                contribution_percent = (
                    weighted_contribution / total_value * 100) if total_value > 0 else 0

                province_item = {
                    "id": province,
                    "name": province,
                    "value": round(contribution_percent, 1),
                    "absolute": f"{prov_value:.0f}度"
                }

                # 获取城市数据
                if include_cities:
                    cities = sorted(prov_station["城市"].unique())
                    city_items = []

                    for city in cities[:5]:
                        city_station = prov_station[prov_station["城市"] == city]
                        city_gun_days = city_station["枪天数"].sum(
                        ) if '枪天数' in city_station.columns else 0

                        if city_gun_days > 0:
                            city_power = city_station["充电量"].sum()
                            city_value = city_power / city_gun_days
                        else:
                            city_value = 0

                        # 城市对省份的加权贡献
                        city_weighted_contribution = (
                            city_value * city_gun_days / prov_gun_days) if prov_gun_days > 0 else 0
                        city_contribution_percent = (
                            city_weighted_contribution / prov_value * 100) if prov_value > 0 else 0

                        city_items.append({
                            "id": city,
                            "name": city,
                            "value": round(city_contribution_percent, 1),
                            "absolute": f"{city_value:.0f}度"
                        })

                    province_item["children"] = sorted(
                        city_items, key=lambda x: x["value"], reverse=True)

                geo_items.append(province_item)

            result = {
                "total": f"{total_value:.0f}度",
                "items": sorted(geo_items, key=lambda x: x["value"], reverse=True)
            }
        else:
            result = {"total": "0度", "items": []}

    return result


def generate_car_model_contribution(penetration_df: pd.DataFrame, metric: str) -> Dict[str, Any]:
    """生成车型贡献度（仅公充渗透率指标）"""
    if metric != "penetration" or len(penetration_df) == 0:
        return {}

    total_self = penetration_df["自建站充电量"].sum()
    total_public = penetration_df["公充电量"].sum()
    total_penetration = (total_self / total_public *
                         100) if total_public > 0 else 0

    car_models = sorted(penetration_df["车型"].unique())
    car_items = []

    for model in car_models:
        model_pen = penetration_df[penetration_df["车型"] == model]
        model_self = model_pen["自建站充电量"].sum()
        model_public = model_pen["公充电量"].sum()
        model_penetration = (model_self / model_public *
                             100) if model_public > 0 else 0

        contribution = (model_self / total_self * 100) if total_self > 0 else 0

        car_items.append({
            "id": model,
            "name": f"理想{model}",
            "value": round(contribution, 1),
            "absolute": f"{model_penetration:.1f}%"
        })

    return {
        "total": f"{total_penetration:.1f}%",
        "items": sorted(car_items, key=lambda x: x["value"], reverse=True)
    }


def generate_station_type_contribution(station_df: pd.DataFrame, metric: str) -> Dict[str, Any]:
    """生成场站类型贡献度（revenue、service、power指标的完整实现）"""
    if metric == "penetration" or len(station_df) == 0:
        return {}

    station_types = sorted(station_df["场站类型"].unique())
    type_items = []

    if metric == "revenue":
        # Revenue直接用占比
        total_value = (
            station_df["订单服务费收入（扣除分成）"].sum() +
            station_df["电卡销售收入"].sum() +
            station_df["占位费收入"].sum() -
            station_df["优惠券优惠金额"].sum() -
            station_df["车主优惠金额"].sum() -
            station_df["电卡优惠金额"].sum()
        ) / 10000

        for st_type in station_types:
            type_station = station_df[station_df["场站类型"] == st_type]
            type_value = (
                type_station["订单服务费收入（扣除分成）"].sum() +
                type_station["电卡销售收入"].sum() +
                type_station["占位费收入"].sum() -
                type_station["优惠券优惠金额"].sum() -
                type_station["车主优惠金额"].sum() -
                type_station["电卡优惠金额"].sum()
            ) / 10000

            contribution = (type_value / total_value *
                            100) if total_value > 0 else 0

            type_items.append({
                "id": st_type,
                "name": st_type,
                "value": round(contribution, 1),
                "absolute": f"{type_value:.0f}万"
            })

        return {
            "total": f"{total_value:.0f}万",
            "items": sorted(type_items, key=lambda x: x["value"], reverse=True)
        }

    elif metric == "service":
        # service使用加权贡献
        total_gun_days = station_df["枪天数"].sum(
        ) if '枪天数' in station_df.columns else 0

        if total_gun_days > 0:
            total_net_service = (
                station_df["订单服务费收入（不扣除分成）"].sum() -
                station_df["车主优惠金额"].sum() -
                station_df["优惠券优惠金额"].sum() -
                station_df["电卡优惠金额"].sum()
            )
            total_value = total_net_service / total_gun_days

            for st_type in station_types:
                type_station = station_df[station_df["场站类型"] == st_type]
                type_gun_days = type_station["枪天数"].sum(
                ) if '枪天数' in type_station.columns else 0

                if type_gun_days > 0:
                    type_net_service = (
                        type_station["订单服务费收入（不扣除分成）"].sum() -
                        type_station["车主优惠金额"].sum() -
                        type_station["优惠券优惠金额"].sum() -
                        type_station["电卡优惠金额"].sum()
                    )
                    type_value = type_net_service / type_gun_days
                else:
                    type_value = 0

                weighted_contribution = (
                    type_value * type_gun_days / total_gun_days) if total_gun_days > 0 else 0
                contribution = (weighted_contribution /
                                total_value * 100) if total_value > 0 else 0

                type_items.append({
                    "id": st_type,
                    "name": st_type,
                    "value": round(contribution, 1),
                    "absolute": f"{type_value:.0f}元"
                })

            return {
                "total": f"{total_value:.0f}元",
                "items": sorted(type_items, key=lambda x: x["value"], reverse=True)
            }
        else:
            return {"total": "0元", "items": []}

    elif metric == "power":
        # power使用加权贡献
        total_gun_days = station_df["枪天数"].sum(
        ) if '枪天数' in station_df.columns else 0

        if total_gun_days > 0:
            total_power = station_df["充电量"].sum()
            total_value = total_power / total_gun_days

            for st_type in station_types:
                type_station = station_df[station_df["场站类型"] == st_type]
                type_gun_days = type_station["枪天数"].sum(
                ) if '枪天数' in type_station.columns else 0

                if type_gun_days > 0:
                    type_power = type_station["充电量"].sum()
                    type_value = type_power / type_gun_days
                else:
                    type_value = 0

                weighted_contribution = (
                    type_value * type_gun_days / total_gun_days) if total_gun_days > 0 else 0
                contribution = (weighted_contribution /
                                total_value * 100) if total_value > 0 else 0

                type_items.append({
                    "id": st_type,
                    "name": st_type,
                    "value": round(contribution, 1),
                    "absolute": f"{type_value:.0f}度"
                })

            return {
                "total": f"{total_value:.0f}度",
                "items": sorted(type_items, key=lambda x: x["value"], reverse=True)
            }
        else:
            return {"total": "0度", "items": []}

    return {}


def generate_station_age_contribution(station_df: pd.DataFrame, metric: str) -> Dict[str, Any]:
    """生成新老站贡献度（revenue、service、power指标的完整实现）"""
    if metric == "penetration" or len(station_df) == 0:
        return {}

    station_ages = sorted(station_df["新老站"].unique())
    age_items = []

    if metric == "revenue":
        total_value = (
            station_df["订单服务费收入（扣除分成）"].sum() +
            station_df["电卡销售收入"].sum() +
            station_df["占位费收入"].sum() -
            station_df["优惠券优惠金额"].sum() -
            station_df["车主优惠金额"].sum() -
            station_df["电卡优惠金额"].sum()
        ) / 10000

        for age in station_ages:
            age_station = station_df[station_df["新老站"] == age]
            age_value = (
                age_station["订单服务费收入（扣除分成）"].sum() +
                age_station["电卡销售收入"].sum() +
                age_station["占位费收入"].sum() -
                age_station["优惠券优惠金额"].sum() -
                age_station["车主优惠金额"].sum() -
                age_station["电卡优惠金额"].sum()
            ) / 10000

            contribution = (age_value / total_value *
                            100) if total_value > 0 else 0

            age_items.append({
                "id": age,
                "name": age,
                "value": round(contribution, 1),
                "absolute": f"{age_value:.0f}万"
            })

        return {
            "total": f"{total_value:.0f}万",
            "items": sorted(age_items, key=lambda x: x["value"], reverse=True)
        }

    elif metric == "service":
        total_gun_days = station_df["枪天数"].sum(
        ) if '枪天数' in station_df.columns else 0

        if total_gun_days > 0:
            total_net_service = (
                station_df["订单服务费收入（不扣除分成）"].sum() -
                station_df["车主优惠金额"].sum() -
                station_df["优惠券优惠金额"].sum() -
                station_df["电卡优惠金额"].sum()
            )
            total_value = total_net_service / total_gun_days

            for age in station_ages:
                age_station = station_df[station_df["新老站"] == age]
                age_gun_days = age_station["枪天数"].sum(
                ) if '枪天数' in age_station.columns else 0

                if age_gun_days > 0:
                    age_net_service = (
                        age_station["订单服务费收入（不扣除分成）"].sum() -
                        age_station["车主优惠金额"].sum() -
                        age_station["优惠券优惠金额"].sum() -
                        age_station["电卡优惠金额"].sum()
                    )
                    age_value = age_net_service / age_gun_days
                else:
                    age_value = 0

                weighted_contribution = (
                    age_value * age_gun_days / total_gun_days) if total_gun_days > 0 else 0
                contribution = (weighted_contribution /
                                total_value * 100) if total_value > 0 else 0

                age_items.append({
                    "id": age,
                    "name": age,
                    "value": round(contribution, 1),
                    "absolute": f"{age_value:.0f}元"
                })

            return {
                "total": f"{total_value:.0f}元",
                "items": sorted(age_items, key=lambda x: x["value"], reverse=True)
            }
        else:
            return {"total": "0元", "items": []}

    elif metric == "power":
        total_gun_days = station_df["枪天数"].sum(
        ) if '枪天数' in station_df.columns else 0

        if total_gun_days > 0:
            total_power = station_df["充电量"].sum()
            total_value = total_power / total_gun_days

            for age in station_ages:
                age_station = station_df[station_df["新老站"] == age]
                age_gun_days = age_station["枪天数"].sum(
                ) if '枪天数' in age_station.columns else 0

                if age_gun_days > 0:
                    age_power = age_station["充电量"].sum()
                    age_value = age_power / age_gun_days
                else:
                    age_value = 0

                weighted_contribution = (
                    age_value * age_gun_days / total_gun_days) if total_gun_days > 0 else 0
                contribution = (weighted_contribution /
                                total_value * 100) if total_value > 0 else 0

                age_items.append({
                    "id": age,
                    "name": age,
                    "value": round(contribution, 1),
                    "absolute": f"{age_value:.0f}度"
                })

            return {
                "total": f"{total_value:.0f}度",
                "items": sorted(age_items, key=lambda x: x["value"], reverse=True)
            }
        else:
            return {"total": "0度", "items": []}

    return {}


def generate_numerator_contribution(penetration_df: pd.DataFrame, filters: dict = None) -> Dict[str, Any]:
    """生成分子（自建站充电量）贡献度数据（完整的地域和车型维度）"""
    result = {}

    total_self_charge = penetration_df["自建站充电量"].sum()

    # 地域贡献度 - 根据筛选条件决定显示哪个维度
    if not filters or not filters.get("provinces") or len(filters.get("provinces", [])) == 0:
        # 未筛选省份：显示省份维度（带城市展开）
        geo_items = []

        provinces = sorted(penetration_df["省份"].unique())
        for province in provinces[:10]:
            prov_df = penetration_df[penetration_df["省份"] == province]
            prov_self = prov_df["自建站充电量"].sum()
            contribution = (prov_self / total_self_charge *
                            100) if total_self_charge > 0 else 0

            # 获取城市数据
            cities = sorted(prov_df["城市"].unique())
            city_items = []

            for city in cities[:5]:
                city_df = prov_df[prov_df["城市"] == city]
                city_self = city_df["自建站充电量"].sum()
                city_contribution = (
                    city_self / prov_self * 100) if prov_self > 0 else 0

                city_items.append({
                    "id": city,
                    "name": city,
                    "value": round(city_contribution, 1),
                    "absolute": f"{city_self:,.0f} 度"
                })

            geo_items.append({
                "id": province,
                "name": province,
                "value": round(contribution, 1),
                "absolute": f"{prov_self:,.0f} 度",
                "children": sorted(city_items, key=lambda x: x["value"], reverse=True)
            })

        result["geography"] = {
            "total": f"{total_self_charge:,.0f} 度",
            "items": sorted(geo_items, key=lambda x: x["value"], reverse=True)
        }

    elif filters and filters.get("provinces") and len(filters["provinces"]) > 0 and (not filters.get("cities") or len(filters["cities"]) == 0):
        # 筛选了省份但未筛选城市，显示城市贡献度
        city_items = []

        # 获取所有城市（已经被省份筛选过的数据）
        cities = sorted(penetration_df["城市"].unique())

        for city in cities:
            city_df = penetration_df[penetration_df["城市"] == city]
            city_self = city_df["自建站充电量"].sum()

            # 计算该城市占总自建站充电量的比例
            contribution = (city_self / total_self_charge *
                            100) if total_self_charge > 0 else 0

            city_items.append({
                "id": city,
                "name": city,
                "value": round(contribution, 1),
                "absolute": f"{city_self:,.0f} 度"
            })

        result["city"] = {
            "total": f"{total_self_charge:,.0f} 度",
            "items": sorted(city_items, key=lambda x: x["value"], reverse=True)
        }

    # 如果筛选了省份和城市，则不显示地理维度

    # 车型贡献度 - 只在未筛选车型时显示
    if not filters or not filters.get("carModels") or len(filters["carModels"]) == 0:
        car_models = sorted(penetration_df["车型"].unique())
        car_items = []

        for model in car_models:
            model_df = penetration_df[penetration_df["车型"] == model]
            model_self = model_df["自建站充电量"].sum()
            contribution = (model_self / total_self_charge *
                            100) if total_self_charge > 0 else 0

            car_items.append({
                "id": model,
                "name": f"理想{model}",
                "value": round(contribution, 1),
                "absolute": f"{model_self:,.0f} 度"
            })

        result["carModel"] = {
            "total": f"{total_self_charge:,.0f} 度",
            "items": sorted(car_items, key=lambda x: x["value"], reverse=True)
        }

    return result


def generate_denominator_contribution(penetration_df: pd.DataFrame, filters: dict = None) -> Dict[str, Any]:
    """生成分母（公充电量）贡献度数据（完整的地域和车型维度）"""
    result = {}

    total_public_charge = penetration_df["公充电量"].sum()

    # 地域贡献度 - 根据筛选条件决定显示哪个维度
    if not filters or not filters.get("provinces") or len(filters.get("provinces", [])) == 0:
        # 未筛选省份：显示省份维度（带城市展开）
        geo_items = []

        provinces = sorted(penetration_df["省份"].unique())
        for province in provinces[:10]:
            prov_df = penetration_df[penetration_df["省份"] == province]
            prov_public = prov_df["公充电量"].sum()
            contribution = (prov_public / total_public_charge *
                            100) if total_public_charge > 0 else 0

            # 获取城市数据
            cities = sorted(prov_df["城市"].unique())
            city_items = []

            for city in cities[:5]:
                city_df = prov_df[prov_df["城市"] == city]
                city_public = city_df["公充电量"].sum()
                city_contribution = (
                    city_public / prov_public * 100) if prov_public > 0 else 0

                city_items.append({
                    "id": city,
                    "name": city,
                    "value": round(city_contribution, 1),
                    "absolute": f"{city_public:,.0f} 度"
                })

            geo_items.append({
                "id": province,
                "name": province,
                "value": round(contribution, 1),
                "absolute": f"{prov_public:,.0f} 度",
                "children": sorted(city_items, key=lambda x: x["value"], reverse=True)
            })

        result["geography"] = {
            "total": f"{total_public_charge:,.0f} 度",
            "items": sorted(geo_items, key=lambda x: x["value"], reverse=True)
        }

    elif filters and filters.get("provinces") and len(filters["provinces"]) > 0 and (not filters.get("cities") or len(filters["cities"]) == 0):
        # 筛选了省份但未筛选城市，显示城市贡献度
        city_items = []

        cities = sorted(penetration_df["城市"].unique())
        for city in cities:
            city_df = penetration_df[penetration_df["城市"] == city]
            city_public = city_df["公充电量"].sum()
            contribution = (city_public / total_public_charge *
                            100) if total_public_charge > 0 else 0

            city_items.append({
                "id": city,
                "name": city,
                "value": round(contribution, 1),
                "absolute": f"{city_public:,.0f} 度"
            })

        result["city"] = {
            "total": f"{total_public_charge:,.0f} 度",
            "items": sorted(city_items, key=lambda x: x["value"], reverse=True)
        }

    # 车型贡献度 - 只在未筛选车型时显示
    if not filters or not filters.get("carModels") or len(filters["carModels"]) == 0:
        car_models = sorted(penetration_df["车型"].unique())
        car_items = []

        for model in car_models:
            model_df = penetration_df[penetration_df["车型"] == model]
            model_public = model_df["公充电量"].sum()
            contribution = (model_public / total_public_charge *
                            100) if total_public_charge > 0 else 0

            car_items.append({
                "id": model,
                "name": f"理想{model}",
                "value": round(contribution, 1),
                "absolute": f"{model_public:,.0f} 度"
            })

        result["carModel"] = {
            "total": f"{total_public_charge:,.0f} 度",
            "items": sorted(car_items, key=lambda x: x["value"], reverse=True)
        }

    return result


def generate_service_numerator_contribution(station_df: pd.DataFrame, filters: dict = None) -> Dict[str, Any]:
    """生成服务费收入贡献度数据（完整的地域、场站类型、新老站维度）"""
    result = {}

    # 计算净服务费总额
    total_net_service_fee = (
        station_df["订单服务费收入（不扣除分成）"].sum() -
        station_df["车主优惠金额"].sum() -
        station_df["优惠券优惠金额"].sum() -
        station_df["电卡优惠金额"].sum()
    ) / 10000  # 转换为万元

    # 地域贡献度 - 根据筛选条件决定显示哪个维度
    if not filters or not filters.get("provinces") or len(filters.get("provinces", [])) == 0:
        # 未筛选省份：显示省份维度（带城市展开）
        geo_items = []
        provinces = sorted(station_df["省份"].unique())

        for province in provinces[:10]:
            prov_df = station_df[station_df["省份"] == province]
            prov_net_service_fee = (
                prov_df["订单服务费收入（不扣除分成）"].sum() -
                prov_df["车主优惠金额"].sum() -
                prov_df["优惠券优惠金额"].sum() -
                prov_df["电卡优惠金额"].sum()
            ) / 10000

            contribution = (prov_net_service_fee / total_net_service_fee *
                            100) if total_net_service_fee > 0 else 0

            # 添加城市数据
            cities = sorted(prov_df["城市"].unique())
            city_items = []

            for city in cities[:5]:
                city_df = prov_df[prov_df["城市"] == city]
                city_net_service_fee = (
                    city_df["订单服务费收入（不扣除分成）"].sum() -
                    city_df["车主优惠金额"].sum() -
                    city_df["优惠券优惠金额"].sum() -
                    city_df["电卡优惠金额"].sum()
                ) / 10000

                city_contribution = (
                    city_net_service_fee / prov_net_service_fee * 100) if prov_net_service_fee > 0 else 0

                city_items.append({
                    "id": city,
                    "name": city,
                    "value": round(city_contribution, 1),
                    "absolute": f"{city_net_service_fee:.1f}万元"
                })

            geo_items.append({
                "id": province,
                "name": province,
                "value": round(contribution, 1),
                "absolute": f"{prov_net_service_fee:.1f}万元",
                "children": sorted(city_items, key=lambda x: x["value"], reverse=True)
            })

        result["geography"] = {
            "total": f"{total_net_service_fee:.1f}万元",
            "items": sorted(geo_items, key=lambda x: x["value"], reverse=True)
        }

    elif filters and filters.get("provinces") and len(filters["provinces"]) > 0 and (not filters.get("cities") or len(filters["cities"]) == 0):
        # 筛选了省份但未筛选城市，显示城市贡献度
        city_items = []
        cities = sorted(station_df["城市"].unique())

        for city in cities:
            city_df = station_df[station_df["城市"] == city]
            city_net_service_fee = (
                city_df["订单服务费收入（不扣除分成）"].sum() -
                city_df["车主优惠金额"].sum() -
                city_df["优惠券优惠金额"].sum() -
                city_df["电卡优惠金额"].sum()
            ) / 10000

            contribution = (city_net_service_fee / total_net_service_fee *
                            100) if total_net_service_fee > 0 else 0

            city_items.append({
                "id": city,
                "name": city,
                "value": round(contribution, 1),
                "absolute": f"{city_net_service_fee:.1f}万元"
            })

        result["city"] = {
            "total": f"{total_net_service_fee:.1f}万元",
            "items": sorted(city_items, key=lambda x: x["value"], reverse=True)
        }

    # 场站类型贡献度 - 只在未筛选场站类型时显示
    if not filters or not filters.get("stationTypes") or len(filters["stationTypes"]) == 0:
        station_types = sorted(station_df["场站类型"].unique())
        type_items = []

        for st_type in station_types:
            type_df = station_df[station_df["场站类型"] == st_type]
            type_net_service_fee = (
                type_df["订单服务费收入（不扣除分成）"].sum() -
                type_df["车主优惠金额"].sum() -
                type_df["优惠券优惠金额"].sum() -
                type_df["电卡优惠金额"].sum()
            ) / 10000

            contribution = (type_net_service_fee / total_net_service_fee *
                            100) if total_net_service_fee > 0 else 0

            type_items.append({
                "id": st_type,
                "name": st_type,
                "value": round(contribution, 1),
                "absolute": f"{type_net_service_fee:.1f}万元"
            })

        result["stationType"] = {
            "total": f"{total_net_service_fee:.1f}万元",
            "items": sorted(type_items, key=lambda x: x["value"], reverse=True)
        }

    # 新老站贡献度 - 只在未筛选新老站时显示
    if not filters or not filters.get("stationAges") or len(filters["stationAges"]) == 0:
        station_ages = sorted(station_df["新老站"].unique())
        age_items = []

        for age in station_ages:
            age_df = station_df[station_df["新老站"] == age]
            age_net_service_fee = (
                age_df["订单服务费收入（不扣除分成）"].sum() -
                age_df["车主优惠金额"].sum() -
                age_df["优惠券优惠金额"].sum() -
                age_df["电卡优惠金额"].sum()
            ) / 10000

            contribution = (age_net_service_fee / total_net_service_fee *
                            100) if total_net_service_fee > 0 else 0

            age_items.append({
                "id": age,
                "name": age,
                "value": round(contribution, 1),
                "absolute": f"{age_net_service_fee:.1f}万元"
            })

        result["stationAge"] = {
            "total": f"{total_net_service_fee:.1f}万元",
            "items": sorted(age_items, key=lambda x: x["value"], reverse=True)
        }

    return result


def generate_power_numerator_contribution(station_df: pd.DataFrame, filters: dict = None) -> Dict[str, Any]:
    """生成充电量贡献度数据（完整的地域、场站类型、新老站维度）"""
    result = {}

    # 计算总充电量
    total_power = station_df["充电量"].sum() / 10000  # 转换为万度

    # 地域贡献度 - 根据筛选条件决定显示哪个维度
    if not filters or not filters.get("provinces") or len(filters.get("provinces", [])) == 0:
        # 未筛选省份：显示省份维度（带城市展开）
        geo_items = []
        provinces = sorted(station_df["省份"].unique())

        for province in provinces[:10]:
            prov_df = station_df[station_df["省份"] == province]
            prov_power = prov_df["充电量"].sum() / 10000

            contribution = (prov_power / total_power *
                            100) if total_power > 0 else 0

            # 添加城市数据
            cities = sorted(prov_df["城市"].unique())
            city_items = []

            for city in cities[:5]:
                city_df = prov_df[prov_df["城市"] == city]
                city_power = city_df["充电量"].sum() / 10000

                city_contribution = (
                    city_power / prov_power * 100) if prov_power > 0 else 0

                city_items.append({
                    "id": city,
                    "name": city,
                    "value": round(city_contribution, 1),
                    "absolute": f"{city_power:.1f}万度"
                })

            geo_items.append({
                "id": province,
                "name": province,
                "value": round(contribution, 1),
                "absolute": f"{prov_power:.1f}万度",
                "children": sorted(city_items, key=lambda x: x["value"], reverse=True)
            })

        result["geography"] = {
            "total": f"{total_power:.1f}万度",
            "items": sorted(geo_items, key=lambda x: x["value"], reverse=True)
        }

    elif filters and filters.get("provinces") and len(filters["provinces"]) > 0 and (not filters.get("cities") or len(filters["cities"]) == 0):
        # 筛选了省份但未筛选城市，显示城市贡献度
        city_items = []
        cities = sorted(station_df["城市"].unique())

        for city in cities:
            city_df = station_df[station_df["城市"] == city]
            city_power = city_df["充电量"].sum() / 10000

            contribution = (city_power / total_power *
                            100) if total_power > 0 else 0

            city_items.append({
                "id": city,
                "name": city,
                "value": round(contribution, 1),
                "absolute": f"{city_power:.1f}万度"
            })

        result["city"] = {
            "total": f"{total_power:.1f}万度",
            "items": sorted(city_items, key=lambda x: x["value"], reverse=True)
        }

    # 场站类型贡献度 - 只在未筛选场站类型时显示
    if not filters or not filters.get("stationTypes") or len(filters["stationTypes"]) == 0:
        station_types = sorted(station_df["场站类型"].unique())
        type_items = []

        for st_type in station_types:
            type_df = station_df[station_df["场站类型"] == st_type]
            type_power = type_df["充电量"].sum() / 10000

            contribution = (type_power / total_power *
                            100) if total_power > 0 else 0

            type_items.append({
                "id": st_type,
                "name": st_type,
                "value": round(contribution, 1),
                "absolute": f"{type_power:.1f}万度"
            })

        result["stationType"] = {
            "total": f"{total_power:.1f}万度",
            "items": sorted(type_items, key=lambda x: x["value"], reverse=True)
        }

    # 新老站贡献度 - 只在未筛选新老站时显示
    if not filters or not filters.get("stationAges") or len(filters["stationAges"]) == 0:
        station_ages = sorted(station_df["新老站"].unique())
        age_items = []

        for age in station_ages:
            age_df = station_df[station_df["新老站"] == age]
            age_power = age_df["充电量"].sum() / 10000

            contribution = (age_power / total_power *
                            100) if total_power > 0 else 0

            age_items.append({
                "id": age,
                "name": age,
                "value": round(contribution, 1),
                "absolute": f"{age_power:.1f}万度"
            })

        result["stationAge"] = {
            "total": f"{total_power:.1f}万度",
            "items": sorted(age_items, key=lambda x: x["value"], reverse=True)
        }

    return result


def generate_revenue_component_contribution(station_df: pd.DataFrame, component_type: str,
                                            filters: dict = None, region: str = None) -> Dict[str, Any]:
    """生成Net收入组件贡献度（完整实现所有维度）"""
    result = {}

    # 根据组件类型选择对应的列
    column_map = {
        'service-fee': '订单服务费收入（扣除分成）',
        'card-sales': '电卡销售收入',
        'parking-fee': '占位费收入',
        'coupon-discount': '优惠券优惠金额',
        'owner-discount': '车主优惠金额',
        'card-discount': '电卡优惠金额'
    }

    column_name = column_map.get(component_type)
    if not column_name:
        return result

    # 计算总额
    total_amount = station_df[column_name].sum() / 10000  # 转换为万元

    # 地域贡献度 - 根据筛选条件和区域决定是否显示
    # 全国数据不显示地理维度
    if region != "全国":
        if not filters or not filters.get("provinces") or len(filters.get("provinces", [])) == 0:
            # 未筛选省份：显示省份维度（带城市展开）
            geo_items = []
            provinces = sorted(station_df["省份"].unique())

            for province in provinces[:10]:
                prov_df = station_df[station_df["省份"] == province]
                prov_amount = prov_df[column_name].sum() / 10000

                contribution = (prov_amount / total_amount *
                                100) if total_amount > 0 else 0

                # 添加城市数据
                cities = sorted(prov_df["城市"].unique())
                city_items = []

                for city in cities[:5]:
                    city_df = prov_df[prov_df["城市"] == city]
                    city_amount = city_df[column_name].sum() / 10000

                    city_contribution = (
                        city_amount / prov_amount * 100) if prov_amount > 0 else 0

                    city_items.append({
                        "id": city,
                        "name": city,
                        "value": round(city_contribution, 1),
                        "absolute": f"{city_amount:.1f}万元"
                    })

                geo_items.append({
                    "id": province,
                    "name": province,
                    "value": round(contribution, 1),
                    "absolute": f"{prov_amount:.1f}万元",
                    "children": sorted(city_items, key=lambda x: x["value"], reverse=True)
                })

            result["geography"] = {
                "total": f"{total_amount:.1f}万元",
                "items": sorted(geo_items, key=lambda x: x["value"], reverse=True)
            }

        elif filters and filters.get("provinces") and len(filters["provinces"]) > 0 and (not filters.get("cities") or len(filters["cities"]) == 0):
            # 筛选了省份但未筛选城市，显示城市贡献度
            city_items = []
            cities = sorted(station_df["城市"].unique())

            for city in cities:
                city_df = station_df[station_df["城市"] == city]
                city_amount = city_df[column_name].sum() / 10000

                contribution = (city_amount / total_amount *
                                100) if total_amount > 0 else 0

                city_items.append({
                    "id": city,
                    "name": city,
                    "value": round(contribution, 1),
                    "absolute": f"{city_amount:.1f}万元"
                })

            result["city"] = {
                "total": f"{total_amount:.1f}万元",
                "items": sorted(city_items, key=lambda x: x["value"], reverse=True)
            }

    # 场站类型贡献度 - 只在未筛选场站类型时显示
    if not filters or not filters.get("stationTypes") or len(filters["stationTypes"]) == 0:
        station_types = sorted(station_df["场站类型"].unique())
        type_items = []

        for st_type in station_types:
            type_df = station_df[station_df["场站类型"] == st_type]
            type_amount = type_df[column_name].sum() / 10000

            contribution = (type_amount / total_amount *
                            100) if total_amount > 0 else 0

            type_items.append({
                "id": st_type,
                "name": st_type,
                "value": round(contribution, 1),
                "absolute": f"{type_amount:.1f}万元"
            })

        result["stationType"] = {
            "total": f"{total_amount:.1f}万元",
            "items": sorted(type_items, key=lambda x: x["value"], reverse=True)
        }

    # 新老站贡献度 - 只在未筛选新老站时显示
    if not filters or not filters.get("stationAges") or len(filters["stationAges"]) == 0:
        station_ages = sorted(station_df["新老站"].unique())
        age_items = []

        for age in station_ages:
            age_df = station_df[station_df["新老站"] == age]
            age_amount = age_df[column_name].sum() / 10000

            contribution = (age_amount / total_amount *
                            100) if total_amount > 0 else 0

            age_items.append({
                "id": age,
                "name": age,
                "value": round(contribution, 1),
                "absolute": f"{age_amount:.1f}万元"
            })

        result["stationAge"] = {
            "total": f"{total_amount:.1f}万元",
            "items": sorted(age_items, key=lambda x: x["value"], reverse=True)
        }

    return result


def generate_component_data(station_df: pd.DataFrame, penetration_df: pd.DataFrame,
                            filters: dict = None, region: str = None) -> Dict[str, Any]:
    """生成组件贡献度数据（分子分母分析）"""
    component_data = {}

    # 1. 分子（理想车主自建站充电量）贡献度
    component_data["numerator"] = generate_numerator_contribution(
        penetration_df, filters)

    # 2. 分母（理想车主公充电量）贡献度
    component_data["denominator"] = generate_denominator_contribution(
        penetration_df, filters)

    # 3. service-numerator（订单服务费收入）贡献度
    component_data["service-numerator"] = generate_service_numerator_contribution(
        station_df, filters)

    # 4. power-numerator（直营站充电量）贡献度
    component_data["power-numerator"] = generate_power_numerator_contribution(
        station_df, filters)

    return component_data


def generate_component_data_for_period(station_df: pd.DataFrame, penetration_df: pd.DataFrame,
                                       filters: dict = None, region: str = None,
                                       component: str = None) -> Dict[str, Any]:
    """为特定时间段生成组件贡献度数据（处理所有组件类型）"""
    component_data = {}

    # 如果指定了特定组件
    if component:
        if component == 'numerator':
            # 理想车主自建站充电量（分子）
            component_data['numerator'] = generate_numerator_contribution(
                penetration_df, filters)

        elif component == 'denominator':
            # 理想车主公充电量（分母）
            component_data['denominator'] = generate_denominator_contribution(
                penetration_df, filters)

        elif component == 'service-numerator':
            # 订单服务费收入
            component_data['service-numerator'] = generate_service_numerator_contribution(
                station_df, filters)

        elif component == 'power-numerator':
            # 直营站充电量
            component_data['power-numerator'] = generate_power_numerator_contribution(
                station_df, filters)

        elif component.startswith('revenue-'):
            # Net收入组件
            revenue_component = component.replace('revenue-', '')
            component_data[component] = generate_revenue_component_contribution(
                station_df, revenue_component, filters, region
            )

        elif component == 'all':
            # 返回所有基础组件
            component_data['numerator'] = generate_numerator_contribution(
                penetration_df, filters)
            component_data['denominator'] = generate_denominator_contribution(
                penetration_df, filters)
            component_data['service-numerator'] = generate_service_numerator_contribution(
                station_df, filters)
            component_data['power-numerator'] = generate_power_numerator_contribution(
                station_df, filters)

    else:
        # 没有指定组件，生成所有组件数据

        # 1. 分子（理想车主自建站充电量）贡献度
        component_data["numerator"] = generate_numerator_contribution(
            penetration_df, filters)

        # 2. 分母（理想车主公充电量）贡献度
        component_data["denominator"] = generate_denominator_contribution(
            penetration_df, filters)

        # 3. service-numerator（订单服务费收入）贡献度
        component_data["service-numerator"] = generate_service_numerator_contribution(
            station_df, filters)

        # 4. power-numerator（直营站充电量）贡献度
        component_data["power-numerator"] = generate_power_numerator_contribution(
            station_df, filters)

        # 5. 处理Net收入的所有子组件
        revenue_components = ['service-fee', 'card-sales', 'parking-fee',
                              'coupon-discount', 'owner-discount', 'card-discount']

        for comp in revenue_components:
            component_data[f"revenue-{comp}"] = generate_revenue_component_contribution(
                station_df, comp, filters, region
            )

    return component_data


def generate_breakdown_data(station_df: pd.DataFrame, penetration_df: pd.DataFrame) -> Dict[str, Any]:
    """生成指标拆解数据"""
    breakdown_data = {}

    # 公充渗透率拆解
    if len(penetration_df) > 0:
        numerator = penetration_df["自建站充电量"].sum()
        denominator = penetration_df["公充电量"].sum()
        value = (numerator / denominator * 100) if denominator > 0 else 0

        breakdown_data["penetration"] = {
            "value": f"{value:.1f}%",
            "numerator": f"{numerator:,.0f} 度",
            "denominator": f"{denominator:,.0f} 度"
        }
    else:
        breakdown_data["penetration"] = {
            "value": "0.0%",
            "numerator": "0 度",
            "denominator": "0 度"
        }

    # Net收入拆解
    if len(station_df) > 0:
        total_revenue = (
            station_df['订单服务费收入（扣除分成）'].sum() +
            station_df['电卡销售收入'].sum() +
            station_df['占位费收入'].sum() -
            station_df['优惠券优惠金额'].sum() -
            station_df['车主优惠金额'].sum() -
            station_df['电卡优惠金额'].sum()
        ) / 10000

        breakdown_data["revenue"] = {
            "value": f"{total_revenue:.0f}万",
            "components": [
                {
                    "label": "订单服务费收入（扣除分成）",
                    "value": f"{station_df['订单服务费收入（扣除分成）'].sum()/10000:.0f}万"
                },
                {
                    "label": "电卡销售收入",
                    "value": f"{station_df['电卡销售收入'].sum()/10000:.0f}万"
                },
                {
                    "label": "占位费收入",
                    "value": f"{station_df['占位费收入'].sum()/10000:.0f}万"
                },
                {
                    "label": "优惠券优惠金额",
                    "value": f"-{station_df['优惠券优惠金额'].sum()/10000:.0f}万"
                },
                {
                    "label": "车主优惠金额",
                    "value": f"-{station_df['车主优惠金额'].sum()/10000:.0f}万"
                },
                {
                    "label": "电卡优惠金额",
                    "value": f"-{station_df['电卡优惠金额'].sum()/10000:.0f}万"
                }
            ]
        }
    else:
        breakdown_data["revenue"] = {
            "value": "0万",
            "components": []
        }

    # 单枪日服务费收入拆解
    if len(station_df) > 0 and '枪天数' in station_df.columns:
        gun_days = station_df["枪天数"].sum()
        net_service_fee = (
            station_df['订单服务费收入（不扣除分成）'].sum() -
            station_df['车主优惠金额'].sum() -
            station_df['优惠券优惠金额'].sum() -
            station_df['电卡优惠金额'].sum()
        )

        service_value = net_service_fee / gun_days if gun_days > 0 else 0

        breakdown_data["service"] = {
            "value": f"{service_value:.0f}元",
            "numerator": f"{net_service_fee/10000:.1f}万元",
            "denominator": f"{gun_days:,.0f} 天"
        }
    else:
        breakdown_data["service"] = {
            "value": "0元",
            "numerator": "0万元",
            "denominator": "0 天"
        }

    # 单枪日电量拆解
    if len(station_df) > 0 and '枪天数' in station_df.columns:
        gun_days = station_df["枪天数"].sum()
        total_power = station_df['充电量'].sum()
        power_value = total_power / gun_days if gun_days > 0 else 0

        breakdown_data["power"] = {
            "value": f"{power_value:.0f}度",
            "numerator": f"{total_power/10000:.1f}万度",
            "denominator": f"{gun_days:,.0f} 天"
        }
    else:
        breakdown_data["power"] = {
            "value": "0度",
            "numerator": "0万度",
            "denominator": "0 天"
        }

    return breakdown_data


def generate_contribution_data(station_df: pd.DataFrame, penetration_df: pd.DataFrame,
                               filters: dict = None) -> Dict[str, Any]:
    """生成贡献度分析数据（使用加权贡献）"""
    contribution_data = {}

    # 判断是否筛选了单个省份
    is_single_province = False
    if filters and filters.get("provinces") and len(filters["provinces"]) == 1 and (not filters.get("cities") or len(filters["cities"]) == 0):
        is_single_province = True

    # 为每个指标生成贡献度数据
    for metric in ["penetration", "revenue", "service", "power"]:
        metric_contribution = {}

        if is_single_province:
            # 筛选了单个省份但未筛选城市，显示城市维度
            if metric == "penetration":
                city_contrib = generate_city_contribution(
                    station_df, penetration_df, metric)
                if city_contrib:
                    metric_contribution["city"] = city_contrib
            else:
                city_contrib = generate_city_contribution(
                    station_df, penetration_df, metric)
                if city_contrib:
                    metric_contribution["city"] = city_contrib
        else:
            # 未筛选省份或筛选了多个省份，显示省份维度（带城市展开）
            if metric == "penetration":
                geo_contrib = generate_province_contribution(
                    station_df, penetration_df, metric, include_cities=True)
                if geo_contrib:
                    metric_contribution["geography"] = geo_contrib
            else:
                geo_contrib = generate_province_contribution(
                    station_df, penetration_df, metric, include_cities=True)
                if geo_contrib:
                    metric_contribution["geography"] = geo_contrib

        # 车型贡献度（仅公充渗透率）
        if metric == "penetration" and (not filters or not filters.get("carModels")):
            car_contrib = generate_car_model_contribution(
                penetration_df, metric)
            if car_contrib:
                metric_contribution["carModel"] = car_contrib

        # 场站类型贡献度（除公充渗透率外）
        if metric in ["revenue", "service", "power"] and (not filters or not filters.get("stationTypes")):
            type_contrib = generate_station_type_contribution(
                station_df, metric)
            if type_contrib:
                metric_contribution["stationType"] = type_contrib

        # 新老站贡献度（除公充渗透率外）
        if metric in ["revenue", "service", "power"] and (not filters or not filters.get("stationAges")):
            age_contrib = generate_station_age_contribution(station_df, metric)
            if age_contrib:
                metric_contribution["stationAge"] = age_contrib

        contribution_data[metric] = metric_contribution

    return contribution_data

# 修改 generate_table_data 函数（约第7120行）


def generate_table_data(station_df: pd.DataFrame, penetration_df: pd.DataFrame,
                        current_quarter: int = None, current_year: int = None) -> Dict[str, Any]:
    """生成表格数据 - 按省份/城市聚合的月度数据"""

    # 确保列名是Excel格式
    if not station_df.empty:
        # 检查是否需要转换
        if 'province' in station_df.columns:
            station_df = convert_db_columns_to_excel_format(
                station_df, 'station')

    if not penetration_df.empty:
        # 检查是否需要转换
        if 'province' in penetration_df.columns:
            penetration_df = convert_db_columns_to_excel_format(
                penetration_df, 'penetration')

    # 获取月份列表
    months = sorted(station_df["统计日期"].unique()) if len(
        station_df) > 0 and "统计日期" in station_df.columns else []
    month_strings = [m.strftime("%Y-%m") for m in months]

    # 获取季度信息
    if not current_quarter or not current_year:
        if months:
            latest_month = months[-1]
            month_num = latest_month.month
            current_year = latest_month.year
            current_quarter = (month_num - 1) // 3 + 1

    if current_quarter and current_year:
        quarter_start_month = (current_quarter - 1) * 3 + 1
        quarter_end_month = current_quarter * 3
    else:
        quarter_start_month = None
        quarter_end_month = None

    table_data = {}

    for metric in ["penetration", "revenue", "service", "power"]:
        metric_data = {
            "months": month_strings,
            "provinces": []
        }

        # 根据指标类型选择数据源获取省份列表
        if metric == "penetration":
            provinces = sorted(penetration_df["省份"].unique()) if len(
                penetration_df) > 0 and "省份" in penetration_df.columns else []
        else:
            provinces = sorted(station_df["省份"].unique()) if len(
                station_df) > 0 and "省份" in station_df.columns else []

        for province in provinces:
            province_data = {
                "id": province,
                "name": province,
                "quarterTotal": "",
                "monthlyData": [],
                "sparklineData": [],
                "cities": []
            }

            # 获取该省份的数据
            prov_station = station_df[station_df["省份"] ==
                                      province] if "省份" in station_df.columns else pd.DataFrame()
            prov_pen = penetration_df[penetration_df["省份"] ==
                                      province] if "省份" in penetration_df.columns else pd.DataFrame()

            # 计算省份级别的月度数据
            monthly_values = []
            for month in months:
                month_station = prov_station[prov_station["统计日期"] == month] if len(
                    prov_station) > 0 else pd.DataFrame()
                month_pen = prov_pen[prov_pen["统计日期"] == month] if len(
                    prov_pen) > 0 else pd.DataFrame()

                value = calculate_metric_value(
                    metric, month_station, month_pen)
                province_data["monthlyData"].append(value["formatted"])
                monthly_values.append(value["raw"])

            province_data["sparklineData"] = monthly_values

            # 计算季度总计
            if current_quarter and current_year and quarter_start_month and quarter_end_month:
                # 筛选当前季度的数据
                quarter_station_data = prov_station[
                    (prov_station["统计日期"].dt.year == current_year) &
                    (prov_station["统计日期"].dt.month >= quarter_start_month) &
                    (prov_station["统计日期"].dt.month <= quarter_end_month)
                ] if len(prov_station) > 0 and "统计日期" in prov_station.columns else pd.DataFrame()

                quarter_pen_data = prov_pen[
                    (prov_pen["统计日期"].dt.year == current_year) &
                    (prov_pen["统计日期"].dt.month >= quarter_start_month) &
                    (prov_pen["统计日期"].dt.month <= quarter_end_month)
                ] if len(prov_pen) > 0 and "统计日期" in prov_pen.columns else pd.DataFrame()

                quarter_value = calculate_quarter_metric_value(
                    metric, quarter_station_data, quarter_pen_data)
                province_data["quarterTotal"] = quarter_value["formatted"]
            else:
                province_data["quarterTotal"] = "--"

            # 获取城市数据
            if metric == "penetration":
                cities = sorted(prov_pen["城市"].unique()) if len(
                    prov_pen) > 0 and "城市" in prov_pen.columns else []
            else:
                cities = sorted(prov_station["城市"].unique()) if len(
                    prov_station) > 0 and "城市" in prov_station.columns else []

            for city in cities:
                city_data = {
                    "id": city,
                    "name": city,
                    "quarterTotal": "",
                    "monthlyData": [],
                    "sparklineData": []
                }

                # 获取该城市的数据
                city_station = prov_station[prov_station["城市"] == city] if "城市" in prov_station.columns and len(
                    prov_station) > 0 else pd.DataFrame()
                city_pen = prov_pen[prov_pen["城市"] == city] if "城市" in prov_pen.columns and len(
                    prov_pen) > 0 else pd.DataFrame()

                # 计算城市级别的月度数据
                city_monthly_values = []
                for month in months:
                    month_city_station = city_station[city_station["统计日期"] == month] if len(
                        city_station) > 0 else pd.DataFrame()
                    month_city_pen = city_pen[city_pen["统计日期"] == month] if len(
                        city_pen) > 0 else pd.DataFrame()

                    value = calculate_metric_value(
                        metric, month_city_station, month_city_pen)
                    city_data["monthlyData"].append(value["formatted"])
                    city_monthly_values.append(value["raw"])

                city_data["sparklineData"] = city_monthly_values

                # 计算城市的季度总计
                if current_quarter and current_year and quarter_start_month and quarter_end_month:
                    # 筛选当前季度的数据
                    quarter_city_station = city_station[
                        (city_station["统计日期"].dt.year == current_year) &
                        (city_station["统计日期"].dt.month >= quarter_start_month) &
                        (city_station["统计日期"].dt.month <= quarter_end_month)
                    ] if len(city_station) > 0 and "统计日期" in city_station.columns else pd.DataFrame()

                    quarter_city_pen = city_pen[
                        (city_pen["统计日期"].dt.year == current_year) &
                        (city_pen["统计日期"].dt.month >= quarter_start_month) &
                        (city_pen["统计日期"].dt.month <= quarter_end_month)
                    ] if len(city_pen) > 0 and "统计日期" in city_pen.columns else pd.DataFrame()

                    quarter_value = calculate_quarter_metric_value(
                        metric, quarter_city_station, quarter_city_pen)
                    city_data["quarterTotal"] = quarter_value["formatted"]
                else:
                    city_data["quarterTotal"] = "--"

                province_data["cities"].append(city_data)

            metric_data["provinces"].append(province_data)

        table_data[metric] = metric_data

    return table_data


def get_filter_options(station_df: pd.DataFrame, penetration_df: pd.DataFrame) -> Dict[str, Any]:
    """获取筛选器选项"""
    # 获取车型选项
    car_models = sorted(penetration_df["车型"].unique()) if len(
        penetration_df) > 0 else []

    # 车型分组（根据实际数据判断）
    hybrid_models = []
    electric_models = []

    for model in car_models:
        if model in ["L9", "L8", "L7", "L6", "ONE"]:
            hybrid_models.append({"id": model, "name": f"理想{model}"})
        elif model in ["MEGA", "W01", "i6", "i8"]:
            electric_models.append({"id": model, "name": f"理想{model}"})
        else:
            # 默认归为增程
            hybrid_models.append({"id": model, "name": model})

    car_types = {}
    if hybrid_models:
        car_types["hybrid"] = {
            "name": "增程",
            "models": hybrid_models
        }
    if electric_models:
        car_types["electric"] = {
            "name": "纯电",
            "models": electric_models
        }

    # 获取场站类型选项
    station_types = []
    if len(station_df) > 0:
        for st in sorted(station_df["场站类型"].unique()):
            if st == "城市门店":
                station_types.append({"id": "store", "name": st})
            elif st == "城市自营":
                station_types.append({"id": "self", "name": st})
            elif st == "城市加盟":
                station_types.append({"id": "franchise", "name": st})
            else:
                station_types.append({"id": st, "name": st})

    # 获取新老站选项
    station_ages = []
    if len(station_df) > 0:
        for age in sorted(station_df["新老站"].unique()):
            if age == "新站":
                station_ages.append({"id": "new", "name": age})
            elif age == "老站":
                station_ages.append({"id": "old", "name": age})
            else:
                station_ages.append({"id": age, "name": age})

    return {
        "carTypes": car_types,
        "stationTypes": station_types,
        "stationAges": station_ages
    }

# 修改 get_province_city_data 函数（约第7420行）


def get_province_city_data(station_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """获取省份城市层级数据"""
    # 确保列名是Excel格式
    if not station_df.empty and 'province' in station_df.columns:
        station_df = convert_db_columns_to_excel_format(station_df, 'station')

    # 获取省份列表
    provinces = sorted(station_df["省份"].unique()) if len(
        station_df) > 0 and "省份" in station_df.columns else []

    province_data = []
    for province in provinces:
        # 获取该省份的城市
        cities = sorted(station_df[station_df["省份"] == province]["城市"].unique(
        )) if "城市" in station_df.columns else []

        province_data.append({
            "province": province,
            "cities": cities
        })

    return province_data


def aggregate_data_by_month(df: pd.DataFrame, data_type: str = 'station') -> pd.DataFrame:
    """月度聚合（调用aggregation_service）"""
    return aggregation_service.aggregate_by_month(df, data_type)


def aggregate_data_by_week(df: pd.DataFrame, data_type: str = 'station') -> pd.DataFrame:
    """周度聚合（调用aggregation_service）"""
    return aggregation_service.aggregate_by_week(df, data_type)


def aggregate_data_by_quarter(df: pd.DataFrame, data_type: str = 'station') -> pd.DataFrame:
    """季度聚合（调用aggregation_service）"""
    return aggregation_service.aggregate_by_quarter(df, data_type)


async def calculate_national_metrics_with_targets(version_id: int = None) -> Dict[str, Any]:
    """计算全国的各项指标（包含目标值对比）"""
    # 直接调用data_service的方法
    return await data_service.get_national_metrics(version_id)


async def generate_all_region_cards_data(current_region_key: str, version_id: int = None) -> Dict[str, List[Dict[str, Any]]]:
    """为每个指标生成所有大区的卡片数据"""
    regions = ["北区", "中区", "东一区", "东二区", "西区", "南区"]
    region_id_map = {
        "北区": "north",
        "中区": "center",
        "东一区": "east1",
        "东二区": "east2",
        "西区": "west",
        "南区": "south"
    }

    # ========== 新增：获取当前季度 ==========
    current_date = datetime.now()
    current_year = current_date.year
    current_quarter = (current_date.month - 1) // 3 + 1
    quarter_key = f"{current_year}-Q{current_quarter}"

    all_cards = {}

    for metric in ["penetration", "revenue", "service", "power"]:
        cards = []
        for region in regions:
            # ========== 修改：获取季度数据 ==========
            station_sql = f"""
                SELECT * FROM charging_aggregated_station_data
                WHERE version_id = {version_id} 
                AND region = '{region}' 
                AND aggregation_type = 'quarterly'
                AND period_key = '{quarter_key}'
            """
            station_df = await db_manager.execute_query_df(station_sql, {})

            penetration_sql = f"""
                SELECT * FROM charging_aggregated_penetration_data
                WHERE version_id = {version_id}
                AND region = '{region}'
                AND aggregation_type = 'quarterly'
                AND period_key = '{quarter_key}'
            """
            penetration_df = await db_manager.execute_query_df(penetration_sql, {})

            # 转换列名
            if not station_df.empty:
                station_df = convert_db_columns_to_excel_format(
                    station_df, 'station')
            if not penetration_df.empty:
                penetration_df = convert_db_columns_to_excel_format(
                    penetration_df, 'penetration')

            # 计算指标（现在是季度数据）
            metrics = calculate_current_metrics(station_df, penetration_df)

            # 获取目标值（保持原样）
            target_df = await data_service.get_region_data(region, 'target', version_id)
            if not target_df.empty:
                target_df = convert_db_columns_to_excel_format(
                    target_df, 'target')

            target = get_target_value(
                region, metric, f"Q{current_quarter}", target_df)  # 使用当前季度

            # 计算达成率
            if metric == "penetration":
                target_str = f"{target:.1f}%"
                actual_value = metrics.get(metric, {}).get("value", "0.0%")
                actual_num = float(actual_value.replace('%', ''))
                achievement_rate = (actual_num / target *
                                    100) if target > 0 else 0
            elif metric == "revenue":
                target_str = f"{target:,.0f}万"
                actual_value = metrics.get(metric, {}).get("value", "0万")
                actual_num = float(actual_value.replace(
                    '万', '').replace(',', ''))
                achievement_rate = (actual_num / target *
                                    100) if target > 0 else 0
            elif metric == "service":
                target_str = f"{target:.0f}元"
                actual_value = metrics.get(metric, {}).get("value", "0元")
                actual_num = float(actual_value.replace('元', ''))
                achievement_rate = (actual_num / target *
                                    100) if target > 0 else 0
            else:  # power
                target_str = f"{target:.0f}度"
                actual_value = metrics.get(metric, {}).get("value", "0度")
                actual_num = float(actual_value.replace('度', ''))
                achievement_rate = (actual_num / target *
                                    100) if target > 0 else 0

            card = {
                "id": region_id_map[region],
                "name": region,
                "target": target_str,
                "actual": actual_value,
                "achievementRate": f"{min(100, achievement_rate):.2f}"
            }
            cards.append(card)

        all_cards[metric] = cards

    return all_cards
# 修改 convert_db_columns_to_excel_format 函数（约第8580行）


def convert_db_columns_to_excel_format(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """将数据库列名转换为Excel格式的列名"""
    if df.empty:
        return df

    df = df.copy()

    if data_type == 'station':
        column_map = {
            'stat_date': '统计日期',
            'station_id': '场站ID',
            'region': '区域',
            'province': '省份',
            'city': '城市',
            'station_type': '场站类型',
            'station_age': '新老站',
            'gun_count': '场站枪数',
            'total_guns': '场站枪数',
            'station_count': '场站数',
            'gun_days': '枪天数',
            'service_fee_after_share': '订单服务费收入（扣除分成）',
            'card_sales_revenue': '电卡销售收入',
            'parking_fee_revenue': '占位费收入',
            'coupon_discount': '优惠券优惠金额',
            'owner_discount': '车主优惠金额',
            'card_discount': '电卡优惠金额',
            'charging_volume': '充电量',
            'service_fee_before_share': '订单服务费收入（不扣除分成）',
            # 聚合数据特有列
            'period_key': '周期键',
            'aggregation_type': '聚合类型'
        }
    elif data_type == 'penetration':
        column_map = {
            'stat_date': '统计日期',
            'region': '区域',
            'province': '省份',
            'city': '城市',
            'car_model': '车型',
            'self_charge_volume': '自建站充电量',
            'public_charge_volume': '公充电量',
            # 聚合数据特有列
            'period_key': '周期键',
            'aggregation_type': '聚合类型'
        }
    elif data_type == 'target':
        column_map = {
            'region': '区域',
            'metric_name': '指标',
            'target_period': '目标周期',
            'target_value': '目标值'
        }
    else:
        return df

    # 只重命名存在的列
    existing_cols = {k: v for k, v in column_map.items() if k in df.columns}
    if existing_cols:
        df = df.rename(columns=existing_cols)

    # 如果有period_key，添加年周或年季列
    if 'period_key' in df.columns and 'aggregation_type' in df.columns and not df.empty:
        agg_type = df['aggregation_type'].iloc[0]
        if agg_type == 'weekly':
            df['年周'] = df['period_key']
        elif agg_type == 'quarterly':
            df['年季'] = df['period_key']
    elif '周期键' in df.columns and '聚合类型' in df.columns and not df.empty:
        agg_type = df['聚合类型'].iloc[0]
        if agg_type == 'weekly':
            df['年周'] = df['周期键']
        elif agg_type == 'quarterly':
            df['年季'] = df['周期键']

    return df


def get_region_name(region: str) -> str:
    """获取区域名称映射"""
    region_map = {
        "east1": "东一区",
        "east2": "东二区",
        "north": "北区",
        "south": "南区",
        "west": "西区",
        "center": "中区",
        "national": "全国"
    }
    return region_map.get(region, region)


async def apply_filters(region: str, filters: dict, version_id: int = None) -> Dict[str, Any]:
    """应用筛选条件并返回筛选后的完整数据结构"""
    # 获取数据
    station_df = await data_service.get_region_data(region, 'monthly', version_id)
    penetration_df = await data_service.get_region_data(region, 'penetration_monthly', version_id)

    # 应用筛选
    if filters:
        station_df = data_service.apply_filters(station_df, filters, 'station')
        penetration_df = data_service.apply_filters(
            penetration_df, filters, 'penetration')

    # 转换列名
    station_df = convert_db_columns_to_excel_format(station_df, 'station')
    penetration_df = convert_db_columns_to_excel_format(
        penetration_df, 'penetration')

    # 计算筛选后的各项数据
    metrics = calculate_current_metrics(station_df, penetration_df)
    monthly_data = generate_monthly_data(station_df, penetration_df)
    weekly_data = await generate_weekly_data_async(region, filters, version_id)
    table_data = generate_table_data(station_df, penetration_df)
    breakdown_data = generate_breakdown_data(station_df, penetration_df)
    contribution_data = generate_contribution_data(
        station_df, penetration_df, filters)

    return {
        "metrics": metrics,
        "monthlyData": monthly_data,
        "weeklyData": weekly_data,
        "tableData": table_data,
        "breakdownData": breakdown_data,
        "contributionData": contribution_data
    }


async def generate_weekly_data_async(region: str, filters: dict = None, version_id: int = None) -> Dict[str, List[Dict]]:
    """异步版本的周度数据生成"""
    # 获取周度数据
    station_df = await data_service.get_region_data(region, 'weekly', version_id)
    penetration_df = await data_service.get_region_data(region, 'penetration_weekly', version_id)

    # 获取全国数据用于对比
    national_station_df = None
    national_pen_df = None
    if region != "全国":
        # 获取所有区域的周度数据作为全国数据
        all_regions = ["北区", "中区", "东一区", "东二区", "西区", "南区"]
        national_station_list = []
        national_pen_list = []

        for r in all_regions:
            r_station = await data_service.get_region_data(r, 'weekly', version_id)
            r_pen = await data_service.get_region_data(r, 'penetration_weekly', version_id)
            if not r_station.empty:
                national_station_list.append(r_station)
            if not r_pen.empty:
                national_pen_list.append(r_pen)

        if national_station_list:
            national_station_df = pd.concat(
                national_station_list, ignore_index=True)
        if national_pen_list:
            national_pen_df = pd.concat(national_pen_list, ignore_index=True)

    # 应用筛选
    if filters:
        station_df = data_service.apply_filters(station_df, filters, 'station')
        penetration_df = data_service.apply_filters(
            penetration_df, filters, 'penetration')

        # 全国数据也应用筛选（除了地理位置）
        if national_station_df is not None:
            national_filters = {k: v for k, v in filters.items() if k not in [
                'provinces', 'cities']}
            if national_filters:
                national_station_df = data_service.apply_filters(
                    national_station_df, national_filters, 'station')
                national_pen_df = data_service.apply_filters(
                    national_pen_df, national_filters, 'penetration')

    # 转换列名
    station_df = convert_db_columns_to_excel_format(station_df, 'station')
    penetration_df = convert_db_columns_to_excel_format(
        penetration_df, 'penetration')
    if national_station_df is not None:
        national_station_df = convert_db_columns_to_excel_format(
            national_station_df, 'station')
    if national_pen_df is not None:
        national_pen_df = convert_db_columns_to_excel_format(
            national_pen_df, 'penetration')

    # 生成周度数据
    return generate_weekly_data(station_df, penetration_df, national_station_df, national_pen_df)


async def generate_monthly_data_async(region: str, filters: dict = None, version_id: int = None) -> Dict[str, List[Dict]]:
    """异步版本的月度数据生成"""
    # 获取月度数据
    station_df = await data_service.get_region_data(region, 'monthly', version_id)
    penetration_df = await data_service.get_region_data(region, 'penetration_monthly', version_id)

    # 获取全国数据
    national_station_df = None
    national_pen_df = None
    if region != "全国":
        all_regions = ["北区", "中区", "东一区", "东二区", "西区", "南区"]
        national_station_list = []
        national_pen_list = []

        for r in all_regions:
            r_station = await data_service.get_region_data(r, 'monthly', version_id)
            r_pen = await data_service.get_region_data(r, 'penetration_monthly', version_id)
            if not r_station.empty:
                national_station_list.append(r_station)
            if not r_pen.empty:
                national_pen_list.append(r_pen)

        if national_station_list:
            national_station_df = pd.concat(
                national_station_list, ignore_index=True)
        if national_pen_list:
            national_pen_df = pd.concat(national_pen_list, ignore_index=True)

    # 应用筛选
    if filters:
        station_df = data_service.apply_filters(station_df, filters, 'station')
        penetration_df = data_service.apply_filters(
            penetration_df, filters, 'penetration')

        if national_station_df is not None:
            national_filters = {k: v for k, v in filters.items() if k not in [
                'provinces', 'cities']}
            if national_filters:
                national_station_df = data_service.apply_filters(
                    national_station_df, national_filters, 'station')
                national_pen_df = data_service.apply_filters(
                    national_pen_df, national_filters, 'penetration')

    # 转换列名
    station_df = convert_db_columns_to_excel_format(station_df, 'station')
    penetration_df = convert_db_columns_to_excel_format(
        penetration_df, 'penetration')
    if national_station_df is not None:
        national_station_df = convert_db_columns_to_excel_format(
            national_station_df, 'station')
    if national_pen_df is not None:
        national_pen_df = convert_db_columns_to_excel_format(
            national_pen_df, 'penetration')

    # 生成月度数据
    return generate_monthly_data(station_df, penetration_df, national_station_df, national_pen_df)


def calculate_metrics_for_period(station_df: pd.DataFrame, penetration_df: pd.DataFrame) -> Dict[str, Any]:
    """计算特定时期的指标（与calculate_current_metrics完全相同的逻辑）"""
    metrics = {}

    # 获取最新时期的数据（如果是聚合数据，应该只有一个时期）
    if len(station_df) > 0:
        if '统计日期' in station_df.columns:
            latest_date = station_df["统计日期"].max()
            current_station = station_df[station_df["统计日期"] == latest_date]
        else:
            current_station = station_df
    else:
        current_station = pd.DataFrame()

    if len(penetration_df) > 0:
        if '统计日期' in penetration_df.columns:
            latest_date = penetration_df["统计日期"].max()
            current_pen = penetration_df[penetration_df["统计日期"] == latest_date]
        else:
            current_pen = penetration_df
    else:
        current_pen = pd.DataFrame()

    # 1. 公充渗透率
    if len(current_pen) > 0:
        total_self_charge = current_pen["自建站充电量"].sum(
        ) if "自建站充电量" in current_pen.columns else current_pen["self_charge_volume"].sum()
        total_public_charge = current_pen["公充电量"].sum(
        ) if "公充电量" in current_pen.columns else current_pen["public_charge_volume"].sum()
        penetration_rate = (
            total_self_charge / total_public_charge * 100) if total_public_charge > 0 else 0
        metrics["penetration"] = {
            "value": f"{penetration_rate:.1f}%",
            "numerator": float(total_self_charge),
            "denominator": float(total_public_charge)
        }
    else:
        metrics["penetration"] = {"value": "0.0%",
                                  "numerator": 0, "denominator": 0}

    # 2. Net收入（单位：万元）
    if len(current_station) > 0:
        # 处理列名兼容性
        service_after = current_station["订单服务费收入（扣除分成）"].sum(
        ) if "订单服务费收入（扣除分成）" in current_station.columns else current_station["service_fee_after_share"].sum()
        card_sales = current_station["电卡销售收入"].sum(
        ) if "电卡销售收入" in current_station.columns else current_station["card_sales_revenue"].sum()
        parking = current_station["占位费收入"].sum(
        ) if "占位费收入" in current_station.columns else current_station["parking_fee_revenue"].sum()
        coupon = current_station["优惠券优惠金额"].sum(
        ) if "优惠券优惠金额" in current_station.columns else current_station["coupon_discount"].sum()
        owner = current_station["车主优惠金额"].sum(
        ) if "车主优惠金额" in current_station.columns else current_station["owner_discount"].sum()
        card_disc = current_station["电卡优惠金额"].sum(
        ) if "电卡优惠金额" in current_station.columns else current_station["card_discount"].sum()

        revenue = (service_after + card_sales + parking -
                   coupon - owner - card_disc) / 10000
        metrics["revenue"] = {
            "value": f"{revenue:.0f}万", "amount": float(revenue)}
    else:
        metrics["revenue"] = {"value": "0万", "amount": 0}

    # 3. 单枪日服务费收入（使用枪天数）
    if len(current_station) > 0:
        gun_days_col = "枪天数" if "枪天数" in current_station.columns else "gun_days"
        if gun_days_col in current_station.columns:
            total_gun_days = current_station[gun_days_col].sum()
            if total_gun_days > 0:
                service_before = current_station["订单服务费收入（不扣除分成）"].sum(
                ) if "订单服务费收入（不扣除分成）" in current_station.columns else current_station["service_fee_before_share"].sum()
                owner = current_station["车主优惠金额"].sum(
                ) if "车主优惠金额" in current_station.columns else current_station["owner_discount"].sum()
                coupon = current_station["优惠券优惠金额"].sum(
                ) if "优惠券优惠金额" in current_station.columns else current_station["coupon_discount"].sum()
                card_disc = current_station["电卡优惠金额"].sum(
                ) if "电卡优惠金额" in current_station.columns else current_station["card_discount"].sum()

                net_service_fee = service_before - owner - coupon - card_disc
                service_fee = net_service_fee / total_gun_days
                metrics["service"] = {
                    "value": f"{service_fee:.0f}元", "amount": float(service_fee)}
            else:
                metrics["service"] = {"value": "0元", "amount": 0}
        else:
            metrics["service"] = {"value": "0元", "amount": 0}
    else:
        metrics["service"] = {"value": "0元", "amount": 0}

    # 4. 单枪日电量（使用枪天数）
    if len(current_station) > 0:
        gun_days_col = "枪天数" if "枪天数" in current_station.columns else "gun_days"
        if gun_days_col in current_station.columns:
            total_gun_days = current_station[gun_days_col].sum()
            if total_gun_days > 0:
                charging_vol = current_station["充电量"].sum(
                ) if "充电量" in current_station.columns else current_station["charging_volume"].sum()
                power = charging_vol / total_gun_days
                metrics["power"] = {
                    "value": f"{power:.0f}度", "amount": float(power)}
            else:
                metrics["power"] = {"value": "0度", "amount": 0}
        else:
            metrics["power"] = {"value": "0度", "amount": 0}
    else:
        metrics["power"] = {"value": "0度", "amount": 0}

    return metrics


def calculate_breakdown_for_period(station_df: pd.DataFrame, penetration_df: pd.DataFrame) -> Dict[str, Any]:
    """计算特定时期的拆解数据（与generate_breakdown_data完全相同）"""
    breakdown_data = {}

    # 公充渗透率拆解
    if len(penetration_df) > 0:
        numerator = penetration_df["自建站充电量"].sum(
        ) if "自建站充电量" in penetration_df.columns else penetration_df["self_charge_volume"].sum()
        denominator = penetration_df["公充电量"].sum(
        ) if "公充电量" in penetration_df.columns else penetration_df["public_charge_volume"].sum()
        value = (numerator / denominator * 100) if denominator > 0 else 0

        breakdown_data["penetration"] = {
            "value": f"{value:.1f}%",
            "numerator": f"{numerator:,.0f} 度",
            "denominator": f"{denominator:,.0f} 度"
        }
    else:
        breakdown_data["penetration"] = {
            "value": "0.0%",
            "numerator": "0 度",
            "denominator": "0 度"
        }

    # Net收入拆解
    if len(station_df) > 0:
        # 处理列名兼容性
        service_after = station_df['订单服务费收入（扣除分成）'].sum(
        ) if '订单服务费收入（扣除分成）' in station_df.columns else station_df['service_fee_after_share'].sum()
        card_sales = station_df['电卡销售收入'].sum(
        ) if '电卡销售收入' in station_df.columns else station_df['card_sales_revenue'].sum()
        parking = station_df['占位费收入'].sum(
        ) if '占位费收入' in station_df.columns else station_df['parking_fee_revenue'].sum()
        coupon = station_df['优惠券优惠金额'].sum(
        ) if '优惠券优惠金额' in station_df.columns else station_df['coupon_discount'].sum()
        owner = station_df['车主优惠金额'].sum(
        ) if '车主优惠金额' in station_df.columns else station_df['owner_discount'].sum()
        card_disc = station_df['电卡优惠金额'].sum(
        ) if '电卡优惠金额' in station_df.columns else station_df['card_discount'].sum()

        total_revenue = (service_after + card_sales +
                         parking - coupon - owner - card_disc) / 10000

        breakdown_data["revenue"] = {
            "value": f"{total_revenue:.0f}万",
            "components": [
                {
                    "label": "订单服务费收入（扣除分成）",
                    "value": f"{service_after/10000:.0f}万"
                },
                {
                    "label": "电卡销售收入",
                    "value": f"{card_sales/10000:.0f}万"
                },
                {
                    "label": "占位费收入",
                    "value": f"{parking/10000:.0f}万"
                },
                {
                    "label": "优惠券优惠金额",
                    "value": f"-{coupon/10000:.0f}万"
                },
                {
                    "label": "车主优惠金额",
                    "value": f"-{owner/10000:.0f}万"
                },
                {
                    "label": "电卡优惠金额",
                    "value": f"-{card_disc/10000:.0f}万"
                }
            ]
        }
    else:
        breakdown_data["revenue"] = {
            "value": "0万",
            "components": []
        }

    # 单枪日服务费收入拆解
    if len(station_df) > 0:
        gun_days_col = '枪天数' if '枪天数' in station_df.columns else 'gun_days'
        if gun_days_col in station_df.columns:
            gun_days = station_df[gun_days_col].sum()

            service_before = station_df['订单服务费收入（不扣除分成）'].sum(
            ) if '订单服务费收入（不扣除分成）' in station_df.columns else station_df['service_fee_before_share'].sum()
            owner = station_df['车主优惠金额'].sum(
            ) if '车主优惠金额' in station_df.columns else station_df['owner_discount'].sum()
            coupon = station_df['优惠券优惠金额'].sum(
            ) if '优惠券优惠金额' in station_df.columns else station_df['coupon_discount'].sum()
            card_disc = station_df['电卡优惠金额'].sum(
            ) if '电卡优惠金额' in station_df.columns else station_df['card_discount'].sum()

            net_service_fee = service_before - owner - coupon - card_disc
            service_value = net_service_fee / gun_days if gun_days > 0 else 0

            breakdown_data["service"] = {
                "value": f"{service_value:.0f}元",
                "numerator": f"{net_service_fee/10000:.1f}万元",
                "denominator": f"{gun_days:,.0f} 天"
            }
        else:
            breakdown_data["service"] = {
                "value": "0元",
                "numerator": "0万元",
                "denominator": "0 天"
            }
    else:
        breakdown_data["service"] = {
            "value": "0元",
            "numerator": "0万元",
            "denominator": "0 天"
        }

    # 单枪日电量拆解
    if len(station_df) > 0:
        gun_days_col = '枪天数' if '枪天数' in station_df.columns else 'gun_days'
        if gun_days_col in station_df.columns:
            gun_days = station_df[gun_days_col].sum()
            charging_vol = station_df['充电量'].sum(
            ) if '充电量' in station_df.columns else station_df['charging_volume'].sum()
            power_value = charging_vol / gun_days if gun_days > 0 else 0

            breakdown_data["power"] = {
                "value": f"{power_value:.0f}度",
                "numerator": f"{charging_vol/10000:.1f}万度",
                "denominator": f"{gun_days:,.0f} 天"
            }
        else:
            breakdown_data["power"] = {
                "value": "0度",
                "numerator": "0万度",
                "denominator": "0 天"
            }
    else:
        breakdown_data["power"] = {
            "value": "0度",
            "numerator": "0万度",
            "denominator": "0 天"
        }

    return breakdown_data


def calculate_contribution_for_period(station_df: pd.DataFrame, penetration_df: pd.DataFrame,
                                      filters: dict = None) -> Dict[str, Any]:
    """计算特定时期的贡献度数据（复用generate_contribution_data）"""
    # 转换列名以确保兼容
    if not station_df.empty:
        station_df = station_df.copy()
        # 如果是数据库格式的列名，转换为Excel格式
        if 'service_fee_after_share' in station_df.columns:
            station_df = convert_db_columns_to_excel_format(
                station_df, 'station')

    if not penetration_df.empty:
        penetration_df = penetration_df.copy()
        # 如果是数据库格式的列名，转换为Excel格式
        if 'self_charge_volume' in penetration_df.columns:
            penetration_df = convert_db_columns_to_excel_format(
                penetration_df, 'penetration')

    return generate_contribution_data(station_df, penetration_df, filters)


def calculate_quarter_breakdown(metric: str, station_df: pd.DataFrame, penetration_df: pd.DataFrame) -> Dict[str, Any]:
    """计算季度拆解数据"""
    breakdown_data = {}

    if metric == "penetration":
        # 公充渗透率拆解
        if len(penetration_df) > 0:
            total_self = penetration_df["自建站充电量"].sum(
            ) if "自建站充电量" in penetration_df.columns else penetration_df["self_charge_volume"].sum()
            total_public = penetration_df["公充电量"].sum(
            ) if "公充电量" in penetration_df.columns else penetration_df["public_charge_volume"].sum()

            breakdown_data = {
                "numerator": f"{total_self:,.0f} 度",
                "denominator": f"{total_public:,.0f} 度"
            }
        else:
            breakdown_data = {
                "numerator": "0 度",
                "denominator": "0 度"
            }

    elif metric == "service":
        # 单枪日服务费收入拆解
        gun_days_col = "枪天数" if "枪天数" in station_df.columns else "gun_days"
        if len(station_df) > 0 and gun_days_col in station_df.columns:
            total_gun_days = station_df[gun_days_col].sum()

            service_before = station_df["订单服务费收入（不扣除分成）"].sum(
            ) if "订单服务费收入（不扣除分成）" in station_df.columns else station_df["service_fee_before_share"].sum()
            owner = station_df["车主优惠金额"].sum(
            ) if "车主优惠金额" in station_df.columns else station_df["owner_discount"].sum()
            coupon = station_df["优惠券优惠金额"].sum(
            ) if "优惠券优惠金额" in station_df.columns else station_df["coupon_discount"].sum()
            card = station_df["电卡优惠金额"].sum(
            ) if "电卡优惠金额" in station_df.columns else station_df["card_discount"].sum()

            net_service_fee = service_before - owner - coupon - card

            breakdown_data = {
                "numerator": f"{net_service_fee/10000:.1f}万元",
                "denominator": f"{total_gun_days:,.0f} 天"
            }
        else:
            breakdown_data = {
                "numerator": "0万元",
                "denominator": "0 天"
            }

    elif metric == "power":
        # 单枪日电量拆解
        gun_days_col = "枪天数" if "枪天数" in station_df.columns else "gun_days"
        if len(station_df) > 0 and gun_days_col in station_df.columns:
            total_gun_days = station_df[gun_days_col].sum()
            total_power = station_df["充电量"].sum(
            ) if "充电量" in station_df.columns else station_df["charging_volume"].sum()

            breakdown_data = {
                "numerator": f"{total_power/10000:.1f}万度",
                "denominator": f"{total_gun_days:,.0f} 天"
            }
        else:
            breakdown_data = {
                "numerator": "0万度",
                "denominator": "0 天"
            }

    elif metric == "revenue":
        # Net收入组件
        if len(station_df) > 0:
            service_after = station_df['订单服务费收入（扣除分成）'].sum(
            ) if '订单服务费收入（扣除分成）' in station_df.columns else station_df['service_fee_after_share'].sum()
            card_sales = station_df['电卡销售收入'].sum(
            ) if '电卡销售收入' in station_df.columns else station_df['card_sales_revenue'].sum()
            parking = station_df['占位费收入'].sum(
            ) if '占位费收入' in station_df.columns else station_df['parking_fee_revenue'].sum()
            coupon = station_df['优惠券优惠金额'].sum(
            ) if '优惠券优惠金额' in station_df.columns else station_df['coupon_discount'].sum()
            owner = station_df['车主优惠金额'].sum(
            ) if '车主优惠金额' in station_df.columns else station_df['owner_discount'].sum()
            card_disc = station_df['电卡优惠金额'].sum(
            ) if '电卡优惠金额' in station_df.columns else station_df['card_discount'].sum()

            components = [
                {
                    "label": "订单服务费收入（扣除分成）",
                    "value": f"{service_after/10000:.0f}万"
                },
                {
                    "label": "电卡销售收入",
                    "value": f"{card_sales/10000:.0f}万"
                },
                {
                    "label": "占位费收入",
                    "value": f"{parking/10000:.0f}万"
                },
                {
                    "label": "优惠券优惠金额",
                    "value": f"-{coupon/10000:.0f}万"
                },
                {
                    "label": "车主优惠金额",
                    "value": f"-{owner/10000:.0f}万"
                },
                {
                    "label": "电卡优惠金额",
                    "value": f"-{card_disc/10000:.0f}万"
                }
            ]
            breakdown_data = {"components": components}
        else:
            breakdown_data = {"components": []}

    return breakdown_data


def generate_quarter_component_data(component: str, metric: str, station_df: pd.DataFrame,
                                    penetration_df: pd.DataFrame, region_name: str,
                                    filters: dict) -> Dict[str, Any]:
    """生成季度组件数据"""
    # 确保列名是Excel格式
    if not station_df.empty and 'service_fee_after_share' in station_df.columns:
        station_df = convert_db_columns_to_excel_format(station_df, 'station')
    if not penetration_df.empty and 'self_charge_volume' in penetration_df.columns:
        penetration_df = convert_db_columns_to_excel_format(
            penetration_df, 'penetration')

    component_data = {}

    if component == 'numerator':
        # 理想车主自建站充电量（分子）
        component_data = generate_numerator_contribution(
            penetration_df, filters)

    elif component == 'denominator':
        # 理想车主公充电量（分母）
        component_data = generate_denominator_contribution(
            penetration_df, filters)

    elif component == 'service-numerator':
        # 订单服务费收入
        component_data = generate_service_numerator_contribution(
            station_df, filters)

    elif component == 'power-numerator':
        # 直营站充电量
        component_data = generate_power_numerator_contribution(
            station_df, filters)

    elif component == 'all':
        # 返回所有组件的分析数据
        all_components = {}

        if metric == 'penetration':
            all_components['numerator'] = generate_numerator_contribution(
                penetration_df, filters)
            all_components['denominator'] = generate_denominator_contribution(
                penetration_df, filters)
        elif metric == 'service':
            all_components['service-numerator'] = generate_service_numerator_contribution(
                station_df, filters)
        elif metric == 'power':
            all_components['power-numerator'] = generate_power_numerator_contribution(
                station_df, filters)
        elif metric == 'revenue':
            # 所有收入组件
            revenue_components = ['service-fee', 'card-sales', 'parking-fee',
                                  'coupon-discount', 'owner-discount', 'card-discount']
            for comp_type in revenue_components:
                all_components[f'revenue-{comp_type}'] = generate_revenue_component_contribution(
                    station_df, comp_type, filters, region_name
                )

        component_data = all_components

    return {"componentData": component_data}


if __name__ == "__main__":
    port = int(os.environ.get("SERVICE_PORT", 8080))
    uvicorn.run(app=app, host="0.0.0.0", port=port)
