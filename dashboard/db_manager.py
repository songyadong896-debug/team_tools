# db_manager.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy import text, select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
import json
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager
from database import AsyncSessionLocal, async_engine
from charging_models import *

class AsyncDatabaseManager:
    def __init__(self):
        """初始化异步数据库管理器"""
        self.engine = async_engine
    
    @asynccontextmanager
    async def get_session(self):
        """获取异步会话的上下文管理器"""
        async with AsyncSessionLocal() as session:
            try:
                yield session
            finally:
                await session.close()
    async def get_version_upload_time(self, version_id: int) -> datetime:
        """获取版本的上传时间"""
        async with self.get_session() as session:
            result = await session.execute(
                text("""
                    SELECT upload_time 
                    FROM charging_data_versions 
                    WHERE id = :version_id
                """),
                {"version_id": version_id}
            )
            row = result.fetchone()
            return row.upload_time if row else datetime.now()
    
    async def execute_query(self, sql: str, params: dict = None) -> List[Dict]:
        """执行查询并返回字典列表"""
        async with self.get_session() as session:
            result = await session.execute(text(sql), params or {})
            rows = result.fetchall()
            if rows:
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]
            return []
    
    async def execute_query_df(self, sql: str, params: dict = None) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        data = await self.execute_query(sql, params)
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    async def get_latest_version_id(self) -> Optional[int]:
        """获取最新版本ID"""
        async with self.get_session() as session:
            result = await session.execute(
                select(ChargingDataVersion.id)
                .order_by(ChargingDataVersion.upload_time.desc())
                .limit(1)
            )
            row = result.scalar()
            return row if row else None
    
    async def create_new_version(self, description: str = None) -> int:
        """创建新版本"""
        async with self.get_session() as session:
            new_version = ChargingDataVersion(
                version_date=datetime.now().date(),
                upload_time=datetime.now(),
                description=description
            )
            session.add(new_version)
            await session.commit()
            await session.refresh(new_version)
            return new_version.id
    
    async def insert_station_daily_batch(self, version_id: int, df: pd.DataFrame) -> None:
        """批量插入单站日数据 - 优化版本使用原生SQL"""
        if df.empty:
            return
            
        async with self.get_session() as session:
            # 每批次处理的记录数
            batch_size = 10000
            total_rows = len(df)
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                # 构建批量插入的值列表
                values = []
                for _, row in batch_df.iterrows():
                    values.append(f"""(
                        {version_id},
                        '{row['统计日期']}',
                        '{str(row['场站ID']).replace("'", "''")}',
                        '{row['区域'].replace("'", "''")}',
                        '{row['省份'].replace("'", "''")}',
                        '{row['城市'].replace("'", "''")}',
                        {f"'{row['场站类型'].replace(chr(39), chr(39)*2)}'" if pd.notna(row.get('场站类型')) else 'NULL'},
                        {f"'{row['新老站'].replace(chr(39), chr(39)*2)}'" if pd.notna(row.get('新老站')) else 'NULL'},
                        {int(row.get('场站枪数', 0))},
                        {float(row.get('订单服务费收入（扣除分成）', 0))},
                        {float(row.get('电卡销售收入', 0))},
                        {float(row.get('占位费收入', 0))},
                        {float(row.get('优惠券优惠金额', 0))},
                        {float(row.get('车主优惠金额', 0))},
                        {float(row.get('电卡优惠金额', 0))},
                        {float(row.get('充电量', 0))},
                        {float(row.get('订单服务费收入（不扣除分成）', 0))}
                    )""")
                
                # 构建完整的INSERT语句
                insert_sql = f"""
                    INSERT INTO charging_station_data_daily 
                    (version_id, stat_date, station_id, region, province, city, 
                     station_type, station_age, gun_count, service_fee_after_share,
                     card_sales_revenue, parking_fee_revenue, coupon_discount,
                     owner_discount, card_discount, charging_volume, service_fee_before_share)
                    VALUES {','.join(values)}
                """
                
                # 执行批量插入
                await session.execute(text(insert_sql))
                await session.commit()
                
                print(f"已插入 {end_idx}/{total_rows} 条站点数据")

    async def insert_penetration_daily_batch(self, version_id: int, df: pd.DataFrame) -> None:
        """批量插入渗透率日数据 - 优化版本使用原生SQL"""
        if df.empty:
            return
            
        async with self.get_session() as session:
            batch_size = 5000
            total_rows = len(df)
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                values = []
                for _, row in batch_df.iterrows():
                    values.append(f"""(
                        {version_id},
                        '{row['统计日期']}',
                        '{row['区域'].replace("'", "''")}',
                        '{row['省份'].replace("'", "''")}',
                        '{row['城市'].replace("'", "''")}',
                        '{row['车型'].replace("'", "''")}',
                        {float(row.get('自建站充电量', 0))},
                        {float(row.get('公充电量', 0))}
                    )""")
                
                insert_sql = f"""
                    INSERT INTO charging_penetration_data_daily 
                    (version_id, stat_date, region, province, city, car_model,
                     self_charge_volume, public_charge_volume)
                    VALUES {','.join(values)}
                """
                
                await session.execute(text(insert_sql))
                await session.commit()
                
                print(f"已插入 {end_idx}/{total_rows} 条渗透率数据")

    async def insert_target_batch(self, version_id: int, df: pd.DataFrame) -> None:
        """批量插入目标数据"""
        async with self.get_session() as session:
            records = []
            for _, row in df.iterrows():
                target_val = row['目标']
                if isinstance(target_val, str) and target_val.endswith('%'):
                    target_val = float(target_val.replace('%', ''))
                else:
                    target_val = float(target_val)
                
                record = ChargingTarget(
                    version_id=version_id,
                    region=row['区域'],
                    metric_name=row['指标'],
                    target_period=row['目标周期'],
                    target_value=target_val
                )
                records.append(record)
            
            if records:
                session.add_all(records)
                await session.commit()
    
    async def insert_aggregated_station_data(self, version_id: int, df: pd.DataFrame, 
                                            aggregation_type: str, period_key: str = None) -> None:
        """插入预聚合站点数据 - 优化版本"""
        if df.empty:
            return
            
        async with self.get_session() as session:
            batch_size = 5000
            total_rows = len(df)
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                values = []
                for _, row in batch_df.iterrows():
                    pk = period_key if period_key else row.get('年周', row.get('年季', ''))
                    values.append(f"""(
                        {version_id},
                        '{aggregation_type}',
                        '{pk}',
                        '{row['统计日期']}',
                        '{row['区域'].replace("'", "''")}',
                        '{row['省份'].replace("'", "''")}',
                        '{row['城市'].replace("'", "''")}',
                        {f"'{row['场站类型'].replace(chr(39), chr(39)*2)}'" if pd.notna(row.get('场站类型')) else 'NULL'},
                        {f"'{row['新老站'].replace(chr(39), chr(39)*2)}'" if pd.notna(row.get('新老站')) else 'NULL'},
                        {int(row.get('场站数', 0))},
                        {int(row.get('场站枪数', 0))},
                        {float(row.get('枪天数', 0))},
                        {float(row.get('订单服务费收入（扣除分成）', 0))},
                        {float(row.get('电卡销售收入', 0))},
                        {float(row.get('占位费收入', 0))},
                        {float(row.get('优惠券优惠金额', 0))},
                        {float(row.get('车主优惠金额', 0))},
                        {float(row.get('电卡优惠金额', 0))},
                        {float(row.get('充电量', 0))},
                        {float(row.get('订单服务费收入（不扣除分成）', 0))}
                    )""")
                
                insert_sql = f"""
                    INSERT INTO charging_aggregated_station_data 
                    (version_id, aggregation_type, period_key, stat_date, region, province, city,
                     station_type, station_age, station_count, total_guns, gun_days,
                     service_fee_after_share, card_sales_revenue, parking_fee_revenue,
                     coupon_discount, owner_discount, card_discount, charging_volume,
                     service_fee_before_share)
                    VALUES {','.join(values)}
                """
                
                await session.execute(text(insert_sql))
                await session.commit()

    async def insert_aggregated_penetration_data(self, version_id: int, df: pd.DataFrame,
                                                aggregation_type: str, period_key: str = None) -> None:
        """插入预聚合渗透率数据 - 优化版本"""
        if df.empty:
            return
            
        async with self.get_session() as session:
            batch_size = 5000
            total_rows = len(df)
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                values = []
                for _, row in batch_df.iterrows():
                    pk = period_key if period_key else row.get('年周', row.get('年季', ''))
                    values.append(f"""(
                        {version_id},
                        '{aggregation_type}',
                        '{pk}',
                        '{row['统计日期']}',
                        '{row['区域'].replace("'", "''")}',
                        '{row['省份'].replace("'", "''")}',
                        '{row['城市'].replace("'", "''")}',
                        '{row['车型'].replace("'", "''")}',
                        {float(row.get('自建站充电量', 0))},
                        {float(row.get('公充电量', 0))}
                    )""")
                
                insert_sql = f"""
                    INSERT INTO charging_aggregated_penetration_data 
                    (version_id, aggregation_type, period_key, stat_date, region, province, city,
                     car_model, self_charge_volume, public_charge_volume)
                    VALUES {','.join(values)}
                """
                
                await session.execute(text(insert_sql))
                await session.commit()

    async def update_filter_cache(self, version_id: int, region: str, option_type: str, values: List) -> None:
        """更新筛选选项缓存"""
        async with self.get_session() as session:
            # 查找现有记录
            result = await session.execute(
                select(ChargingFilterCache).where(
                    and_(
                        ChargingFilterCache.version_id == version_id,
                        ChargingFilterCache.region == region,
                        ChargingFilterCache.option_type == option_type
                    )
                )
            )
            existing = result.scalar()
            
            if existing:
                # 更新现有记录
                existing.option_values = values
                existing.updated_at = datetime.utcnow()
            else:
                # 创建新记录
                new_cache = ChargingFilterCache(
                    version_id=version_id,
                    region=region,
                    option_type=option_type,
                    option_values=values
                )
                session.add(new_cache)
            
            await session.commit()
    
    async def get_filter_options(self, version_id: int, region: str) -> Dict:
        """获取筛选选项"""
        async with self.get_session() as session:
            result = await session.execute(
                select(ChargingFilterCache).where(
                    and_(
                        ChargingFilterCache.version_id == version_id,
                        ChargingFilterCache.region == region
                    )
                )
            )
            caches = result.scalars().all()
            
            options = {}
            for cache in caches:
                options[cache.option_type] = cache.option_values
            
            return options

# 创建全局数据库管理器实例
db_manager = AsyncDatabaseManager()