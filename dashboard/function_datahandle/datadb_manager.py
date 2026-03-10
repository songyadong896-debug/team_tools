# function_datahandle/datadb_manager.py
from typing import Dict, List, Any, Callable, Optional
from datetime import datetime
import json
from abc import ABC, abstractmethod
from sqlalchemy import text

# 导入你的数据库连接
import sys
from pathlib import Path
root_path = Path(__file__).parent.parent  # 回到根目录
sys.path.append(str(root_path))

from database import AsyncSessionLocal, execute_raw_query, fetch_one, fetch_all

class DataDBWriter(ABC):
    """数据库写入器基类"""
    
    @abstractmethod
    def get_table_schema(self) -> Dict[str, str]:
        """返回表结构定义
        返回格式: {
            'main_table': 'CREATE TABLE ...',
            'history_table': 'CREATE TABLE ...'
        }
        """
        pass
    
    @abstractmethod
    def prepare_main_records(self, df: Any, version_id: str) -> List[Dict]:
        """准备主表要插入的记录"""
        pass
    
    @abstractmethod
    def prepare_history_record(self, summary: Dict, version_id: str, project_id: str) -> Dict:
        """准备历史记录表要插入的记录"""
        pass
    
    @abstractmethod
    def get_table_names(self) -> Dict[str, str]:
        """返回表名
        返回格式: {
            'main': '主表名',
            'history': '历史表名'
        }
        """
        pass


class DataDBManager:
    """统一的数据库管理器"""
    
    _writers: Dict[str, DataDBWriter] = {}
    _table_configs: Dict[str, Dict] = {}
    
    @classmethod
    def register_writer(cls, project_id: str, writer: DataDBWriter):
        """注册项目的数据库写入器"""
        cls._writers[project_id] = writer
        cls._table_configs[project_id] = writer.get_table_names()
        print(f"Registered DB writer for project: {project_id}")
    
    @classmethod
    def get_writer(cls, project_id: str) -> Optional[DataDBWriter]:
        """获取项目的写入器"""
        return cls._writers.get(project_id)
    
    @classmethod
    def get_table_schema(cls, project_id: str) -> Optional[Dict[str, str]]:
        """获取项目的表结构"""
        writer = cls.get_writer(project_id)
        return writer.get_table_schema() if writer else None
    
    @classmethod
    async def bulk_insert(cls, table_name: str, records: List[Dict]) -> int:
        """批量插入数据"""
        if not records:
            return 0
        
        # 获取字段名
        columns = list(records[0].keys())
        placeholders = ', '.join([f":{col}" for col in columns])
        columns_str = ', '.join([f"`{col}`" for col in columns])
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        async with AsyncSessionLocal() as session:
            try:
                # 批量执行
                for record in records:
                    await session.execute(text(query), record)
                await session.commit()
                return len(records)
            except Exception as e:
                await session.rollback()
                raise e
    
    @classmethod
    async def insert(cls, table_name: str, record: Dict) -> int:
        """插入单条记录"""
        columns = list(record.keys())
        placeholders = ', '.join([f":{col}" for col in columns])
        columns_str = ', '.join([f"`{col}`" for col in columns])
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        async with AsyncSessionLocal() as session:
            try:
                result = await session.execute(text(query), record)
                await session.commit()
                return result.lastrowid
            except Exception as e:
                await session.rollback()
                raise e
    
    @classmethod
    async def execute_query(cls, query: str, params: Dict = None) -> List[Dict]:
        """执行查询并返回字典列表"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(text(query), params or {})
            # 转换为字典列表
            rows = result.fetchall()
            if rows:
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]
            return []
    
    @classmethod
    async def save_project_data(cls, project_id: str, processed_data: Dict[str, Any], 
                                version_id: str) -> bool:
        """保存项目数据到数据库"""
        writer = cls.get_writer(project_id)
        if not writer:
            print(f"No writer registered for project: {project_id}")
            return False
        
        try:
            # 获取表名
            tables = writer.get_table_names()
            
            # 准备主表记录
            df = processed_data["processed_data"]
            records = writer.prepare_main_records(df, version_id)
            
            # 批量插入主表
            if records:
                inserted = await cls.bulk_insert(tables['main'], records)
                print(f"Inserted {inserted} records into {tables['main']}")
            
            # 准备并插入历史记录
            summary = processed_data["summary"]
            history_record = writer.prepare_history_record(summary, version_id, project_id)
            await cls.insert(tables['history'], history_record)
            print(f"Inserted history record into {tables['history']}")
            
            return True
            
        except Exception as e:
            print(f"Error saving data for project {project_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    @classmethod
    async def create_tables_if_not_exists(cls, project_id: str) -> bool:
        """创建项目所需的表（如果不存在）"""
        writer = cls.get_writer(project_id)
        if not writer:
            print(f"No writer registered for project: {project_id}")
            return False
        
        try:
            schemas = writer.get_table_schema()
            
            # 执行建表语句
            async with AsyncSessionLocal() as session:
                for table_type, create_sql in schemas.items():
                    await session.execute(text(create_sql))
                await session.commit()
                
            print(f"Tables created/verified for project: {project_id}")
            return True
            
        except Exception as e:
            print(f"Error creating tables for project {project_id}: {str(e)}")
            return False
    
    @classmethod
    def auto_register_writers(cls):
        """自动注册所有项目的写入器"""
        # 注册 PxxDash 项目
        try:
            from .project_writers import PxxDashDBWriter
            cls.register_writer("pxxdash", PxxDashDBWriter())
        except ImportError as e:
            print(f"Warning: PxxDashDBWriter not found - {e}")
        
        # TODO: 在这里添加其他项目的注册
        # try:
        #     from .project_writers import StationParamsDBWriter
        #     cls.register_writer("station_params", StationParamsDBWriter())
        # except ImportError:
        #     print("Warning: StationParamsDBWriter not found")
        
        print(f"DB Writers auto-registration completed. Total: {len(cls._writers)}")