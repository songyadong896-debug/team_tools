# function_datahandle/project_writers.py
from .datadb_manager import DataDBWriter
from typing import Dict, List, Any
from datetime import datetime

class PxxDashDBWriter(DataDBWriter):
    """PXX Dashboard 项目的数据库写入器"""
    
    def get_table_names(self) -> Dict[str, str]:
        return {
            'main': 'pxx_dashboard_data',
            'history': 'pxx_upload_history'
        }
    
    def get_table_schema(self) -> Dict[str, str]:
        return {
            'main_table': """
                CREATE TABLE IF NOT EXISTS pxx_dashboard_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    version_id VARCHAR(50),
                    region VARCHAR(20),
                    province VARCHAR(20),
                    city VARCHAR(50),
                    project_type VARCHAR(20),
                    bd VARCHAR(50),
                    product_type VARCHAR(20),
                    gun_count INT,
                    project_id VARCHAR(50),
                    project_name VARCHAR(100),
                    approved_model VARCHAR(100),
                    cumulative_model VARCHAR(100),
                    current_quarter_model VARCHAR(100),
                    quarter VARCHAR(10),
                    data_version VARCHAR(50),
                    upload_time DATETIME,
                    update_time DATETIME,
                    INDEX idx_version (version_id),
                    INDEX idx_project (project_id)
                )
            """,
            'history_table': """
                CREATE TABLE IF NOT EXISTS pxx_upload_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    version_id VARCHAR(50),
                    project_id VARCHAR(20),
                    total_records INT,
                    total_projects INT,
                    total_guns INT,
                    summary_json TEXT,
                    upload_time DATETIME,
                    INDEX idx_version (version_id)
                )
            """
        }
    
    def prepare_main_records(self, df: Any, version_id: str) -> List[Dict]:
        """准备主表记录"""
        records = []
        for _, row in df.iterrows():
            record = {
                'version_id': version_id,
                'region': row['大区'],
                'province': row['省份'],
                'city': row['城市'],
                'project_type': row['项目类型'],
                'bd': row['BD'],
                'product_type': row['产品类型'],
                'gun_count': int(row['枪数']),
                'project_id': str(row['项目上线id']),
                'project_name': row['项目名称'],
                'approved_model': str(row['上会财务模型']),
                'cumulative_model': str(row['累计财务模型']),
                'current_quarter_model': str(row['本季度财务模型']),
                'quarter': row['季度'],
                'data_version': row['数据版本'],
                'upload_time': row['上传时间'],
                'update_time': row['更新时间']
            }
            records.append(record)
        return records
    
    def prepare_history_record(self, summary: Dict, version_id: str, project_id: str) -> Dict:
        """准备历史记录"""
        return {
            'version_id': version_id,
            'project_id': project_id,
            'total_records': summary['total_records'],
            'total_projects': summary['total_projects'],
            'total_guns': summary['total_guns'],
            'summary_json': str(summary),
            'upload_time': datetime.fromisoformat(summary['upload_time'])
        }


# 示例：另一个项目的写入器
class StationParamsDBWriter(DataDBWriter):
    """场站参数项目的数据库写入器"""
    
    def get_table_names(self) -> Dict[str, str]:
        return {
            'main': 'station_params',
            'history': 'station_upload_history'
        }
    
    def get_table_schema(self) -> Dict[str, str]:
        return {
            'main_table': """
                CREATE TABLE IF NOT EXISTS station_params (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    version_id VARCHAR(50),
                    station_id VARCHAR(50),
                    station_name VARCHAR(100),
                    region VARCHAR(20),
                    province VARCHAR(20),
                    city VARCHAR(50),
                    bd VARCHAR(50),
                    project_type VARCHAR(20),
                    product_type VARCHAR(20),
                    approved_params FLOAT,
                    current_quarter_params FLOAT,
                    cumulative_params FLOAT,
                    quarter VARCHAR(10),
                    quarter_ratio FLOAT,
                    param_diff FLOAT,
                    upload_time DATETIME,
                    update_time DATETIME,
                    INDEX idx_version (version_id),
                    INDEX idx_station (station_id)
                )
            """,
            'history_table': """
                CREATE TABLE IF NOT EXISTS station_upload_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    version_id VARCHAR(50),
                    project_id VARCHAR(20),
                    total_records INT,
                    summary_data TEXT,
                    upload_time DATETIME,
                    INDEX idx_version (version_id)
                )
            """
        }
    
    def prepare_main_records(self, df: Any, version_id: str) -> List[Dict]:
        # 实现具体的数据准备逻辑
        pass
    
    def prepare_history_record(self, summary: Dict, version_id: str, project_id: str) -> Dict:
        # 实现具体的历史记录准备逻辑
        pass