# station_params_handler.py
import pandas as pd
import io
from typing import Dict, List, Tuple, Any
from datetime import datetime
from function_datahandle import DataUploadHandler
from database import db_manager  # 使用你现有的数据库管理器

class StationParamsHandler(DataUploadHandler):
    """场站参数数据上传处理器"""
    
    # 定义期望的列名
    EXPECTED_COLUMNS = [
        'ID', '场站名称', '大区', '省份', '城市', 'BD', 
        '项目类型', '产品类型', '过会参数', '本季度参数', 
        '累计参数', '季度'
    ]
    
    # 定义验证规则
    VALIDATION_RULES = {
        '大区': ['东一区', '东二区', '北区', '南区', '西区', '中区'],  # 允许的大区值
        '项目类型': ['自营', '加盟', '合作'],  # 示例
        '产品类型': ['快充', '慢充', '超充']   # 示例
    }
    
    async def validate_file(self, file_content: bytes, filename: str) -> Tuple[bool, List[str]]:
        """验证文件格式"""
        errors = []
        
        # 检查文件名
        if not filename.endswith(('.xlsx', '.xls')):
            errors.append("文件格式错误：只支持Excel文件")
            return False, errors
        
        # 检查文件大小
        if len(file_content) > 50 * 1024 * 1024:  # 50MB
            errors.append("文件过大：不能超过50MB")
            return False, errors
            
        return True, errors
    
    async def parse_file(self, file_content: bytes) -> Dict[str, pd.DataFrame]:
        """解析Excel文件"""
        try:
            # 读取Excel文件
            excel_file = pd.ExcelFile(io.BytesIO(file_content))
            
            # 检查是否有sheet
            if len(excel_file.sheet_names) == 0:
                raise ValueError("Excel文件中没有工作表")
            
            # 读取第一个sheet
            df = pd.read_excel(io.BytesIO(file_content), sheet_name=0)
            
            return {"main": df}  # 返回主数据
            
        except Exception as e:
            raise ValueError(f"解析Excel文件失败: {str(e)}")
    
    async def validate_data(self, data: Dict[str, pd.DataFrame]) -> Tuple[bool, List[str]]:
        """验证数据内容"""
        errors = []
        df = data.get("main")
        
        if df is None or df.empty:
            errors.append("数据表为空")
            return False, errors
        
        # 1. 检查列名
        missing_cols = set(self.EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            errors.append(f"缺少必要的列: {', '.join(missing_cols)}")
        
        extra_cols = set(df.columns) - set(self.EXPECTED_COLUMNS)
        if extra_cols:
            errors.append(f"包含未知列: {', '.join(extra_cols)}")
        
        # 2. 检查必填字段
        required_fields = ['ID', '场站名称', '大区', '省份', '城市']
        for field in required_fields:
            if field in df.columns and df[field].isna().any():
                errors.append(f"列 '{field}' 包含空值")
        
        # 3. 检查数据有效性
        if '大区' in df.columns:
            invalid_regions = df[~df['大区'].isin(self.VALIDATION_RULES['大区'])]['大区'].unique()
            if len(invalid_regions) > 0:
                errors.append(f"无效的大区值: {', '.join(invalid_regions)}")
        
        # 4. 检查数值字段
        numeric_fields = ['过会参数', '本季度参数', '累计参数']
        for field in numeric_fields:
            if field in df.columns:
                try:
                    pd.to_numeric(df[field], errors='coerce')
                    if df[field].isna().any():
                        errors.append(f"列 '{field}' 包含非数值数据")
                except:
                    errors.append(f"列 '{field}' 数据类型错误")
        
        # 5. 检查ID唯一性
        if 'ID' in df.columns and df['ID'].duplicated().any():
            duplicate_ids = df[df['ID'].duplicated()]['ID'].unique()
            errors.append(f"存在重复的ID: {', '.join(map(str, duplicate_ids))}")
        
        return len(errors) == 0, errors
    
    async def process_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """处理数据 - 这里可以做各种pandas计算"""
        df = data["main"].copy()
        
        # 1. 数据清洗
        # 去除前后空格
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].str.strip()
        
        # 2. 数据类型转换
        numeric_fields = ['过会参数', '本季度参数', '累计参数']
        for field in numeric_fields:
            df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)
        
        # 3. 计算新字段（示例）
        # 计算本季度占累计的百分比
        df['季度占比'] = (df['本季度参数'] / df['累计参数'] * 100).round(2)
        df['季度占比'] = df['季度占比'].fillna(0)
        
        # 计算与过会参数的差异
        df['参数差异'] = df['本季度参数'] - df['过会参数']
        
        # 4. 添加上传时间
        df['上传时间'] = self.upload_time
        df['更新时间'] = self.upload_time
        
        return {
            "processed_data": df,
            "summary": {
                "total_records": len(df),
                "total_stations": df['场站名称'].nunique(),
                "regions": df['大区'].unique().tolist(),
                "upload_time": self.upload_time.isoformat()
            }
        }
    
    async def save_data(self, processed_data: Dict[str, Any], version_id: str) -> bool:
        """保存数据到数据库"""
        try:
            df = processed_data["processed_data"]
            
            # 构建插入数据
            records = []
            for _, row in df.iterrows():
                record = {
                    'version_id': version_id,
                    'station_id': row['ID'],
                    'station_name': row['场站名称'],
                    'region': row['大区'],
                    'province': row['省份'],
                    'city': row['城市'],
                    'bd': row['BD'],
                    'project_type': row['项目类型'],
                    'product_type': row['产品类型'],
                    'approved_params': float(row['过会参数']),
                    'current_quarter_params': float(row['本季度参数']),
                    'cumulative_params': float(row['累计参数']),
                    'quarter': row['季度'],
                    'quarter_ratio': float(row['季度占比']),
                    'param_diff': float(row['参数差异']),
                    'upload_time': row['上传时间'],
                    'update_time': row['更新时间']
                }
                records.append(record)
            
            # 批量插入（使用你的db_manager）
            # 假设你有一个表叫 station_params
            await db_manager.bulk_insert('station_params', records)
            
            # 也可以保存汇总信息
            summary = processed_data["summary"]
            await db_manager.insert('upload_history', {
                'version_id': version_id,
                'project_id': self.project_id,
                'total_records': summary['total_records'],
                'summary_data': str(summary),
                'upload_time': self.upload_time
            })
            
            return True
            
        except Exception as e:
            print(f"保存数据失败: {str(e)}")
            return False