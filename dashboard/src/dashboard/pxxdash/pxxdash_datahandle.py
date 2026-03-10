# src/dashboard/pxxdash/pxxdash_datahandle.py

# 修改导入路径（因为文件不在根目录）
from function_datahandle import DataUploadHandler, DataDBManager
import pandas as pd
import io
from typing import Dict, List, Tuple, Any
from datetime import datetime

# 调整导入路径 - 从根目录导入
import sys
from pathlib import Path
# 添加根目录到Python路径
root_path = Path(__file__).parent.parent.parent.parent  # 回到根目录
sys.path.append(str(root_path))


class PxxDashHandler(DataUploadHandler):
    """PXX Dashboard财务模型数据上传处理器"""

    # 定义期望的列名
    EXPECTED_COLUMNS = [
        '大区', '省份', '城市', '项目类型', 'BD', '产品类型',
        '枪数', '项目上线id', '项目名称', '上会财务模型',
        '累计财务模型', '本季度财务模型', '季度'
    ]

    # 定义验证规则
    VALIDATION_RULES = {
        '大区': ['东一区', '东二区', '北区', '南区', '西区', '中区']
    }

    async def validate_file(self, file_content: bytes, filename: str) -> Tuple[bool, List[str]]:
        """验证文件格式"""
        errors = []

        # 检查文件扩展名
        if not filename.endswith(('.xlsx', '.xls')):
            errors.append("文件格式错误：只支持Excel文件(.xlsx, .xls)")
            return False, errors

        # 检查文件大小（50MB限制）
        if len(file_content) > 50 * 1024 * 1024:
            errors.append("文件过大：不能超过50MB")
            return False, errors

        return True, errors

    async def parse_file(self, file_content: bytes) -> Dict[str, pd.DataFrame]:
        """解析Excel文件"""
        try:
            # 读取Excel文件的第一个sheet
            df = pd.read_excel(io.BytesIO(file_content), sheet_name=0)

            # 去除列名前后的空格
            df.columns = df.columns.str.strip()

            return {"main": df}

        except Exception as e:
            raise ValueError(f"解析Excel文件失败: {str(e)}")

    async def validate_data(self, data: Dict[str, pd.DataFrame]) -> Tuple[bool, List[str]]:
        """验证数据内容"""
        errors = []
        df = data.get("main")

        if df is None or df.empty:
            errors.append("数据表为空")
            return False, errors

        # 1. 检查列名完整性
        missing_cols = set(self.EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            errors.append(f"缺少必要的列: {', '.join(missing_cols)}")
            return False, errors

        # 2. 检查必填字段（不允许空值）
        required_fields = ['大区', '省份', '城市', '项目上线id', '项目名称', '季度']
        for field in required_fields:
            if df[field].isna().any():
                null_count = df[field].isna().sum()
                errors.append(f"列 '{field}' 包含 {null_count} 个空值")

        # 3. 检查大区值的有效性
        if '大区' in df.columns:
            invalid_regions = df[~df['大区'].isin(
                self.VALIDATION_RULES['大区'])]['大区'].unique()
            if len(invalid_regions) > 0:
                errors.append(
                    f"无效的大区值: {', '.join(map(str, invalid_regions))}")

        # 4. 检查项目上线id的唯一性（在同一季度内）
        if '项目上线id' in df.columns and '季度' in df.columns:
            duplicates = df.groupby(['项目上线id', '季度']).size()
            duplicates = duplicates[duplicates > 1]
            if len(duplicates) > 0:
                dup_ids = duplicates.index.tolist()[:5]
                errors.append(f"存在重复的项目ID和季度组合: {dup_ids}...")

        # 5. 检查枪数的合理性
        if '枪数' in df.columns:
            df['枪数'] = pd.to_numeric(df['枪数'], errors='coerce')
            if (df['枪数'] < 0).any():
                errors.append("枪数不能为负数")
            if (df['枪数'] > 100).any():
                errors.append("存在异常的枪数值（大于100），请确认是否正确")

        return len(errors) == 0, errors

    async def process_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """处理数据 - 数据清洗和计算"""
        df = data["main"].copy()

        # 1. 数据清洗
        # 去除字符串类型列的前后空格
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()

        # 2. 数据类型转换
        numeric_fields = ['枪数']
        for field in numeric_fields:
            df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)

        # 3. 添加元数据
        df['数据版本'] = self.project_id + "_" + \
            datetime.now().strftime("%Y%m%d%H%M%S")
        df['上传时间'] = self.upload_time
        df['更新时间'] = self.upload_time

        # 5. 生成汇总统计
        summary = {
            "total_records": len(df),
            "total_projects": df['项目上线id'].nunique(),
            "total_guns": int(df['枪数'].sum()),
            "regions": df['大区'].unique().tolist(),
            "quarters": df['季度'].unique().tolist(),
            "upload_time": self.upload_time.isoformat()
        }

        return {
            "processed_data": df,
            "summary": summary
        }

    async def save_data(self, processed_data: Dict[str, Any], version_id: str) -> bool:
        """保存数据到数据库 - 使用统一的数据库管理器"""
        try:
            # 直接使用DataDBManager，不需要传入db_manager
            success = await DataDBManager.save_project_data(
                project_id=self.project_id,
                processed_data=processed_data,
                version_id=version_id
            )
            
            return success
            
        except Exception as e:
            print(f"保存数据失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
