from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime

class DataUploadHandler(ABC):
    """数据上传处理器基类"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.upload_time = datetime.now()
        self.errors = []
        
    @abstractmethod
    async def validate_file(self, file_content: bytes, filename: str) -> Tuple[bool, List[str]]:
        """
        验证文件格式和基础结构
        返回: (是否有效, 错误信息列表)
        """
        pass
    
    @abstractmethod
    async def parse_file(self, file_content: bytes) -> Dict[str, pd.DataFrame]:
        """
        解析文件内容，返回sheet名称和DataFrame的字典
        """
        pass
    
    @abstractmethod
    async def validate_data(self, data: Dict[str, pd.DataFrame]) -> Tuple[bool, List[str]]:
        """
        验证数据内容（列名、数据类型、业务规则等）
        返回: (是否有效, 错误信息列表)
        """
        pass
    
    @abstractmethod
    async def process_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        处理数据（清洗、转换、计算等）
        """
        pass
    
    @abstractmethod
    async def save_data(self, processed_data: Dict[str, Any], version_id: str) -> bool:
        """
        保存数据到数据库
        """
        pass
    
    async def handle_upload(self, file_content: bytes, filename: str, **kwargs) -> Dict[str, Any]:
        """
        完整的上传处理流程
        """
        # 1. 验证文件
        is_valid, errors = await self.validate_file(file_content, filename)
        if not is_valid:
            return {
                "success": False,
                "errors": errors
            }
        
        # 2. 解析文件
        data = await self.parse_file(file_content)
        
        # 3. 验证数据
        is_valid, errors = await self.validate_data(data)
        if not is_valid:
            return {
                "success": False,
                "errors": errors
            }
        
        # 4. 处理数据
        processed_data = await self.process_data(data)
        
        # 5. 生成版本ID
        version_id = datetime.now().strftime("%Y%m%d%H%M%S") + "_" + self.project_id
        
        # 6. 保存数据
        success = await self.save_data(processed_data, version_id)
        
        return {
            "success": success,
            "version_id": version_id,
            "upload_time": self.upload_time.isoformat()
        }