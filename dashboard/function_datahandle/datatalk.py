from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

class UploadRequest(BaseModel):
    """统一的上传请求模型"""
    project_id: str
    file_type: str = "excel"
    options: Optional[Dict[str, Any]] = {}
    user: Optional[str] = "system"

class UploadResponse(BaseModel):
    """统一的上传响应模型"""
    success: bool
    project_id: str
    task_id: Optional[str] = None
    version_id: Optional[str] = None
    message: str
    upload_time: Optional[datetime] = None
    data: Optional[Dict[str, Any]] = None
    errors: Optional[List[str]] = None
    
class UploadProgress(BaseModel):
    """上传进度模型"""
    task_id: str
    status: str  # processing, completed, failed
    progress: int  # 0-100
    message: str
    start_time: datetime
    end_time: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None