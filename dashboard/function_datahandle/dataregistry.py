# function_datahandle/dataregistry.py
from typing import Dict, Type, Optional, List
from .dataupload import DataUploadHandler


class DataRegistry:
    """数据处理器注册表"""

    _handlers: Dict[str, Type[DataUploadHandler]] = {}
    _configs: Dict[str, dict] = {}

    @classmethod
    def register_handler(cls, project_id: str, handler_class: Type[DataUploadHandler], config: dict = None):
        """注册项目的处理器"""
        cls._handlers[project_id] = handler_class
        if config:
            cls._configs[project_id] = config
        print(f"Registered handler for project: {project_id}")

    @classmethod
    def get_handler(cls, project_id: str) -> Optional[Type[DataUploadHandler]]:
        """获取项目的处理器类"""
        return cls._handlers.get(project_id)

    @classmethod
    def get_config(cls, project_id: str) -> Optional[dict]:
        """获取项目配置"""
        return cls._configs.get(project_id)

    @classmethod
    def list_projects(cls) -> List[str]:
        """列出所有已注册的项目"""
        return list(cls._handlers.keys())

    @classmethod
    def create_handler(cls, project_id: str) -> Optional[DataUploadHandler]:
        """创建处理器实例"""
        handler_class = cls.get_handler(project_id)
        if handler_class:
            return handler_class(project_id)
        return None

    @classmethod
    def auto_register_handlers(cls):
        """
        自动注册所有处理器
        在这里添加所有项目的注册代码
        """
        # 示例：注册场站参数项目
        try:
            # 使用完整的导入路径
            from src.dashboard.pxxdash import PxxDashHandler
            cls.register_handler(
                project_id="pxxdash",
                handler_class=PxxDashHandler,
                config={
                    "name": "PXX Dashboard",
                    "description": "PXX项目财务模型数据管理",
                    "allowed_file_types": ["xlsx", "xls"],
                    "max_file_size": 50 * 1024 * 1024,  # 50MB
                    "async_threshold": 5 * 1024 * 1024   # 5MB以上异步处理
                }
            )
            from .datadb_manager import DataDBManager
            DataDBManager.auto_register_writers()
            print("Successfully registered PxxDashHandler")
        except ImportError as e:
            print(f"Warning: PxxDashHandler not found - {e}")

        # TODO: 在这里添加更多项目的注册
        # try:
        #     from your_new_handler import YourNewHandler
        #     cls.register_handler(
        #         project_id="your_project_id",
        #         handler_class=YourNewHandler,
        #         config={
        #             "name": "项目名称",
        #             "description": "项目描述",
        #             "allowed_file_types": ["xlsx"],
        #             "max_file_size": 50 * 1024 * 1024
        #         }
        #     )
        # except ImportError:
        #     print("Warning: YourNewHandler not found")

        print(f"自动注册完成，共注册 {len(cls._handlers)} 个数据处理器")
