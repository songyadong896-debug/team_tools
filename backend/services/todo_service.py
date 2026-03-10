import json
import os
from typing import List, Dict, Optional
from datetime import datetime

class TodoService:
    """待办事项服务：管理用户的待办事项"""

    def __init__(self):
        self.data_file = "./database/todos.json"
        os.makedirs("./database", exist_ok=True)
        self.todos = self._load_todos()

    def _load_todos(self) -> List[Dict]:
        """从文件加载待办事项"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            return []
        except Exception as e:
            print(f"加载待办事项失败: {e}")
            return []

    def _save_todos(self):
        """保存待办事项到文件"""
        try:
            with open(self.data_file, "w", encoding="utf-8") as f:
                json.dump(self.todos, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存待办事项失败: {e}")

    def add_todo(
        self,
        title: str,
        description: Optional[str] = None,
        priority: str = "medium"
    ) -> int:
        """
        添加待办事项

        Args:
            title: 标题
            description: 描述
            priority: 优先级 (low, medium, high)

        Returns:
            待办事项ID
        """
        todo_id = len(self.todos) + 1
        todo = {
            "id": todo_id,
            "title": title,
            "description": description or "",
            "priority": priority,
            "completed": False,
            "created_at": datetime.now().isoformat(),
            "completed_at": None
        }

        self.todos.append(todo)
        self._save_todos()
        print(f"✅ 已添加待办事项 #{todo_id}: {title}")
        return todo_id

    def get_todos(self, include_completed: bool = True) -> List[Dict]:
        """
        获取待办事项列表

        Args:
            include_completed: 是否包含已完成的事项

        Returns:
            待办事项列表
        """
        if include_completed:
            return self.todos
        else:
            return [todo for todo in self.todos if not todo["completed"]]

    def get_todo(self, todo_id: int) -> Optional[Dict]:
        """获取单个待办事项"""
        for todo in self.todos:
            if todo["id"] == todo_id:
                return todo
        return None

    def complete_todo(self, todo_id: int):
        """完成待办事项"""
        for todo in self.todos:
            if todo["id"] == todo_id:
                todo["completed"] = True
                todo["completed_at"] = datetime.now().isoformat()
                self._save_todos()
                print(f"✅ 已完成待办事项 #{todo_id}")
                return

        raise ValueError(f"待办事项 #{todo_id} 不存在")

    def uncomplete_todo(self, todo_id: int):
        """取消完成待办事项"""
        for todo in self.todos:
            if todo["id"] == todo_id:
                todo["completed"] = False
                todo["completed_at"] = None
                self._save_todos()
                print(f"↩️ 已取消完成待办事项 #{todo_id}")
                return

        raise ValueError(f"待办事项 #{todo_id} 不存在")

    def delete_todo(self, todo_id: int):
        """删除待办事项"""
        self.todos = [todo for todo in self.todos if todo["id"] != todo_id]
        self._save_todos()
        print(f"🗑️ 已删除待办事项 #{todo_id}")

    def update_todo(
        self,
        todo_id: int,
        title: Optional[str] = None,
        description: Optional[str] = None,
        priority: Optional[str] = None
    ):
        """更新待办事项"""
        for todo in self.todos:
            if todo["id"] == todo_id:
                if title:
                    todo["title"] = title
                if description is not None:
                    todo["description"] = description
                if priority:
                    todo["priority"] = priority

                self._save_todos()
                print(f"✅ 已更新待办事项 #{todo_id}")
                return

        raise ValueError(f"待办事项 #{todo_id} 不存在")

    def get_statistics(self) -> Dict:
        """获取统计信息"""
        total = len(self.todos)
        completed = sum(1 for todo in self.todos if todo["completed"])
        pending = total - completed

        priority_stats = {
            "high": sum(1 for todo in self.todos if todo["priority"] == "high" and not todo["completed"]),
            "medium": sum(1 for todo in self.todos if todo["priority"] == "medium" and not todo["completed"]),
            "low": sum(1 for todo in self.todos if todo["priority"] == "low" and not todo["completed"])
        }

        return {
            "total": total,
            "completed": completed,
            "pending": pending,
            "completion_rate": round(completed / total * 100, 1) if total > 0 else 0,
            "priority_stats": priority_stats
        }

    def clear_completed(self):
        """清除所有已完成的待办事项"""
        self.todos = [todo for todo in self.todos if not todo["completed"]]
        self._save_todos()
        print("🗑️ 已清除所有已完成的待办事项")
