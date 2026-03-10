import json
import os
from typing import Dict, List
from datetime import datetime, timedelta
from collections import Counter

class AnalyticsService:
    """数据分析服务：分析用户的使用情况"""

    def __init__(self):
        self.data_file = "./database/analytics.json"
        os.makedirs("./database", exist_ok=True)
        self.data = self._load_data()

    def _load_data(self) -> Dict:
        """从文件加载数据"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            return {
                "messages": [],
                "daily_stats": {}
            }
        except Exception as e:
            print(f"加载分析数据失败: {e}")
            return {
                "messages": [],
                "daily_stats": {}
            }

    def _save_data(self):
        """保存数据到文件"""
        try:
            with open(self.data_file, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存分析数据失败: {e}")

    def log_message(
        self,
        conversation_id: str,
        user_message: str,
        ai_response: str
    ):
        """
        记录对话消息

        Args:
            conversation_id: 对话ID
            user_message: 用户消息
            ai_response: AI回复
        """
        timestamp = datetime.now().isoformat()
        date = datetime.now().strftime("%Y-%m-%d")

        # 记录消息
        self.data["messages"].append({
            "conversation_id": conversation_id,
            "timestamp": timestamp,
            "user_message": user_message,
            "ai_response": ai_response,
            "user_message_length": len(user_message),
            "ai_response_length": len(ai_response)
        })

        # 更新每日统计
        if date not in self.data["daily_stats"]:
            self.data["daily_stats"][date] = {
                "message_count": 0,
                "conversations": set()
            }

        self.data["daily_stats"][date]["message_count"] += 1

        # 保持最近1000条消息
        if len(self.data["messages"]) > 1000:
            self.data["messages"] = self.data["messages"][-1000:]

        self._save_data()

    def get_statistics(self) -> Dict:
        """
        获取统计信息

        Returns:
            统计数据字典
        """
        messages = self.data.get("messages", [])

        if not messages:
            return {
                "total_messages": 0,
                "knowledge_count": 0,
                "total_todos": 0,
                "completed_todos": 0,
                "active_days": 0,
                "average_message_length": 0,
                "most_active_hour": "N/A"
            }

        # 基础统计
        total_messages = len(messages)

        # 计算活跃天数
        dates = set()
        for msg in messages:
            try:
                date = datetime.fromisoformat(msg["timestamp"]).strftime("%Y-%m-%d")
                dates.add(date)
            except:
                pass

        active_days = len(dates)

        # 计算平均消息长度
        user_message_lengths = [msg.get("user_message_length", 0) for msg in messages]
        average_message_length = sum(user_message_lengths) / len(user_message_lengths) if user_message_lengths else 0

        # 找出最活跃的时间段
        hours = []
        for msg in messages:
            try:
                hour = datetime.fromisoformat(msg["timestamp"]).hour
                hours.append(hour)
            except:
                pass

        most_active_hour = "N/A"
        if hours:
            hour_counts = Counter(hours)
            most_common_hour = hour_counts.most_common(1)[0][0]
            most_active_hour = f"{most_common_hour:02d}:00-{most_common_hour+1:02d}:00"

        # 从其他服务获取统计
        from services.knowledge_service import KnowledgeService
        from services.todo_service import TodoService

        try:
            knowledge_service = KnowledgeService()
            knowledge_count = knowledge_service.get_knowledge_count()
        except:
            knowledge_count = 0

        try:
            todo_service = TodoService()
            todo_stats = todo_service.get_statistics()
            total_todos = todo_stats["total"]
            completed_todos = todo_stats["completed"]
        except:
            total_todos = 0
            completed_todos = 0

        return {
            "total_messages": total_messages,
            "knowledge_count": knowledge_count,
            "total_todos": total_todos,
            "completed_todos": completed_todos,
            "active_days": active_days,
            "average_message_length": round(average_message_length, 1),
            "most_active_hour": most_active_hour
        }

    def get_daily_activity(self, days: int = 7) -> List[Dict]:
        """
        获取每日活动数据

        Args:
            days: 获取最近几天的数据

        Returns:
            每日活动列表
        """
        messages = self.data.get("messages", [])
        daily_counts = Counter()

        # 统计每天的消息数
        for msg in messages:
            try:
                date = datetime.fromisoformat(msg["timestamp"]).strftime("%Y-%m-%d")
                daily_counts[date] += 1
            except:
                pass

        # 生成最近N天的数据
        result = []
        today = datetime.now().date()
        for i in range(days):
            date = today - timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")
            result.append({
                "date": date_str,
                "message_count": daily_counts.get(date_str, 0)
            })

        return list(reversed(result))

    def get_message_history(self, limit: int = 50) -> List[Dict]:
        """
        获取消息历史

        Args:
            limit: 返回的消息数量

        Returns:
            消息列表
        """
        messages = self.data.get("messages", [])
        return messages[-limit:]

    def clear_analytics(self):
        """清空所有分析数据"""
        self.data = {
            "messages": [],
            "daily_stats": {}
        }
        self._save_data()
        print("🗑️ 已清空所有分析数据")
