import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
import json
from datetime import datetime

# 加载环境变量
load_dotenv()

# 导入服务
from services.ai_service import AIService
from services.knowledge_service import KnowledgeService
from services.todo_service import TodoService
from services.analytics_service import AnalyticsService

# 创建FastAPI应用
app = FastAPI(
    title="AI Assistant API",
    description="智能机器人助手API",
    version="1.0.0"
)

# CORS配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("FRONTEND_URL", "http://localhost:5173")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 初始化服务
ai_service = AIService()
knowledge_service = KnowledgeService()
todo_service = TodoService()
analytics_service = AnalyticsService()

# 数据模型
class ChatMessage(BaseModel):
    message: str
    conversation_id: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    conversation_id: str
    timestamp: str

class LearnRequest(BaseModel):
    content: str
    category: Optional[str] = "general"

class TodoItem(BaseModel):
    title: str
    description: Optional[str] = None
    priority: Optional[str] = "medium"

# API路由

@app.get("/")
async def root():
    return {
        "message": "AI Assistant API",
        "version": "1.0.0",
        "status": "running"
    }

@app.post("/api/chat", response_model=ChatResponse)
async def chat(message: ChatMessage):
    """
    聊天接口：处理用户消息并返回AI回复
    支持命令：/学习、/待办、/待办列表、/完成、/分析
    """
    try:
        user_message = message.message.strip()
        conversation_id = message.conversation_id or generate_conversation_id()

        # 检查是否是特殊命令
        if user_message.startswith("/学习 "):
            content = user_message[4:].strip()
            knowledge_service.add_knowledge(content)
            response_text = f"✅ 已学习并记住：{content}"

        elif user_message.startswith("/待办 "):
            todo_title = user_message[4:].strip()
            todo_id = todo_service.add_todo(todo_title)
            response_text = f"✅ 已添加待办事项 #{todo_id}：{todo_title}"

        elif user_message == "/待办列表":
            todos = todo_service.get_todos()
            if todos:
                response_text = "📋 你的待办事项：\n\n" + "\n".join(
                    f"{i+1}. {'✅' if todo['completed'] else '⬜'} {todo['title']}"
                    for i, todo in enumerate(todos)
                )
            else:
                response_text = "暂无待办事项"

        elif user_message.startswith("/完成 "):
            todo_id = int(user_message[4:].strip())
            todo_service.complete_todo(todo_id)
            response_text = f"✅ 已完成待办事项 #{todo_id}"

        elif user_message == "/分析":
            stats = analytics_service.get_statistics()
            response_text = f"""📊 使用统计：

- 总对话次数：{stats['total_messages']}
- 学习的知识数：{stats['knowledge_count']}
- 待办事项：{stats['total_todos']} 个（已完成 {stats['completed_todos']} 个）
- 活跃天数：{stats['active_days']} 天
"""

        else:
            # 正常对话：检索相关知识并生成回复
            relevant_knowledge = knowledge_service.search_knowledge(user_message)
            response_text = await ai_service.chat(
                user_message,
                conversation_id,
                context=relevant_knowledge
            )

        # 记录对话
        analytics_service.log_message(conversation_id, user_message, response_text)

        return ChatResponse(
            response=response_text,
            conversation_id=conversation_id,
            timestamp=datetime.now().isoformat()
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/learn")
async def learn(request: LearnRequest):
    """学习新知识"""
    try:
        knowledge_service.add_knowledge(request.content, request.category)
        return {"status": "success", "message": "知识已学习"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/knowledge")
async def get_knowledge():
    """获取所有已学习的知识"""
    try:
        knowledge = knowledge_service.get_all_knowledge()
        return {"knowledge": knowledge}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/todos")
async def create_todo(todo: TodoItem):
    """创建待办事项"""
    try:
        todo_id = todo_service.add_todo(
            todo.title,
            todo.description,
            todo.priority
        )
        return {"status": "success", "todo_id": todo_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/todos")
async def list_todos():
    """获取所有待办事项"""
    try:
        todos = todo_service.get_todos()
        return {"todos": todos}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/todos/{todo_id}/complete")
async def complete_todo(todo_id: int):
    """完成待办事项"""
    try:
        todo_service.complete_todo(todo_id)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/todos/{todo_id}")
async def delete_todo(todo_id: int):
    """删除待办事项"""
    try:
        todo_service.delete_todo(todo_id)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics")
async def get_analytics():
    """获取使用统计"""
    try:
        stats = analytics_service.get_statistics()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 辅助函数
def generate_conversation_id():
    """生成对话ID"""
    return f"conv_{datetime.now().strftime('%Y%m%d%H%M%S')}"

# 启动服务器
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    reload = os.getenv("RELOAD", "True").lower() == "true"

    print(f"""
    ╔═══════════════════════════════════════╗
    ║   AI Assistant API Server Started     ║
    ╠═══════════════════════════════════════╣
    ║   URL: http://localhost:{port}       ║
    ║   Docs: http://localhost:{port}/docs ║
    ╚═══════════════════════════════════════╝
    """)

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=reload
    )
