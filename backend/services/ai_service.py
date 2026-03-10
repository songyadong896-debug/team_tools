import os
from zhipuai import ZhipuAI
from typing import Optional, List

class AIService:
    """AI服务：使用智谱AI GLM-4进行对话"""

    def __init__(self):
        api_key = os.getenv("ZHIPUAI_API_KEY")
        if not api_key or api_key == "your_zhipuai_api_key_here":
            raise ValueError(
                "请在.env文件中配置ZHIPUAI_API_KEY。"
                "获取方式：https://open.bigmodel.cn/"
            )
        self.client = ZhipuAI(api_key=api_key)
        self.conversations = {}  # 存储对话历史

    async def chat(
        self,
        user_message: str,
        conversation_id: str,
        context: Optional[str] = None
    ) -> str:
        """
        与AI对话

        Args:
            user_message: 用户消息
            conversation_id: 对话ID
            context: 额外的上下文信息（如检索到的知识）

        Returns:
            AI的回复
        """
        # 获取或创建对话历史
        if conversation_id not in self.conversations:
            self.conversations[conversation_id] = []

        history = self.conversations[conversation_id]

        # 构建系统提示
        system_prompt = """你是一个智能助手，名叫"小智"。你的特点是：

1. 友好、专业、有耐心
2. 能够学习和记住用户告诉你的信息
3. 帮助用户管理待办事项和日程
4. 分析和总结数据

当用户告诉你一些信息时，要表现出记住了这些信息的样子。"""

        # 如果有相关知识，添加到系统提示中
        if context:
            system_prompt += f"\n\n相关记忆和知识：\n{context}"

        # 添加当前消息到历史
        history.append({
            "role": "user",
            "content": user_message
        })

        try:
            # 调用智谱AI API
            # 智谱AI需要将系统提示作为第一条消息
            messages = [{"role": "system", "content": system_prompt}] + history

            response = self.client.chat.completions.create(
                model="glm-4-flash",  # 使用GLM-4-Flash，速度快且免费额度多
                messages=messages,
                max_tokens=2048,
                temperature=0.7,
            )

            # 提取回复文本
            assistant_message = response.choices[0].message.content

            # 将AI回复添加到历史
            history.append({
                "role": "assistant",
                "content": assistant_message
            })

            # 保持对话历史在合理长度（最近20轮）
            if len(history) > 40:
                self.conversations[conversation_id] = history[-40:]

            return assistant_message

        except Exception as e:
            print(f"AI服务错误: {e}")
            return f"抱歉，我遇到了一些问题：{str(e)}"

    def clear_conversation(self, conversation_id: str):
        """清除对话历史"""
        if conversation_id in self.conversations:
            del self.conversations[conversation_id]

    def get_conversation_count(self) -> int:
        """获取对话数量"""
        return len(self.conversations)
