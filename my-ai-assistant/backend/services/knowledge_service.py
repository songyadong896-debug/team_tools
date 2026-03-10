import os
import chromadb
from chromadb.config import Settings
from typing import List, Dict
from datetime import datetime
import uuid

class KnowledgeService:
    """知识服务：使用向量数据库存储和检索知识"""

    def __init__(self):
        # 初始化ChromaDB
        persist_dir = os.getenv("CHROMA_PERSIST_DIR", "./database/chroma")
        os.makedirs(persist_dir, exist_ok=True)

        self.client = chromadb.PersistentClient(path=persist_dir)

        # 获取或创建知识库集合
        try:
            self.collection = self.client.get_or_create_collection(
                name="knowledge_base",
                metadata={"description": "用户教给AI的知识"}
            )
        except Exception as e:
            print(f"初始化知识库失败: {e}")
            raise

    def add_knowledge(
        self,
        content: str,
        category: str = "general",
        metadata: Dict = None
    ) -> str:
        """
        添加新知识到知识库

        Args:
            content: 知识内容
            category: 知识类别
            metadata: 额外的元数据

        Returns:
            知识ID
        """
        knowledge_id = str(uuid.uuid4())

        # 构建元数据
        meta = {
            "category": category,
            "timestamp": datetime.now().isoformat(),
            "source": "user"
        }
        if metadata:
            meta.update(metadata)

        try:
            # 添加到向量数据库
            self.collection.add(
                documents=[content],
                metadatas=[meta],
                ids=[knowledge_id]
            )
            print(f"✅ 已添加知识: {content[:50]}...")
            return knowledge_id

        except Exception as e:
            print(f"添加知识失败: {e}")
            raise

    def search_knowledge(
        self,
        query: str,
        top_k: int = 3
    ) -> str:
        """
        检索相关知识

        Args:
            query: 查询文本
            top_k: 返回最相关的k条知识

        Returns:
            相关知识的文本（格式化后）
        """
        try:
            # 检查集合是否为空
            if self.collection.count() == 0:
                return ""

            # 执行相似度搜索
            results = self.collection.query(
                query_texts=[query],
                n_results=min(top_k, self.collection.count())
            )

            # 格式化结果
            if results and results["documents"] and results["documents"][0]:
                knowledge_items = results["documents"][0]
                return "\n".join(f"- {item}" for item in knowledge_items)
            else:
                return ""

        except Exception as e:
            print(f"检索知识失败: {e}")
            return ""

    def get_all_knowledge(self) -> List[Dict]:
        """
        获取所有知识

        Returns:
            知识列表
        """
        try:
            if self.collection.count() == 0:
                return []

            # 获取所有文档
            results = self.collection.get()

            knowledge_list = []
            if results and results["documents"]:
                for i, doc in enumerate(results["documents"]):
                    knowledge_list.append({
                        "id": results["ids"][i],
                        "content": doc,
                        "metadata": results["metadatas"][i] if results["metadatas"] else {}
                    })

            return knowledge_list

        except Exception as e:
            print(f"获取知识列表失败: {e}")
            return []

    def delete_knowledge(self, knowledge_id: str):
        """删除知识"""
        try:
            self.collection.delete(ids=[knowledge_id])
            print(f"✅ 已删除知识: {knowledge_id}")
        except Exception as e:
            print(f"删除知识失败: {e}")
            raise

    def get_knowledge_count(self) -> int:
        """获取知识数量"""
        try:
            return self.collection.count()
        except:
            return 0

    def clear_all_knowledge(self):
        """清空所有知识"""
        try:
            # 删除并重新创建集合
            self.client.delete_collection("knowledge_base")
            self.collection = self.client.get_or_create_collection(
                name="knowledge_base",
                metadata={"description": "用户教给AI的知识"}
            )
            print("✅ 已清空所有知识")
        except Exception as e:
            print(f"清空知识失败: {e}")
            raise
