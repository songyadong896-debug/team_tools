# # database.py

# from urllib.parse import quote_plus as urlquote
# from sqlalchemy import create_engine, Column, Integer, String
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker, Session
# from typing import AsyncGenerator
# from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
# import os

# # 从环境变量获取数据库配置
# DB_USER = os.getenv("DB_USER", "lcp_business_management_rw")
# DB_PASSWORD = os.getenv("DB_PASSWORD", "")
# # 对密码进行 URL 编码
# encoded_password = urlquote(DB_PASSWORD)
# DB_HOST = os.getenv("DB_HOST", "0.0.0.0")
# DB_PORT = os.getenv("DB_PORT", "3306")
# DB_NAME = os.getenv("DB_NAME", "lcp_business_management")

# # 异步数据库 URL
# DATABASE_URL = f"mysql+asyncmy://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# # 创建异步引擎
# engine = create_async_engine(DATABASE_URL, echo=True)

# # 创建异步会话
# AsyncSessionLocal = sessionmaker(
#     bind=engine,
#     class_=AsyncSession,
#     expire_on_commit=False
# )

# Base = declarative_base()

# # 获取异步数据库会话
# async def get_db() -> AsyncGenerator[AsyncSession, None]:
#     async with AsyncSessionLocal() as session:
#         yield session

# database.py

from urllib.parse import quote_plus as urlquote
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import text
import os

# 调试用 (Docker状态下不用调整)
# 从环境变量获取数据库配置
# DB_USER = 'root'
# DB_PASSWORD = '123456'   
# # # 对密码进行 URL 编码
# encoded_password = urlquote(DB_PASSWORD)
# DB_HOST = 'localhost'
# DB_PORT = '3306'
# DB_NAME = 'lcp_business_management'
# encoded_password = urlquote(DB_PASSWORD)


# # 从环境变量获取数据库配置
DB_USER = os.getenv("DB_USER", "lcp_business_management_rw")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
# 对密码进行 URL 编码
encoded_password = urlquote(DB_PASSWORD)
DB_HOST = os.getenv("DB_HOST", "0.0.0.0")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "lcp_business_management")

# 异步数据库 URL
# 调试用 asyncmy - aiomysql
DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 创建异步引擎
async_engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    echo=False  # 生产环境设为 False
)

# 创建异步会话
AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession, 
    expire_on_commit=False
    # pool_pre_ping=True
)

Base = declarative_base()

# 获取异步数据库会话
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
        
async def execute_raw_query(query: str, params: dict = None):
    """执行原生SQL查询"""
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(query), params or {})
        return result

async def fetch_one(query: str, params: dict = None):
    """执行查询并返回单条结果"""
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(query), params or {})
        return result.fetchone()

async def fetch_all(query: str, params: dict = None):
    """执行查询并返回所有结果"""
    async with AsyncSessionLocal() as session:
        result = await session.execute(text(query), params or {})
        return result.fetchall()