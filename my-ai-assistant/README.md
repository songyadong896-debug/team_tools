# 我的智能机器人助手

一个功能完整的智能机器人系统，具备对话交流、知识学习、事项管理和数据分析能力。

## 功能特性

- **对话交流**：基于Claude AI的自然语言对话
- **知识学习**：使用RAG技术学习和记忆知识文档
- **事项管理**：待办事项、日程安排、提醒功能
- **数据分析**：对话数据分析和可视化展示

## 技术栈

### 后端
- Python 3.10+
- FastAPI - Web框架
- 智谱AI GLM-4 - AI能力（免费）
- ChromaDB - 向量数据库
- SQLite - 关系型数据库

### 前端
- React 18
- Vite - 构建工具
- TailwindCSS - 样式框架
- Axios - HTTP客户端

## 快速开始

### 1. 环境准备

确保已安装：
- Python 3.10+
- Node.js 16+
- npm/yarn

### 2. 后端设置

```bash
cd backend
pip install -r requirements.txt
cp .env.example .env
# 编辑 .env 文件，填入你的 Claude API Key
python main.py
```

后端将在 http://localhost:8000 启动

### 3. 前端设置

```bash
cd frontend
npm install
npm run dev
```

前端将在 http://localhost:5173 启动

### 4. 访问应用

在浏览器打开 http://localhost:5173 即可开始使用

## 配置说明

### 获取智谱AI API Key

1. 访问 https://open.bigmodel.cn/
2. 注册/登录账号（支持手机号、微信登录）
3. 进入个人中心 - API Keys
4. 创建新的API密钥
5. 将密钥填入 `backend/.env` 文件

**完全免费，新用户即送免费额度，足够个人使用！**

## 使用指南

### 对话交流
直接在聊天框输入问题即可与AI助手对话。

### 知识学习
使用命令格式：
```
/学习 [知识内容]
```
例如：`/学习 我喜欢喝咖啡，每天早上都要来一杯`

### 事项管理
- 添加待办：`/待办 买菜`
- 查看待办：`/待办列表`
- 完成待办：`/完成 1`

### 数据分析
使用命令：`/分析` 查看你的对话统计和使用情况

## 项目结构

```
my-ai-assistant/
├── backend/           # 后端服务
│   ├── main.py       # 主程序
│   ├── models/       # 数据模型
│   ├── services/     # 业务逻辑
│   └── database/     # 数据库配置
├── frontend/          # 前端应用
│   ├── src/
│   │   ├── components/  # React组件
│   │   ├── services/    # API调用
│   │   └── App.jsx      # 主应用
│   └── public/
└── README.md
```

## 开发说明

- 后端API文档：http://localhost:8000/docs
- 前端开发端口：5173
- 后端开发端口：8000

## 许可证

MIT License
