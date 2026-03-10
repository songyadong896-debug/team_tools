# 快速开始指南

## 第一步：安装依赖

### 1. 安装后端依赖

打开终端，进入后端目录：

```bash
cd backend
pip install -r requirements.txt
```

如果你使用的是国内网络，可以使用国内镜像加速：

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 2. 安装前端依赖

打开新的终端窗口，进入前端目录：

```bash
cd frontend
npm install
```

或使用淘宝镜像：

```bash
npm install --registry=https://registry.npmmirror.com
```

## 第二步：配置智谱AI API密钥

### 1. 获取API密钥（完全免费）

访问 https://open.bigmodel.cn/

- 点击右上角"登录/注册"
- 可以使用手机号或微信登录
- 登录后，点击右上角个人中心
- 在左侧菜单找到"API Keys"
- 点击"添加新的API key"创建密钥
- 复制生成的API Key

**免费额度说明：**
- 新用户注册即送免费tokens
- GLM-4-Flash模型免费额度非常充足
- 完全够个人使用，无需付费

### 2. 配置环境变量

在 `backend` 目录下：

```bash
# Windows
copy .env.example .env

# Mac/Linux
cp .env.example .env
```

然后编辑 `.env` 文件，将你的API密钥填入：

```env
ZHIPUAI_API_KEY=你复制的API密钥
```

保存文件。

## 第三步：启动服务

### 1. 启动后端服务

在后端目录（backend）的终端中：

```bash
python main.py
```

你应该看到类似这样的输出：

```
╔═══════════════════════════════════════╗
║   AI Assistant API Server Started     ║
╠═══════════════════════════════════════╣
║   URL: http://localhost:8000         ║
║   Docs: http://localhost:8000/docs   ║
╚═══════════════════════════════════════╝
```

后端服务现在运行在 http://localhost:8000

### 2. 启动前端服务

在前端目录（frontend）的终端中：

```bash
npm run dev
```

你应该看到类似这样的输出：

```
VITE v5.0.8  ready in 500 ms

➜  Local:   http://localhost:5173/
➜  Network: use --host to expose
```

前端服务现在运行在 http://localhost:5173

## 第四步：开始使用

1. 在浏览器中打开 http://localhost:5173
2. 你会看到智能助手的聊天界面
3. 试着发送一条消息，比如"你好"

恭喜！你的智能助手已经成功运行了！

## 功能演示

### 1. 对话交流

直接输入消息即可与AI对话：

```
你好
今天天气怎么样？
帮我写一个Python函数
```

### 2. 知识学习

教会AI记住信息：

```
/学习 我的生日是3月15日
/学习 我喜欢喝咖啡，不喜欢茶
/学习 公司地址是北京市朝阳区xxx
```

之后AI会记住这些信息，在对话中提及相关话题时会主动使用。

### 3. 待办事项

管理你的待办事项：

```
/待办 买菜
/待办 写周报
/待办 准备会议材料
/待办列表           # 查看所有待办
/完成 1             # 完成第1项待办
```

### 4. 数据分析

查看使用统计：

```
/分析
```

会显示你的对话次数、学习的知识数量、待办事项统计等。

## 常见问题

### Q1: 安装依赖失败怎么办？

**Python依赖问题：**
- 确保Python版本为3.10+：`python --version`
- 尝试升级pip：`pip install --upgrade pip`
- 使用国内镜像源

**Node.js依赖问题：**
- 确保Node.js版本为16+：`node --version`
- 清除npm缓存：`npm cache clean --force`
- 删除node_modules重新安装

### Q2: 后端启动失败

**错误：ValueError: 请在.env文件中配置ZHIPUAI_API_KEY**
- 检查是否创建了 `.env` 文件
- 检查API密钥是否正确填写
- 确保没有多余的空格或引号

**错误：端口被占用**
- 修改 `.env` 文件中的 `PORT=8000` 改为其他端口
- 或者关闭占用8000端口的程序

### Q3: 前端无法连接后端

**错误：Network Error 或 连接失败**
- 确保后端服务正在运行
- 检查后端是否在 http://localhost:8000
- 检查浏览器控制台的错误信息

### Q4: API调用报错

**错误：401 Unauthorized**
- API密钥无效或已过期
- 重新生成密钥并更新 `.env` 文件

**错误：429 Too Many Requests**
- API调用频率超限
- 等待一段时间后重试
- 考虑升级API套餐

### Q5: 数据存储在哪里？

- 待办事项：`backend/database/todos.json`
- 对话记录：`backend/database/analytics.json`
- 知识库：`backend/database/chroma/` 目录
- 如果需要清空数据，删除这些文件即可

## 进阶配置

### 修改AI模型

编辑 `backend/services/ai_service.py`，找到：

```python
model="glm-4-flash"
```

可以改为其他模型，如：
- `glm-4` - 更强大的标准版
- `glm-4-plus` - 最强版本
- `glm-4-flash` - 免费快速版（推荐）

### 修改系统提示词

在 `backend/services/ai_service.py` 中找到 `system_prompt`，可以自定义AI的性格和行为。

### 部署到服务器

详见 README.md 中的部署说明。

## 下一步

- 探索更多命令和功能
- 教会AI更多关于你的知识
- 查看API文档：http://localhost:8000/docs
- 根据需求修改和扩展功能

祝你使用愉快！
