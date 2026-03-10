# AI 助手部署指南

本指南将帮助你将AI助手部署到免费的云平台，使其可以在任何设备上访问。

## 📋 部署架构

- **后端**: Render.com (免费套餐)
- **前端**: Vercel (免费套餐)
- **数据库**: SQLite + ChromaDB (自动随后端部署)

## 🚀 部署步骤

### 第一步：准备工作

1. **注册账号**
   - 注册 [Render](https://render.com) 账号
   - 注册 [Vercel](https://vercel.com) 账号
   - 两者都支持用GitHub账号直接登录（推荐）

2. **创建GitHub仓库**
   - 在GitHub上创建一个新仓库
   - 将你的项目代码推送到GitHub

   ```bash
   cd "d:\songyadong\mywork\VS code project\my-ai-assistant"
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/你的用户名/my-ai-assistant.git
   git push -u origin main
   ```

### 第二步：部署后端到Render

1. **登录Render控制台**
   - 访问 https://dashboard.render.com
   - 点击 "New +" → "Web Service"

2. **连接GitHub仓库**
   - 选择你刚创建的GitHub仓库
   - 或者输入仓库URL

3. **配置部署参数**
   - **Name**: `ai-assistant-backend` (或你喜欢的名字)
   - **Region**: 选择 `Oregon` 或距离你最近的区域
   - **Branch**: `main`
   - **Root Directory**: `backend`
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Plan**: 选择 `Free`

4. **设置环境变量**
   点击 "Environment" → "Add Environment Variable"，添加以下变量：

   ```
   ZHIPUAI_API_KEY=你的智谱AI密钥
   FRONTEND_URL=https://你的应用名.vercel.app
   HOST=0.0.0.0
   PORT=10000
   RELOAD=False
   DATABASE_URL=sqlite:///./database/assistant.db
   CHROMA_PERSIST_DIR=./database/chroma
   ```

   **注意**: `FRONTEND_URL` 先留空或填写临时值，等前端部署完成后再回来更新

5. **创建部署**
   - 点击 "Create Web Service"
   - 等待几分钟，Render会自动构建和部署
   - 部署成功后，你会得到一个URL，类似: `https://ai-assistant-backend-xxxx.onrender.com`
   - **记下这个URL，后面需要用到！**

### 第三步：部署前端到Vercel

1. **更新前端环境变量**
   在本地修改 `frontend/.env` 文件：
   ```
   VITE_API_URL=https://你的Render后端URL
   ```

   提交更改到GitHub：
   ```bash
   git add .
   git commit -m "Update API URL for production"
   git push
   ```

2. **登录Vercel控制台**
   - 访问 https://vercel.com/dashboard
   - 点击 "Add New..." → "Project"

3. **导入GitHub仓库**
   - 选择你的GitHub仓库
   - 点击 "Import"

4. **配置部署参数**
   - **Project Name**: `ai-assistant` (或你喜欢的名字)
   - **Framework Preset**: `Vite`
   - **Root Directory**: `frontend`
   - **Build Command**: `npm run build`
   - **Output Directory**: `dist`

5. **设置环境变量**
   展开 "Environment Variables"，添加：
   ```
   VITE_API_URL=https://你的Render后端URL
   ```

6. **部署**
   - 点击 "Deploy"
   - 等待几分钟，Vercel会自动构建和部署
   - 部署成功后，你会得到一个URL，类似: `https://ai-assistant-xxxx.vercel.app`

### 第四步：更新CORS配置

1. **回到Render控制台**
   - 打开你的后端服务
   - 进入 "Environment" 标签页
   - 找到 `FRONTEND_URL` 变量
   - 更新为你的Vercel前端URL: `https://你的应用名.vercel.app`
   - 保存后，Render会自动重新部署

### 第五步：测试部署

1. 访问你的Vercel前端URL
2. 尝试发送消息，测试聊天功能
3. 测试其他功能：学习知识、待办事项等

## 🎉 完成！

现在你的AI助手已经部署成功，可以在任何设备上通过以下URL访问：
- **前端**: `https://你的应用名.vercel.app`
- **后端API**: `https://你的后端应用名.onrender.com`

## 📱 在其他设备上使用

1. **手机/平板**
   - 直接在浏览器中访问前端URL
   - 可以将网页添加到主屏幕，像原生应用一样使用

2. **其他电脑**
   - 在浏览器中访问前端URL即可

## ⚠️ 重要提示

### Render免费套餐限制
- 15分钟无活动后会自动休眠
- 下次访问时需要等待30-60秒冷启动
- 每月750小时免费运行时间（足够个人使用）

### 数据持久化
- Render免费套餐的文件系统不是持久化的
- 如果需要长期保存数据，建议升级到付费套餐或使用外部数据库

### 保护你的API密钥
- 不要将 `.env` 文件提交到GitHub
- 已添加 `.gitignore` 忽略 `.env` 文件
- 所有敏感信息都应该在平台的环境变量中设置

## 🔧 更新和维护

### 更新应用
当你修改代码后：
```bash
git add .
git commit -m "你的更新说明"
git push
```
Render和Vercel会自动检测到代码变化并重新部署。

### 查看日志
- **Render**: Dashboard → 你的服务 → Logs 标签页
- **Vercel**: Dashboard → 你的项目 → Deployments → 点击最新部署 → Function Logs

## 📞 问题排查

### 后端无法访问
1. 检查Render服务是否在运行
2. 查看Render日志是否有错误
3. 确认环境变量是否正确设置

### 前端显示但无法连接后端
1. 检查浏览器控制台是否有CORS错误
2. 确认后端的 `FRONTEND_URL` 环境变量是否正确
3. 确认前端的 `VITE_API_URL` 是否指向正确的后端URL

### 数据丢失
Render免费套餐在休眠/重启后会丢失文件系统数据。如需持久化：
- 升级到付费套餐
- 使用外部数据库服务（如PostgreSQL）

## 💡 优化建议

1. **自定义域名**
   - Vercel和Render都支持绑定自定义域名
   - 在各自的控制台中可以找到域名设置

2. **自动唤醒后端**
   - 可以使用UptimeRobot等服务定期ping你的后端
   - 防止因长时间无访问而休眠

3. **性能监控**
   - Vercel提供免费的性能分析工具
   - Render也有基本的监控功能

## 📚 相关文档

- [Render文档](https://render.com/docs)
- [Vercel文档](https://vercel.com/docs)
- [Vite环境变量](https://vitejs.dev/guide/env-and-mode.html)
- [FastAPI部署](https://fastapi.tiangolo.com/deployment/)
