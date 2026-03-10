# 🚀 快速部署指南

> 详细的部署说明请查看 [DEPLOYMENT.md](./DEPLOYMENT.md)

## 快速步骤

### 1️⃣ 准备GitHub仓库

```bash
# 初始化git仓库（如果还没有）
cd "d:\songyadong\mywork\VS code project\my-ai-assistant"
git init
git add .
git commit -m "Initial commit"

# 在GitHub创建新仓库，然后推送代码
git remote add origin https://github.com/你的用户名/my-ai-assistant.git
git push -u origin main
```

### 2️⃣ 部署后端到Render

1. 访问 https://render.com → 登录
2. New → Web Service
3. 连接你的GitHub仓库
4. 配置：
   - Root Directory: `backend`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. 添加环境变量：
   - `ZHIPUAI_API_KEY`: 你的智谱AI密钥
   - `RELOAD`: `False`
6. 点击 "Create Web Service"
7. **记下部署后的URL**（例如：`https://xxx.onrender.com`）

### 3️⃣ 部署前端到Vercel

1. 访问 https://vercel.com → 登录
2. Add New → Project
3. 导入你的GitHub仓库
4. 配置：
   - Framework: `Vite`
   - Root Directory: `frontend`
   - Build Command: `npm run build`
   - Output Directory: `dist`
5. 添加环境变量：
   - `VITE_API_URL`: `https://你的Render后端URL`
6. 点击 "Deploy"
7. **记下部署后的URL**（例如：`https://xxx.vercel.app`）

### 4️⃣ 更新后端CORS配置

回到Render，在环境变量中添加/更新：
- `FRONTEND_URL`: `https://你的Vercel前端URL`

保存后Render会自动重新部署。

### 5️⃣ 测试

访问你的Vercel URL，测试应用功能！

## 📱 访问方式

部署成功后，你可以在任何设备上访问：
- 电脑：直接访问URL
- 手机/平板：访问URL，可添加到主屏幕

## ⚠️ 注意事项

- Render免费套餐15分钟无活动后会休眠，下次访问需要等待30-60秒唤醒
- 不要将 `.env` 文件提交到GitHub
- 所有敏感信息都在云平台的环境变量中设置

## 🔧 更新应用

修改代码后：
```bash
git add .
git commit -m "更新说明"
git push
```

Render和Vercel会自动检测并重新部署。

---

需要帮助？查看 [完整部署文档](./DEPLOYMENT.md)
