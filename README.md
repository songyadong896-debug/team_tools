# Team Tools 团队工具集

本仓库包含多个团队工具项目，每个项目位于独立的子文件夹中。

## 仓库结构

```
team_tools/
├── my-ai-assistant/          # AI 助手全栈应用
├── 财务看板/                  # 财务数据看板工具
├── 充电站健康度评估/          # 充电站健康度评估工具
└── README.md                 # 本说明文档
```

## 如何使用

### 克隆整个仓库

```bash
git clone https://github.com/songyadong896-debug/team_tools.git
cd team_tools
```

克隆后，您可以直接访问所有项目子文件夹。

### 访问特定项目

进入对应的子文件夹即可：

```bash
cd my-ai-assistant
# 或
cd 财务看板
# 或
cd 充电站健康度评估
```

## 项目列表

### my-ai-assistant
**全栈 AI 助手应用**
- 技术栈：FastAPI (后端) + React (前端)
- 功能：智能对话、知识管理、任务追踪、数据分析
- 详见：[my-ai-assistant/README.md](my-ai-assistant/README.md)

### 财务看板
**财务数据可视化看板工具**
- 技术栈：Python
- 功能：财务模型追踪与数据可视化
- 文件：[财务看板/财务模型追踪看板.py](财务看板/财务模型追踪看板.py)

### 充电站健康度评估
**充电站运营健康度评估与可视化工具**
- 技术栈：HTML/JavaScript
- 功能：充电站健康度评估、数据可视化
- 详见：[充电站健康度评估/README.txt](充电站健康度评估/README.txt)

## 历史分支

为了保留历史记录，各项目的独立分支仍然保留在仓库中：
- `my-ai-assistant` 分支
- `财务看板` 分支
- `充电站健康度评估` 分支

## 维护说明

所有项目现在位于 main 分支的子文件夹中。添加新工具时，请在 main 分支创建新的子文件夹。
