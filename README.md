# Team Tools 团队工具集

本仓库包含多个团队工具项目，每个项目位于独立的分支中。

## 仓库结构

本仓库采用多分支管理方式，每个工具项目对应一个独立分支：

- `main` - 主分支，包含仓库说明
- `my-ai-assistant` - AI 助手项目
- `财务看板` - 财务数据看板工具
- `充电站健康度评估` - 充电站健康度评估工具
- `地级行政区划` - 地级行政区划数据工具
- `供应商质量问题追踪看板` - 供应商质量追踪看板

## 如何使用

### 克隆特定项目分支

```bash
# 克隆单个分支
git clone -b <分支名> https://github.com/songyadong896-debug/team_tools.git

# 例如，克隆财务看板项目：
git clone -b 财务看板 https://github.com/songyadong896-debug/team_tools.git
```

### 查看所有分支

```bash
git clone https://github.com/songyadong896-debug/team_tools.git
cd team_tools
git branch -r
```

### 切换到其他项目分支

```bash
git checkout <分支名>
```

## 项目列表

### my-ai-assistant
AI 助手相关项目

### 财务看板
财务数据可视化看板工具

### 充电站健康度评估
充电站运营健康度评估与可视化工具

### 地级行政区划
地级行政区划数据管理工具

### 供应商质量问题追踪看板
供应商质量问题追踪与管理看板

## 维护说明

每个分支独立维护，互不影响。添加新工具时，请创建新的独立分支。
