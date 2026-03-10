FROM docker.m.daocloud.io/library/python:3.10-slim

WORKDIR /app

# 使用国内镜像源加速
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources || \
    sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list.d/debian.sources

# 安装必要的系统包
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# 自动切换可用的pip镜像源
RUN for mirror in \
        https://mirrors.aliyun.com/pypi/simple/ \
        https://pypi.douban.com/simple/ \
        https://mirrors.cloud.tencent.com/pypi/simple/ \
        https://pypi.tuna.tsinghua.edu.cn/simple; do \
        pip config set global.index-url $mirror && \
        pip config set global.trusted-host $(echo $mirror | sed 's|https://||;s|/.*||') && \
        pip install --upgrade pip && break || continue; \
    done

# 先复制requirements.txt
COPY requirements.txt .

# 安装Python依赖
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]