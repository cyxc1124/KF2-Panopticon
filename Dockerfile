# KF2-Panopticon Dockerfile
# 多阶段构建，优化镜像大小

FROM python:3.12-slim as base

# 构建参数（由GitHub Action传入）
ARG GIT_TAG=""
ARG GIT_COMMIT=""
ARG GIT_BRANCH=""
ARG BUILD_TIME=""
ARG BUILD_NUMBER=""

# 添加 OCI 标签以连接到 GitHub 仓库
LABEL org.opencontainers.image.source=https://github.com/cyxc1124/KF2-Panopticon
LABEL org.opencontainers.image.description="KF2-Panopticon"
LABEL org.opencontainers.image.title="KF2-Panopticon"
LABEL org.opencontainers.image.vendor="cyxc1124"
LABEL org.opencontainers.image.version=${GIT_TAG}
LABEL org.opencontainers.image.revision=${GIT_COMMIT}

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai \
    GIT_TAG=${GIT_TAG} \
    GIT_COMMIT=${GIT_COMMIT} \
    GIT_BRANCH=${GIT_BRANCH} \
    BUILD_TIME=${BUILD_TIME} \
    BUILD_NUMBER=${BUILD_NUMBER}

# 安装系统依赖
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        vim \
        curl \
        procps \
        net-tools \
        iputils-ping \
        telnet \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 复制并设置入口点脚本
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# 创建数据和日志目录
RUN mkdir -p /app/data /app/logs

# 暴露端口（仅 web 模式需要）
EXPOSE 9001

# 非 root 用户运行（安全最佳实践）
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

# 入口点脚本
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# 默认模式：web（可在 K8s 中覆盖）
CMD ["web"]
