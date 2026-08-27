# 保留完整 Go 工具链，供容器内构建、测试和修复使用。
FROM golang:1.22

WORKDIR /app

# 先下载模块依赖，便于利用 Docker 缓存并支持后续离线构建。
COPY go.mod ./
RUN go mod download

COPY . .
RUN go build ./...

CMD ["bash"]
