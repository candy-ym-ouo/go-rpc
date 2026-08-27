基于 Go 实现的 RPC 服务与监控 Web 项目，提供多编解码协议、服务发现、负载均衡、连接池及运行指标查询能力。

# go-rpc 评测与运行说明

## 项目说明

`go-rpc` 是一个仅依赖 Go 标准库的轻量级 RPC 教学项目，包含 TCP 帧协议、JSON/Gob/二进制编解码、连接池、超时重试、静态/Consul 服务发现、负载均衡策略和嵌入式监控 API。

## 环境要求

- Go 1.21 或更高版本
- Docker Desktop（构建评测镜像时需要）
- 可选：Consul 1.x（仅使用 Consul 注册中心时需要）

## 本地构建、测试与运行

```bash
# 整体编译检查、静态检查和测试
go vet ./...
go test ./...
go build ./...
make check

# 启动服务端
go run ./cmd/server -addr :9001

# 新开终端：调用服务并启动监控台
go run ./cmd/client -target 127.0.0.1:9001 -codec gob -web :8080

# 打包本机平台发布包
make package
```

客户端成功调用后会输出 `Hello, go-rpc!`；监控台默认地址为 `http://localhost:8080`。

## Docker 评测镜像

```bash
chmod +x build_benzhi_docker.sh

# 默认构建 linux/amd64 镜像
./build_benzhi_docker.sh go-rpc

# 分别验证 arm64 与 amd64 镜像
./build_benzhi_docker.sh go-rpc linux/arm64
./build_benzhi_docker.sh go-rpc linux/amd64

# 进入镜像后可继续执行 Go 构建、测试与运行命令
docker run -it go-rpc:latest
```

镜像保留完整 Go 工具链，并在构建阶段预先执行 `go mod download` 和 `go build ./...`。
