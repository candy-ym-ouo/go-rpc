# go-rpc

一个只依赖 Go 标准库的轻量级 RPC 教学项目，具备三种序列化协议、TCP 帧协议、连接池、超时重试、静态/Consul 服务发现、四种负载均衡策略、指标 API 和内嵌 Web 监控台。

## 环境

- Go 1.21 或更高版本
- 可选：Consul 1.x（仅在使用 Consul 注册中心时需要）

## 快速运行

```bash
# 终端一：RPC 服务端
make run-server

# 终端二：调用一次并启动监控台
make run-client
```

客户端会输出 `Hello, go-rpc!`，监控台默认位于 `http://localhost:8080`。

也可直接运行：

```bash
go run ./cmd/server -addr :9001
go run ./cmd/client -target 127.0.0.1:9001 -codec gob -web :8080
```

`-codec` 可选 `gob`、`json`、`binary`。

## 验证与打包

```bash
make check       # 测试、vet、build
make package     # 生成 dist/go-rpc-<os>-<arch>.tar.gz
make stats       # 非测试 Go 文件数和代码行数
```

## 主要目录

- `cmd/server`、`cmd/client`：可运行示例
- `internal/codec`：JSON、Gob、自定义二进制编码
- `internal/protocol`：23 字节定长头帧协议
- `internal/transport`、`internal/pool`：TCP 传输与连接复用
- `internal/registry`、`internal/discovery`：注册发现与负载均衡
- `internal/invoke`、`internal/server`：客户端调用链与服务端反射分发
- `pkg/monitor`：指标和嵌入式管理前端
