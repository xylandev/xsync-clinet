# xsync client

`xsync-client` 是 Linux 下载守护进程。它从 xsync server 持续认领文件，支持断点续传、本地磁盘空间检查、SHA-256 校验、原子落盘和幂等远端删除。

## 构建二进制

```bash
make test
VERSION=1.0.0 make release
```

发布命令生成两个无 CGO 依赖的 Linux 单文件：

- `dist/xsync-client-linux-amd64`
- `dist/xsync-client-linux-arm64`

根据接收机架构复制对应文件并赋予执行权限即可，无需 Go 环境或容器运行时：

```bash
sudo install -m 0755 dist/xsync-client-linux-amd64 /usr/local/bin/xsync-client
xsync-client version
```

## 配置与运行

服务端创建账号时会生成一个完整连接包。安全地把这个单文件复制到客户端后，只需指定连接包和目标目录：

```bash
sudo install -o xsync -g xsync -m 0600 customer-a.yaml /etc/xsync-client/account.yaml
sudo -u xsync xsync-client \
  --client-config /etc/xsync-client/account.yaml \
  --dist /srv/incoming
```

也可以写成等价的子命令形式：

```bash
xsync-client run --client-config /etc/xsync-client/account.yaml --dist /srv/incoming
```

连接包内已经嵌入 CA 公钥证书、下载 API Key 和账号信息，因此不再需要 `ca_file`，也不需要执行客户端 `init`。`--dist` 是客户端下载后的目标目录；可选 `--prefix incoming/` 只同步账号下的指定前缀。并发数、缓冲区、租约、轮询周期和磁盘保留空间都使用内置性能默认值。

作为常驻进程时，可复制 `deploy/xsync-client.service` 到 `/etc/systemd/system/`，放入连接包并按实际服务账号和目录修改后启用。客户端交付物始终是单一静态二进制，不依赖 Docker。

每份客户端配置通过 Download API Key 绑定一个服务端账号。`prefix` 为空时下载该账号的全部对象；设置为 `incoming/` 等前缀时只认领匹配对象。多账号应运行多份配置，并为每份配置使用不同的 `client_id` 和目标目录。

关键性能参数：

```yaml
concurrency: 8
buffer_size: 1048576
```

客户端启用 HTTP/2 和连接复用，每个 worker 可独立认领文件，适合大量小文件和大小文件混合队列。下载缓冲区由池复用；新下载的数据在写盘过程中同步计算 SHA-256，避免完成后再次完整扫描文件。SSD/NVMe 可把 `concurrency` 提升到 16 或 32；机械盘建议从 4 开始，避免多个大文件并发写入造成寻道抖动。`buffer_size` 可配置为 64 KiB 至 16 MiB，通常保持 1 MiB 即可。

旧版独立下载配置仍兼容，可继续使用 `xsync-client run --config /etc/xsync-client/config.yaml`；新部署建议直接使用服务端连接包。

客户端在目标目录的 `.xsync/partial/` 中保存 partial 文件和 sidecar。下载完成后会依次执行：

1. 校验长度和 SHA-256。
2. `fsync` partial 文件。
3. 通过硬链接原子创建最终路径，避免覆盖并发出现的同名文件。
4. `fsync` 父目录。
5. 向服务端提交 object ID、租约、长度和 SHA-256。

目标路径已有相同内容时直接补交确认；同名但哈希不同时保留远端并记录冲突，不覆盖本地文件。
