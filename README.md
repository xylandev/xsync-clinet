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

每个 worker 使用独立的 TCP 连接，不再通过 HTTP/2 把所有下载挤在一条连接上。

- **认领**：一次最多认领 `max_batch` 个对象（默认 8），队列为空时在服务端长轮询 `long_poll`（默认 20 秒），不会空转。
- **续租**：按剩余租约时间的 1/3 续租；服务端表示租约已失效时，立即停止下载，不会与接手的下载端同时写文件。
- **续传**：下载缓冲区由对象池复用，写盘的同时计算 SHA-256。每 5 秒 fsync 一次，并把已落盘的偏移和 SHA-256 中间状态写进检查点，续传时不需要重读已下载的部分。
- **机械盘**：建议把 `concurrency` 从 4 起步调整，避免多个大文件并发写入造成寻道抖动。

旧版独立下载配置仍兼容，可继续使用 `xsync-client run --config /etc/xsync-client/config.yaml`；新部署建议直接使用服务端连接包。

客户端在目标目录的 `.xsync/` 中保存状态：

- `partial/`：进行中的下载和检查点，文件加独占锁，同一个对象不会被两个写入者交错写入；超过 `partial_ttl`（7 天）的残留自动清理；
- `meta/`：每个路径最近一次交付的版本；
- `conflicts/`：被替换下来的非本客户端文件。

下载完成后依次执行：

1. 校验长度和 SHA-256；
2. `fsync` partial 文件，设置文件权限（`file_mode`，默认 0640）；
3. 通过硬链接原子落盘，并 `fsync` 父目录；
4. 向服务端提交对象 ID、租约、长度和 SHA-256。

## 同名文件与失败处理

目标路径已经存在时：

- **内容相同**：直接补交确认。常见于提交前崩溃后的重新投递。
- **已交付过更新的版本**（按服务端版本号判断）：这个较旧的版本直接确认删除，不会覆盖新文件。
- **更新的版本**：原子替换本客户端之前交付的文件。
- **不是本客户端交付的文件**：按 `conflict` 配置处理：
  - `backup`（默认）：把原文件移到 `.xsync/conflicts/`，再落盘；
  - `overwrite`：直接替换；
  - `skip`：交还服务端，由服务端停入死信。

失败分两类：

- **不可能成功的**（路径非法、路径经过符号链接、冲突策略拒绝）：交还服务端并标记为永久失败，对象进入死信，不再反复投递；
- **其他错误**：按投递次数指数退避后重试（10 秒起，最多 10 分钟）。

死信可以用客户端命令查看和处理：

```bash
xsync-client parked  --client-config account.yaml
xsync-client requeue --client-config account.yaml --id <object>
xsync-client drop    --client-config account.yaml --id <object>
```

收到 `SIGTERM` 时，客户端把手里的租约全部交还，不计入投递次数。

服务端下发的路径会经过以下检查：

- 拒绝绝对路径、`..`、控制字符，以及指向 `.xsync/` 的路径；
- 拒绝经过符号链接的路径；
- 并发下载预先预留磁盘空间，多个下载不会同时通过空间检查后把盘写满。
