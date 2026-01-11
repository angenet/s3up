# S3UP

ComfyUI 插件：将输出图像保存到 S3 兼容对象存储，并提供失败落盘补传机制。

## 功能

- S3 兼容存储上传（AWS S3、MinIO、OSS、COS 等）
- 上传失败自动落盘并后台线程补传
- 节点支持交互式配置，默认中文显示

## 安装

1. 将本仓库放入 ComfyUI 的 `custom_nodes/` 目录
2. 安装依赖：

```bash
pip install -r requirements.txt
```

3. 启动 ComfyUI

提示：如果安装了 ComfyUI-Manager，它会自动读取
`requirements.txt` 并安装依赖。

## 使用

- 在节点列表中选择 `S3存储`
- 填写存储配置或使用环境变量
- 将输出图像连接到该节点即可保存

## 配置

优先级：节点输入 > 环境变量。

### 环境变量

- `S3_ENDPOINT`：服务地址（可选）
- `S3_BUCKET`：桶名称（必填）
- `S3_REGION`：区域代码（默认 `us-east-1`）
- `S3_ACCESS_KEY_ID`：访问密钥（必填）
- `S3_SECRET_ACCESS_KEY`：访问密钥密文（必填）
- `S3_USE_SSL`：是否启用 HTTPS（默认 `true`）
- `S3_FORCE_PATH_STYLE`：路径风格（默认 `false`）
- `S3_PREFIX`：对象前缀（默认 `comfyui`）
- `S3_TIMESTAMP_PREFIX`：时间戳前缀（默认 `false`）
- `S3_SPOOL_DIR`：失败暂存目录（默认 `custom_nodes/s3up/spool`）
- `S3_RETRY_MAX`：最大重试次数（默认 `5`）
- `S3_RETRY_BACKOFF_SECONDS`：退避等待秒数（默认 `2`）
- `S3_RETRY_INTERVAL_SECONDS`：扫描间隔秒数（默认 `5`）
- `S3_RETRY_CONCURRENCY`：补传并发（默认 `1`）

## 目录结构

```
custom_nodes/s3up/
  domain/
  infrastructure/
  nodes/
  __init__.py
  requirements.txt
```

## 安全提示

- 不要在代码中硬编码密钥
- 建议使用专用子账号与最小权限策略
