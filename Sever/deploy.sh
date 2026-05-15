#!/usr/bin/env bash
# PeCause — 仅依赖「本目录 + 两个业务镜像」的服务器部署
# 镜像：pecause:latest、pecause-pipeline-svc:latest
#
# 用法：
#   ./deploy.sh                 检查镜像后 compose up -d
#   ./deploy.sh --load-images   若 ./images/*.tar 存在则先 docker load，再启动
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOAD_IMAGES=false
for arg in "$@"; do
  case "$arg" in
    --load-images) LOAD_IMAGES=true ;;
    --help|-h)
      sed -n '1,12p' "$0"
      exit 0
      ;;
    *)
      echo "未知参数: $arg（可用 --load-images）" >&2
      exit 1
      ;;
  esac
done

echo "=========================================="
echo "  PeCause 部署（Sever/）"
echo "=========================================="
echo ""

if ! docker info &>/dev/null; then
  echo "Docker 不可用，请先启动 Docker 服务。" >&2
  exit 1
fi
if ! docker compose version &>/dev/null; then
  echo "未找到「docker compose」插件，请安装 Docker Compose V2。" >&2
  exit 1
fi

need_file() {
  if [[ ! -f "$1" ]]; then
    echo "缺少文件: $1" >&2
    return 1
  fi
}

if [[ ! -f ".env" ]]; then
  if [[ -f ".env.example" ]]; then
    echo "未找到 .env，从 .env.example 复制…"
    cp .env.example .env
    echo "请编辑 .env（至少设置 DOMAIN_CLIENT、DOMAIN_SERVER、JWT 密钥与 API Key），然后重新运行本脚本。"
    exit 1
  fi
  echo "缺少 .env 且无 .env.example，无法继续。" >&2
  exit 1
fi

need_file "Because.yaml" || {
  echo "请存放 Because.yaml 于本目录。" >&2
  exit 1
}
need_file "deploy-compose.yml" || exit 1
need_file "client/nginx.conf" || {
  echo "缺少 client/nginx.conf。" >&2
  exit 1
}

migrate_data_node_if_needed() {
  if [[ -d "data-node" && ! -d "data-node-pecause" ]]; then
    echo "检测到旧目录 data-node，重命名为 data-node-pecause（与 compose 一致）…"
    mv "data-node" "data-node-pecause"
  elif [[ -d "data-node" && -d "data-node-pecause" ]]; then
    echo "提示: 目录 data-node 与 data-node-pecause 同时存在；Mongo 挂载使用 data-node-pecause。"
  fi
}

migrate_data_node_if_needed

mkdir -p images uploads logs pipeline-data data-node-pecause specs

IMG_PECAUSE="pecause"
TAG_PECAUSE="latest"
IMG_PIPE="pecause-pipeline-svc"
TAG_PIPE="latest"

maybe_load_tar_images() {
  local found=false
  if compgen -G "images/*.tar" >/dev/null 2>&1; then found=true; fi
  if compgen -G "images/*.tar.gz" >/dev/null 2>&1; then found=true; fi
  if [[ "$LOAD_IMAGES" != true ]]; then
    return 0
  fi
  if [[ "$found" != true ]]; then
    echo "未找到 ./images/*.tar 或 *.tar.gz，跳过 load。"
    return 0
  fi
  echo "从 ./images 加载镜像…"
  local f
  for f in images/*.tar images/*.tar.gz; do
    [[ -f "$f" ]] || continue
    echo "  docker load -i \"$f\""
    docker load -i "$f"
  done
}

maybe_load_tar_images

if [[ "$LOAD_IMAGES" != true ]]; then
  if ! docker image inspect "${IMG_PECAUSE}:${TAG_PECAUSE}" &>/dev/null || ! docker image inspect "${IMG_PIPE}:${TAG_PIPE}" &>/dev/null; then
    if compgen -G "images/*.tar" >/dev/null 2>&1 || compgen -G "images/*.tar.gz" >/dev/null 2>&1; then
      echo "本地缺少业务镜像但发现 images 下的 tar，自动执行加载…"
      LOAD_IMAGES=true
      maybe_load_tar_images
    fi
  fi
fi

if ! docker image inspect "${IMG_PECAUSE}:${TAG_PECAUSE}" &>/dev/null; then
  echo "未找到镜像 ${IMG_PECAUSE}:${TAG_PECAUSE}" >&2
  echo "请先: docker load -i … 或传入 --load-images（需 tar 放于 ./images/）。" >&2
  exit 1
fi

if ! docker image inspect "${IMG_PIPE}:${TAG_PIPE}" &>/dev/null; then
  echo "未找到镜像 ${IMG_PIPE}:${TAG_PIPE}" >&2
  echo "请先: docker load -i … 或传入 --load-images。" >&2
  exit 1
fi

echo "校验 compose…"
docker compose -f deploy-compose.yml config >/dev/null

echo "启动服务…"
docker compose -f deploy-compose.yml up -d

echo ""
echo "完成。"
echo "  docker compose -f deploy-compose.yml ps"
echo "  docker compose -f deploy-compose.yml logs -f api"
echo ""
echo "持久化目录（务必备份）："
echo "  - pipeline-data   Pipeline（tasks.db、任务工作区、规则等）"
echo "  - data-node-pecause   MongoDB 数据"
echo "  uploads/、logs/、images/"
echo ""
echo "入口: Nginx http://<IP>:8180  |  API http://<IP>:4182"
