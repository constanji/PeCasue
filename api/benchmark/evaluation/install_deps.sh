#!/bin/bash
# 安装评估脚本所需的 Python 依赖

echo "=========================================="
echo "安装 BIRD 评估脚本 Python 依赖"
echo "=========================================="
echo ""

# 检查 Python 3
if ! command -v python3 &> /dev/null; then
    echo "❌ 错误: 未找到 python3，请先安装 Python 3"
    exit 1
fi

echo "✅ 找到 Python: $(python3 --version)"
echo ""

# 检查 pip3
if command -v pip3 &> /dev/null; then
    PIP_CMD="pip3"
elif python3 -m pip --version &> /dev/null; then
    PIP_CMD="python3 -m pip"
else
    echo "❌ 错误: 未找到 pip3，请先安装 pip"
    exit 1
fi

echo "✅ 使用: $PIP_CMD"
echo ""

# 安装依赖
echo "正在安装依赖..."
$PIP_CMD install -r requirements.txt

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✅ 依赖安装成功！"
    echo "=========================================="
    echo ""
    echo "验证安装..."
    python3 -c "from func_timeout import func_timeout, FunctionTimedOut; print('✅ func-timeout: OK')" 2>/dev/null || echo "❌ func-timeout: 未安装"
    python3 -c "import pymysql; print('✅ pymysql: OK')" 2>/dev/null || echo "❌ pymysql: 未安装"
    python3 -c "import psycopg2; print('✅ psycopg2: OK')" 2>/dev/null || echo "❌ psycopg2: 未安装"
    python3 -c "import numpy; print('✅ numpy: OK')" 2>/dev/null || echo "❌ numpy: 未安装"
    python3 -c "import tqdm; print('✅ tqdm: OK')" 2>/dev/null || echo "❌ tqdm: 未安装"
else
    echo ""
    echo "=========================================="
    echo "❌ 依赖安装失败"
    echo "=========================================="
    exit 1
fi
