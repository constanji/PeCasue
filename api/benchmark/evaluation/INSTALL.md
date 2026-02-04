# 评估脚本依赖安装

## 安装 Python 依赖

评估脚本需要以下 Python 包：

```bash
# 使用 pip3 安装（推荐）
pip3 install -r requirements.txt

# 或者使用 python3 -m pip
python3 -m pip install -r requirements.txt

# 或者单独安装
pip3 install func-timeout pymysql psycopg2-binary numpy tqdm
```

## 验证安装

运行以下命令验证依赖是否正确安装：

```bash
python3 -c "from func_timeout import func_timeout, FunctionTimedOut; print('func-timeout installed successfully')"
python3 -c "import pymysql; print('pymysql installed successfully')"
python3 -c "import psycopg2; print('psycopg2 installed successfully')"
```

## 常见问题

### ModuleNotFoundError: No module named 'func_timeout'

**解决方案：**
```bash
pip install func-timeout
```

### 如果使用 conda 环境

```bash
conda install -c conda-forge func-timeout
pip install pymysql psycopg2-binary numpy tqdm
```
