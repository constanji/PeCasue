#!/usr/bin/env node
/**
 * 将基准测试相关文件完整复制到 Bench 目录，保持根目录结构
 * 用法: node scripts/copy-benchmark-to-bench.js
 */

const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const DEST = path.join(ROOT, 'Bench');

const MANIFEST = [
  // api/benchmark 整个目录
  { src: 'api/benchmark', dest: 'api/benchmark', isDir: true },
  // api/server 下的 benchmark 相关
  { src: 'api/server/routes/benchmark.js', dest: 'api/server/routes/benchmark.js', isDir: false },
  { src: 'api/server/controllers/BenchmarkController.js', dest: 'api/server/controllers/BenchmarkController.js', isDir: false },
  { src: 'api/server/services/BenchmarkService.js', dest: 'api/server/services/BenchmarkService.js', isDir: false },
  { src: 'api/server/services/EvaluationService.js', dest: 'api/server/services/EvaluationService.js', isDir: false },
  { src: 'api/server/services/ModelClient.js', dest: 'api/server/services/ModelClient.js', isDir: false },
  // client 路由
  { src: 'client/src/routes/Benchmark.tsx', dest: 'client/src/routes/Benchmark.tsx', isDir: false },
  { src: 'client/src/routes/Result.tsx', dest: 'client/src/routes/Result.tsx', isDir: false },
];

function copyRecursive(src, dest) {
  const stat = fs.statSync(src);
  if (stat.isDirectory()) {
    if (!fs.existsSync(dest)) fs.mkdirSync(dest, { recursive: true });
    for (const name of fs.readdirSync(src)) {
      copyRecursive(path.join(src, name), path.join(dest, name));
    }
  } else {
    const dir = path.dirname(dest);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.copyFileSync(src, dest);
  }
}

function main() {
  console.log('复制基准测试文件到 Bench/ ...\n');
  let copied = 0;
  let skipped = 0;

  for (const { src, dest, isDir } of MANIFEST) {
    const srcPath = path.join(ROOT, src);
    const destPath = path.join(DEST, dest);

    if (!fs.existsSync(srcPath)) {
      console.log(`  [跳过] 源不存在: ${src}`);
      skipped++;
      continue;
    }

    const destDir = isDir ? destPath : path.dirname(destPath);
    if (!fs.existsSync(path.dirname(DEST))) fs.mkdirSync(path.dirname(DEST), { recursive: true });
    if (!fs.existsSync(destDir)) fs.mkdirSync(destDir, { recursive: true });

    if (isDir) {
      copyRecursive(srcPath, destPath);
      const count = countFiles(srcPath);
      console.log(`  [目录] ${src} -> ${dest} (${count} 个文件)`);
      copied += count;
    } else {
      fs.copyFileSync(srcPath, destPath);
      console.log(`  [文件] ${src} -> ${dest}`);
      copied++;
    }
  }

  // 生成 INSTALL.md（安装说明，给旧项目使用）
  const installPath = path.join(DEST, 'INSTALL.md');
  const installContent = [
    '# Bench 基准测试 - 安装说明',
    '',
    '将 Bench 内文件按路径复制到你的项目根目录后，按以下步骤完成集成。',
    '',
    '## 1. 复制文件',
    '',
    '将 Bench 下所有文件按**相同路径**复制到项目根目录，例如：',
    '',
    '```',
    'Bench/api/benchmark/          → 项目根/api/benchmark/',
    'Bench/api/server/routes/       → 项目根/api/server/routes/',
    'Bench/api/server/controllers/ → 项目根/api/server/controllers/',
    'Bench/api/server/services/    → 项目根/api/server/services/',
    'Bench/client/src/routes/      → 项目根/client/src/routes/',
    '```',
    '',
    '基准测试路由依赖 requireJwtAuth、checkAdmin、useAuthRedirect，使用你项目已有的即可（未包含在 Bench 中）。',
    '',
    '## 2. 后端集成',
    '',
    '在 `api/server/routes/index.js` 中：',
    '',
    '```js',
    'const benchmark = require("./benchmark");',
    '// ... 在 module.exports 中加入 benchmark',
    '```',
    '',
    '在 `api/server/index.js` 中（约 148 行附近）：',
    '',
    '```js',
    'app.use("/api/benchmark", routes.benchmark);',
    '```',
    '',
    '## 3. 前端集成',
    '',
    '在 `client/src/routes/index.tsx` 中：',
    '',
    '```js',
    'import Benchmark from "./Benchmark";',
    'import Result from "./Result";',
    '// 在路由 children 中加入：',
    '{ path: "benchmark", element: <Benchmark /> },',
    '{ path: "result/:taskId", element: <Result /> },',
    '```',
    '',
    '## 4. 导航入口',
    '',
    '在管理员菜单（如 AccountSettings.tsx）中加入「基准测试」入口，点击跳转 `/benchmark`。',
    '',
    '## 5. 依赖',
    '',
    '需有 @because/api、@because/data-provider、@because/data-schemas、passport、cookie 等。',
    '数据集见 api/benchmark/README.md，环境变量见 env.benchmark.example。',
  ].join('\n');
  fs.writeFileSync(installPath, installContent, 'utf8');
  console.log('\n  [生成] Bench/INSTALL.md');

  // 生成 INTEGRATION.md
  const readmePath = path.join(DEST, 'INTEGRATION.md');
  const readme = [
    '# Bench - 基准测试模块',
    '',
    '本目录由 scripts/copy-benchmark-to-bench.js 从主项目复制而来，保持根目录结构。',
    '',
    '## 目录结构',
    '',
    'Bench/',
    '├── api/',
    '│   ├── benchmark/          # 数据集、评估脚本、知识库',
    '│   └── server/',
    '│       ├── controllers/BenchmarkController.js',
    '│       ├── routes/benchmark.js',
    '│       └── services/BenchmarkService.js, EvaluationService.js, ModelClient.js',
    '├── client/src/routes/',
    '│   ├── Benchmark.tsx, Result.tsx',
    '├── env.benchmark.example',
    '├── .gitignore',
    '├── INSTALL.md              # 安装说明（给旧项目用）',
    '└── INTEGRATION.md',
    '',
    '## 安装',
    '',
    '详见 INSTALL.md。',
  ].join('\n');
  fs.writeFileSync(readmePath, readme, 'utf8');
  console.log('  [生成] Bench/INTEGRATION.md');

  // 生成 env.benchmark.example
  const envPath = path.join(DEST, 'env.benchmark.example');
  const envContent = [
    '# 基准测试 (Benchmark) 相关环境变量',
    '# 从主项目 .env.example 提取，供参考',
    '',
    '# 数据集放置于 api/benchmark/data/，评估脚本在 api/benchmark/evaluation/。',
    '# 若使用 MySQL/PostgreSQL 执行准确率(EX)评估，需在对应 Python 脚本中配置：',
    '# MYSQL_HOST=localhost',
    '# MYSQL_PORT=3306',
    '# MYSQL_USER=root',
    '# MYSQL_PASSWORD=',
    '# 端点 API Key 由 Because.yaml 与 Endpoints 配置提供。',
  ].join('\n');
  fs.writeFileSync(envPath, envContent, 'utf8');
  console.log('  [生成] Bench/env.benchmark.example');

  // 生成 .gitignore
  const gitignorePath = path.join(DEST, '.gitignore');
  const gitignoreContent = [
    'api/benchmark/results/',
    'api/benchmark/data/*.json',
    'benchmark_results/',
  ].join('\n');
  fs.writeFileSync(gitignorePath, gitignoreContent, 'utf8');
  console.log('  [生成] Bench/.gitignore');

  console.log(`\n完成。共复制 ${copied} 个文件，跳过 ${skipped} 项。`);
}

function countFiles(dir) {
  let n = 0;
  for (const name of fs.readdirSync(dir)) {
    const p = path.join(dir, name);
    if (fs.statSync(p).isDirectory()) n += countFiles(p);
    else n++;
  }
  return n;
}

main();
