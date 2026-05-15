const fs = require('fs-extra');

async function postBuild() {
  try {
    await fs.copy('public/assets', 'dist/assets');
    await fs.copy('public/robots.txt', 'dist/robots.txt');
    if (await fs.pathExists('public/web-apps')) {
      await fs.copy('public/web-apps', 'dist/web-apps');
    }
    if (await fs.pathExists('public/sdkjs')) {
      await fs.copy('public/sdkjs', 'dist/sdkjs');
    }
    if (await fs.pathExists('public/wasm')) {
      await fs.copy('public/wasm', 'dist/wasm');
    }
    console.log('✅ PWA icons and robots.txt copied successfully. Glob pattern warnings resolved.');
  } catch (err) {
    console.error('❌ Error copying files:', err);
    process.exit(1);
  }
}

postBuild();
