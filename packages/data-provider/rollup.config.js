import typescript from '@rollup/plugin-typescript';
import resolve from '@rollup/plugin-node-resolve';
import pkg from './package.json';
import peerDepsExternal from 'rollup-plugin-peer-deps-external';
import commonjs from '@rollup/plugin-commonjs';
import replace from '@rollup/plugin-replace';
import terser from '@rollup/plugin-terser';

const plugins = [
  peerDepsExternal(),
  resolve({
    extensions: ['.ts', '.tsx', '.js', '.jsx', '.json'],
  }),
  replace({
    __IS_DEV__: process.env.NODE_ENV === 'development',
  }),
  typescript({
    tsconfig: './tsconfig.json',
    compilerOptions: {
      // Allow Rollup + plugin to emit; root tsconfig has noEmit: true for IDE-only checks
      noEmit: false,
      importHelpers: true,
    },
    exclude: ['node_modules/**'],
  }),
  commonjs(),
  terser(),
];

export default [
  {
    input: 'src/index.ts',
    output: [
      {
        file: pkg.main,
        format: 'cjs',
        sourcemap: true,
        exports: 'named',
      },
      {
        file: pkg.module,
        format: 'esm',
        sourcemap: true,
        exports: 'named',
      },
    ],
    ...{
      external: [
        ...Object.keys(pkg.dependencies || {}),
        ...Object.keys(pkg.devDependencies || {}),
        ...Object.keys(pkg.peerDependencies || {}),
        'react',
        'react-dom',
      ],
      preserveSymlinks: true,
      plugins,
    },
  },
  // Separate bundle for react-query related part
  {
    input: 'src/react-query/index.ts',
    output: [
      {
        file: 'dist/react-query/index.es.js',
        format: 'esm',
        exports: 'named',
        sourcemap: true,
      },
    ],
    external: [
      ...Object.keys(pkg.dependencies || {}),
      ...Object.keys(pkg.devDependencies || {}),
      ...Object.keys(pkg.peerDependencies || {}),
      'react',
      'react-dom',
      // '@because/data-provider', // Marking main part as external
    ],
    preserveSymlinks: true,
    plugins,
  },
];
