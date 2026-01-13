const esbuild = require('esbuild')

const Config = {
  target: 'esnext',
  bundle: true,
  entryPoints: [
    { in: 'src/index.ts', out: 'cjs/index' }
  ],
}

esbuild.build({
  ...Config,
  platform: 'node',
  format: 'cjs',
  outdir: 'dist',
})
