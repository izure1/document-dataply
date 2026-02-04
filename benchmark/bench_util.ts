import * as fs from 'node:fs'
import * as path from 'node:path'

export interface BenchResult {
  name: string
  times: number[]
  avg: number
  min: number
  max: number
}

/**
 * Runs a benchmark function multiple times and calculates statistics.
 */
export async function runBenchmark(
  name: string,
  fn: () => Promise<void>,
  iterations: number = 5
): Promise<BenchResult> {
  const times: number[] = []

  console.log(`\n[Benchmark: ${name}] Running ${iterations} iterations...`)

  for (let i = 0; i < iterations; i++) {
    const start = performance.now()
    await fn()
    const end = performance.now()
    const duration = end - start
    times.push(duration)
    process.stdout.write(`.`)
  }
  process.stdout.write(` Done.\n`)

  const avg = times.reduce((a, b) => a + b, 0) / times.length
  const min = Math.min(...times)
  const max = Math.max(...times)

  return { name, times, avg, min, max }
}

/**
 * Prints a summary table of benchmark results.
 */
export function printSummary(results: BenchResult[]) {
  console.log('\n' + '='.repeat(80))
  console.log('Benchmark Summary (ms)')
  console.log('-'.repeat(80))
  console.log(
    `${'Name'.padEnd(30)} | ${'Avg'.padStart(10)} | ${'Min'.padStart(10)} | ${'Max'.padStart(10)}`
  )
  console.log('-'.repeat(80))

  for (const r of results) {
    console.log(
      `${r.name.padEnd(30)} | ${r.avg.toFixed(2).padStart(10)} | ${r.min.toFixed(2).padStart(10)} | ${r.max.toFixed(2).padStart(10)}`
    )
  }
  console.log('='.repeat(80) + '\n')
}

/**
 * Clean up database files.
 */
export function cleanupDb(dbPath: string) {
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath)
  }
  const dir = path.dirname(dbPath)
  const base = path.basename(dbPath)
  if (fs.existsSync(dir)) {
    const files = fs.readdirSync(dir)
    for (const file of files) {
      if (file.startsWith(base + '.')) {
        fs.unlinkSync(path.join(dir, file))
      }
    }
  }
}

/**
 * Saves benchmark results to a JSON file for github-action-benchmark.
 */
export function saveResultsJson(results: BenchResult[], filePath: string) {
  const data = results.map((r) => ({
    name: r.name,
    unit: 'ms',
    value: parseFloat(r.avg.toFixed(2))
  }))
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2))
  console.log(`\nBenchmark results saved to ${filePath}`)
}
