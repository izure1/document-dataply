# 🏗️ Architecture Overview

`document-dataply` possesses a unique core architecture designed to efficiently manage server resources when processing large-scale data. This document provides a concise explanation of the internal engine's operating principles.

---

## 1. Cost-Based Optimizer

When searching data, reading through all documents from start to finish (Full Scan) causes severe speed degradation, and therefore is strictly not supported at the engine level. All search and sort operations must pass through an index, and the engine's built-in **Optimizer** calculates the shortest retrieval path.

- **Selectivity Evaluation**: When receiving a query with multiple conditions, the engine statistically predicts which field will yield the fewest results and searches it first. This drastically reduces unnecessary operations and boosts speed.
- **Early Exit**: When using the `limit` parameter, the overall scan process is immediately halted as soon as the target count is reached, conserving server resources.

---

## 2. Built-in Full-Text Search (FTS)

This feature is designed for finding specific words within documents, rather than relying on simple exact matches.

- **Periodic Token Collection**: When data is inserted, the background **`IntervalAnalysisProvider`** automatically runs at set intervals to break down text into token units (supporting both **`whitespace`** and **`ngram`** with configurable **`gramSize`**) and updates their frequencies.
- Users can manually call the `flushAnalysis()` method to immediately reflect the latest statistics (Flush) if necessary.
  ```typescript
  // Force immediate update of FTS statistics (Token collection)
  await db.flushAnalysis();
  ```
- During keyword searches, instead of scanning entire documents, the engine instantly pinpoints the target documents by referring to the frequency table (index) built by this analyzer.

---

## 3. Memory Optimization: Chunk & Stream

Loading millions of records into an array in memory at once can cause the server to crash due to out-of-memory (OOM) errors. This core technology prevents such scenarios.

- **Dynamic Free Memory Tracking**: The database checks real-time available free memory.
- **Dynamic Chunking**: While the user might request the entire dataset, the internal engine sequentially fetches data from the disk in safe units (chunks) that it calculates based on current capacity. This allows querying virtually infinite amounts of data while maintaining a constant memory footprint.

---

## 4. Data Integrity: Concurrency Control and MVCC

This defends against data corruption that can occur when multiple operational logics intersect.

- **Parallel Reads and Serialized Writes**: Read operations in the database can be invoked in parallel without thread limitations. However, write operations such as inserts, updates, and deletes are internally serialized and executed strictly sequentially to ensure data safety.
- **Safe Concurrency Control (MVCC)**: If another process requests a read while data is being written, the engine does not block it with a lock. Instead, it temporarily provides the past version (snapshot) prior to the modification, thereby radically reducing conflicts and delays.
- **I/O Bottleneck Resolution**: This leads to groundbreaking reductions in processing time, especially in large-scale Batch Insert environments.
