![node.js workflow](https://github.com/izure1/document-dataply/actions/workflows/node.js.yml/badge.svg)
![Performance Benchmark](https://github.com/izure1/document-dataply/actions/workflows/benchmark.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

# Document-Dataply

> [!WARNING]
> **This project is currently in the Alpha stage.**
> API structures may change in future updates. Please ensure sufficient testing before deploying to production environments.

## 📖 Introduction

`document-dataply` is a high-performance **Document Database** implemented in pure JavaScript. 
It prevents server memory (RAM) exhaustion even when handling millions of records, and supports ultra-fast searching and batch processing systems. It can be used intuitively without the complex tuning required by RDBMS.

### ✨ Key Features

- **JSON Document Based**: Reads and writes data in raw JavaScript Object (JSON) format. Easily supports querying deeply nested object arrays (`a.b.c`).
- **B+Tree Indexing**: Built-in indexing system to drastically improve query speed (O(log N)).
- **Cost-Based Optimizer**: The engine automatically analyzes data distribution during query requests to find the most optimal execution path.
- **Full-Text Search (FTS)**: Provides a real-time search engine to find specific keywords within long text, such as article contents.
- **Safe Transactions**: If an error occurs during an operation, all modifications are fully rolled back, completely preventing data inconsistency.

---

## 💻 Installation

```bash
npm install document-dataply
```

- **Supported Environments**: Node.js, Electron, NW.js (Experimental support for Bun, Deno)

---

## 🚀 Quick Start

Here is the most basic guide for creating a database, registering an index, and querying data.

```typescript
import { DocumentDataply } from 'document-dataply';

// 1. Define Document Type (Schema)
type UserProfile = {
  name: string;
  age: number;
  tags: string[];
}

async function main() {
  // 2. Create instance and connect to file
  const db = DocumentDataply.Define<UserProfile>()
    .Options({ wal: 'my-database.wal' })
    .Open('my-database.db');
  
  await db.init();

  // 3. Create Index (Specify 'name' field to improve search speed)
  await db.migration(1, async (tx) => {
    await db.createIndex('idx_name', { type: 'btree', fields: ['name'] }, tx);
    console.log('Index created successfully');
  });

  // 4. Insert Single Document
  await db.insert({
    name: 'IU',
    age: 30,
    tags: ['Singer', 'Actor']
  });

  // 5. Query Data
  const query = db.select({
    name: 'IU' 
  });

  // Fetch results as an array at once
  const results = await query.drain();
  console.log(results); 
  // Output: [{ _id: 1, name: 'IU', age: 30, tags: ['Singer', 'Actor'] }]

  // 6. Close the connection
  await db.close();
}

main();
```

---

## 💡 Common Patterns

### Batch Processing for Performance
```typescript
const largeData = Array.from({ length: 10000 }, (_, i) => ({
  name: `User_${i}`,
  age: Math.floor(Math.random() * 50),
  tags: ['imported']
}));

// Much faster than individual inserts
await db.insertBatch(largeData);
```

### Complex Queries with Operators
```typescript
const activeSeniors = await db.select({
  age: { gte: 65 },
  status: { equal: 'active' },
  name: { like: 'John%' }
}).drain();
```

---

## 📚 Detailed Manual

Please refer to the following documents for detailed usage of each feature and the library's core architecture.

1. [**Query & Index Guide**](./docs/QUERY_AND_INDEX.md)
   - Explains various search operators (`lt`, `gt`, etc.) and how to configure composite indices.
2. [**Mutation & Transaction**](./docs/MUTATION_AND_TRANSACTION.md)
   - Covers batch insertion techniques and transaction rollback principles.
3. [**Stream Architecture (Memory Optimization)**](./docs/STREAM.md)
   - Analyzes the secrets of the stream-based architecture that prevents OOM (Out Of Memory) crashes.
4. [**Architecture Overview**](./docs/ARCHITECTURE.md)
   - Deeply explores the internal workings of the database core, such as the optimizer.

---

## 📊 Library Benchmark

`document-dataply` strictly measures speed upon every code merge to defend against performance degradation.

[Check Real-time Performance on Main Branch](https://izure1.github.io/document-dataply/dev/bench/)

## 📜 License
MIT License
