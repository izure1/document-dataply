![node.js workflow](https://github.com/izure1/document-dataply/actions/workflows/node.js.yml/badge.svg)
![Performance Benchmark](https://github.com/izure1/document-dataply/actions/workflows/benchmark.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

# Document-Dataply

> [!WARNING]
> **This project is currently in the Alpha stage.**
> APIs and internal structures may change significantly between versions. Use with caution in production environments.

`document-dataply` is a high-performance document-oriented database library built on top of the [`dataply`](https://github.com/izure1/dataply) record storage engine. It provides a structured way to store, index, and query JSON-style documents, supporting transactions and complex field indexing.

## Key Features

- **Document-Oriented**: Store and retrieve JSON-style documents.
- **B+Tree Indexing**: Supports high-performance lookups using a B+Tree indexing engine.
- **Deep Indexing**: Index nested object fields and specific array elements (e.g., `user.profile.name` or `tags.0`).
- **Flexible Indexing Policies**: Supports full re-indexing for existing data or incremental indexing for future data.
- **Transactions**: ACID-compliant transactions for atomic operations.
- **Rich Querying**: Supports comparison operators (`lt`, `gt`, `equal`, etc.) and pattern matching (`like`).

## Installation

```bash
npm install document-dataply
```

## Quick Start

```typescript
import { DocumentDataply } from 'document-dataply';

type MyDocument = {
  name: string;
  age: number;
  tags: string[];
}

async function main() {
  const db = DocumentDataply.Define<MyDocument>().Options({
    wal: 'my-database.wal',
    indices: {
      name: true, // Index both existing and new data
      age: false, // Index only new data
      'tags.0': true // Index the first element of the 'tags' array
    }
  }).Open('my-database.db');

  // Initialize database
  await db.init();

  // Insert document
  const id = await db.insert({
    name: 'John Doe',
    age: 30,
    tags: ['admin', 'developer']
  });

  // Query document
  const query = db.select({
    name: 'John Doe',
    age: { gte: 25 }
  })

  // Get all results
  const allResults = await query.drain();
  // Or iterate through results
  for await (const doc of query.stream) {
    console.log(doc);
  }

  console.log(allResults);

  // Close database
  await db.close();
}

main();
```

## Advanced Usage

### Indexing Policies

When defining indices in the `options`, you can specify a boolean value.

- `true`: The library indexes all existing documents for that field during `init()`, and also indexes all subsequent insertions.
- `false`: The library only indexes documents inserted after this configuration.

> [!NOTE]
> `db.init()` automatically performs a backfilling process for fields marked as `true`.

### Batch Insertion

To efficiently insert multiple documents, use the following:

```typescript
const ids = await db.insertBatch([
  { name: 'Alice', age: 25, tags: ['user'] },
  { name: 'Bob', age: 28, tags: ['moderator'] }
]);
```

### Querying

`document-dataply` supports powerful search capabilities based on B+Tree indexing.

| Operator | Description |
| :--- | :--- |
| `lt`, `lte`, `gt`, `gte` | Comparison operations |
| `equal`, `notEqual` | Equality check |
| `like` | Pattern matching |
| `or` | Matching within an array |

For detailed operator usage, index constraints (including full scans), and sorting methods, see the [Query Guide (QUERY.md)](./docs/QUERY.md).

### Transactions

Ensure data integrity with ACID-compliant transactions. Use `commit()` and `rollback()` to process multiple operations atomically.

For detailed usage and error handling patterns, see the [Transaction Guide (TRANSACTION.md)](./docs/TRANSACTION.md).

### Updating and Deleting

`document-dataply` provides flexible ways to update or delete documents based on query results. All these operations are **Stream-based**, allowing you to handle millions of records without memory concerns.

- **Partial Update**: Modify only specific fields or use a function for dynamic updates.
- **Full Update**: Replace the entire document while preserving the original `_id`.
- **Delete**: Permanently remove matching documents from both storage and indices.

For details on streaming mechanisms and bandwidth optimization tips, see the [Stream Guide (STREAM.md)](./docs/STREAM.md).

## Tips and Advanced Features

For more information on performance optimization and advanced features, see [TIPS.md](./docs/TIPS.md).

- **Query Optimization**: Automatic index selection for maximum performance.
- **Sorting and Pagination**: Detailed usage of `limit`, `orderBy`, and `sortOrder`.
- **Memory Management**: When to use `stream` vs `drain()`.
- **Performance**: Optimizing bulk data insertion using `insertBatch`.
- **Indexing Policies**: Deep dive into index backfilling and configuration.

## API Reference

### `DocumentDataply.Define<T>().Options(options).Open(file)`
Creates or opens a database instance. `T` defines the document structure.
`options.indices` is an object where keys are field names and values are booleans indicating the [Indexing Policy](#indexing-policies).

### `db.init()`
Initializes the database, sets up internal metadata, and prepares indices.

### `db.insert(document, tx?)`
Inserts a single document. Each document is automatically assigned a unique, immutable `_id` field. The method returns this `_id` (`number`).

### `db.insertBatch(documents, tx?)`
Inserts multiple documents efficiently. Returns an array of `_ids` (`number[]`).

### `db.select(query, options?, tx?)`
Searches for documents matching the query. Passing an empty object (`{}`) as the `query` retrieves all documents.
Returns an object `{ stream, drain }`.
- `stream`: An async iterator to traverse results one by one.
- `drain()`: A promise that resolves to an array of all matching documents.

### `db.partialUpdate(query, newFields, tx?)`
Partially updates documents matching the query. `newFields` can be a partial object or a function that returns a partial object. Returns the number of updated documents.

### `db.fullUpdate(query, newDocument, tx?)`
Fully replaces documents matching the query while preserving their `_id`. Returns the number of updated documents.

### `db.delete(query, tx?)`
Deletes documents matching the query. Returns the number of deleted documents.

### `db.getMetadata(tx?)`
Returns physical storage information (number of pages, number of rows, etc.).

### `db.createTransaction()`
Returns a new `Transaction` object.

### `db.close()`
Flushes changes and closes the database files.

## Benchmark

Automated benchmarks are executed on every push to the `main` branch and for every pull request. This ensures that performance regressions are detected early.

- **Dataset**: 10,000 documents
- **Operations**: Batch Insert, Indexed Select, Partial Update, Full Update, Delete

### Performance Trend

You can view the real-time performance trend and detailed metrics on our [Performance Dashboard](https://izure1.github.io/document-dataply/dev/bench/).

> [!TIP]
> **Continuous Monitoring**: We use `github-action-benchmark` to monitor performance changes. For every PR, a summary of the performance impact is automatically commented to help maintain high efficiency.

## License

MIT
