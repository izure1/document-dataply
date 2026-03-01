![node.js workflow](https://github.com/izure1/document-dataply/actions/workflows/node.js.yml/badge.svg)
![Performance Benchmark](https://github.com/izure1/document-dataply/actions/workflows/benchmark.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

# Document-Dataply

> [!WARNING]
> **This project is currently in the Alpha stage.**
> APIs and internal structures may change significantly between versions. Use with caution in production environments.

`document-dataply` is a **pure JavaScript** high-performance document-oriented database library built on top of the [`dataply`](https://github.com/izure1/dataply) record storage engine. It is designed to handle **millions of rows** with high stability, providing a structured way to store, index, and query JSON-style documents.

## Key Features

- **Document-Oriented**: Store and retrieve JSON-style documents.
- **B+Tree Indexing**: Supports high-performance lookups using a B+Tree indexing engine.
- **Deep Indexing**: Index nested object fields and specific array elements (e.g., `user.profile.name` or `tags.0`).
- **Flexible Indexing Policies**: Supports full re-indexing for existing data or incremental indexing for future data.
- **ACID Transactions**: Reliable atomic operations with **WAL (Write-Ahead Logging)** and **MVCC (Multi-Version Concurrency Control)** support.
- **Modern Architecture**: Fully supports **Async/Await** and **Streaming**, making it ideal for modern high-concurrency server environments.
- **Rich Querying**: Supports comparison operators (`lt`, `gt`, `equal`, etc.) and pattern matching (`like`).

## Platform Compatibility

Built with pure JavaScript, `document-dataply` can be used in various environments:
- **Official Support**: Node.js, Electron, NW.js
- **Experimental Support**: Deno, Bun

## Data Types

Supports standard JSON data types:
- `string`, `number`, `boolean`, `null`
- Nested `object` and `array`

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
  const db = DocumentDataply.Define<MyDocument>()
    .Options({ wal: 'my-database.wal' })
    .Open('my-database.db');
  
  // Register indices before init (Recommended)
  await db.createIndex('name', { type: 'btree', fields: ['name'] });
  await db.createIndex('tags_0', { type: 'btree', fields: ['tags.0'] });
  
  // Composite Index support
  await db.createIndex('idx_name_age', { type: 'btree', fields: ['name', 'age'] });

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
    name: 'John Doe', // Shortcut for { name: { equal: 'John Doe' } }
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

### Dynamic Index Management

`document-dataply` supports creating indices at any time—whether before or after the database is initialized.

- **Pre-Init**: Creating an index before `db.init()` ensures that the database is ready with all necessary structures from the start.
- **Post-Init**: You can call `db.createIndex()` even after the database is already running. The library will automatically create the index and perform **backfilling** (populating the index with existing data) in the background.

```typescript
// Create a new index on an existing database
await db.createIndex('idx_new_field', { type: 'btree', fields: ['newField'] });
```

### Composite Indexing

You can create an index on multiple fields. This is useful for optimizing queries that filter or sort by multiple criteria.

```typescript
await db.createIndex('idx_composite', { 
  type: 'btree', 
  fields: ['category', 'price', 'status'] 
});
```

The sorting is performed element-by-element in the order defined in the `fields` array. If all values are equal, the system uses the internal `_id` as a fallback to ensure stable sorting.

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
| `match` | Full-text search (Requires FTS Index) |

For detailed operator usage, index constraints (including full scans), and sorting methods, see the [Query Guide (QUERY.md)](./docs/QUERY.md).

> [!IMPORTANT]
> **Full-Text Search (match)**: To use the `match` operator, you must configure the field as an FTS index (e.g., `{ type: 'fts', tokenizer: 'whitespace' }`). Standard boolean indices do not support `match`. See [QUERY.md](./docs/QUERY.md#4-full-text-search-fts-indexing) for details.

### Transactions

Ensure data integrity with ACID-compliant transactions. Use `commit()` and `rollback()` to process multiple operations atomically.

For detailed usage and error handling patterns, see the [Transaction Guide (TRANSACTION.md)](./docs/TRANSACTION.md).

### Updating and Deleting

`document-dataply` provides flexible ways to update or delete documents based on query results. All these operations are **Stream-based**, allowing you to handle millions of records without memory concerns.

- **Partial Update**: Modify only specific fields or use a function for dynamic updates.
- **Full Update**: Replace the entire document while preserving the original `_id`.
- **Delete**: Permanently remove matching documents from both storage and indices.

For details on streaming mechanisms and bandwidth optimization tips, see the [Stream Guide (STREAM.md)](./docs/STREAM.md).

### Schema Migration

As your document structure evolves, you can use the `migration()` method to safely update your database. This method uses a `schemeVersion` to track which migrations have been applied.

```typescript
await db.migration(1, async (tx) => {
  // Add a new index for an existing database
  await db.createIndex('age', { type: 'btree', fields: ['age'] }, tx);
});
```

For more details on handling database evolution, see the [Migration Guide (MIGRATION.md)](./docs/MIGRATION.md).

## Tips and Advanced Features

For more information on performance optimization and advanced features, see [TIPS.md](./docs/TIPS.md).

- **Query Optimization**: Automatic index selection for maximum performance.
- **Sorting and Pagination**: Detailed usage of `limit`, `orderBy`, and `sortOrder`.
- **Memory Management**: When to use `stream` vs `drain()`.
- **Performance**: Optimizing bulk data insertion using `insertBatch`.
- **Indexing Policies**: Dynamic index creation and automatic backfilling.
- **Composite Indexes**: Indexing multiple fields for complex queries.

## API Reference

### `db.createIndex(name, options, tx?)`
Registers or creates a named index. Can be called at any time.
- `options`: `{ type: 'btree', fields: string[] }` or `{ type: 'fts', fields: string, tokenizer: ... }`.
- `tx`: Optional transaction.
- Returns `Promise<this>` for chaining.

### `db.dropIndex(name, tx?)`
Removes a named index from the database.
- `name`: The name of the index to drop.
- `tx`: Optional transaction.
- Returns `Promise<this>` for chaining.
- Note: The internal `_id` index cannot be dropped.

### `db.init()`
Initializes the database and sets up system-managed indices. It also triggers backfilling for indices registered before `init()`.

### `db.migration(version, callback, tx?)`
Runs a migration callback if the current `schemeVersion` is lower than the target `version`.
- `version`: The target scheme version (number).
- `callback`: An async function `(tx: Transaction) => Promise<void>`.
- `tx`: Optional transaction.

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
Returns physical storage information and index metadata.
- Returns `Promise<{ pageSize, pageCount, rowCount, indices, schemeVersion }>`
- `indices`: List of user-defined index names.
- `schemeVersion`: The current schema version of the database.

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
