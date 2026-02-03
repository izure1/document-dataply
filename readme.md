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

`document-dataply` supports various comparison operators.

| Operator | Description |
| :--- | :--- |
| `lt` | Less than |
| `lte` | Less than or equal to |
| `gt` | Greater than |
| `gte` | Greater than or equal to |
| `equal` | Equal to |
| `notEqual` | Not equal to |
| `like` | SQL-style pattern matching (e.g., `Jo%`) |
| `or` | If any value in the array is satisfied |

Example of a complex query:
```typescript
const users = await db.select({
  age: { gt: 18, lt: 65 },
  'address.city': 'Seoul',
  tags: { or: ['vip', 'premium'] }
}).drain();
```

> [!IMPORTANT]
> **Query Constraints**: Query conditions (`lt`, `gt`, `equal`, etc.) can only be used on fields explicitly indexed during initialization.
> 
> **If a field in the query is not indexed, that condition will be ignored.**
> 
> If you need to filter by unindexed fields, you should first retrieve the documents and then use JavaScript's native `.filter()` method.
```typescript
const results = await db.select({ /* indexed fields only */ }).drain();
const filtered = results.filter(doc => doc.unindexedField === 'some-value');
```

### Transactions

To ensure the atomicity of multiple operations, use transactions.

```typescript
const tx = db.createTransaction();
try {
  await db.insert({ name: 'Alice', age: 25 }, tx);
  await db.insert({ name: 'Bob', age: 28 }, tx);
  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}
```

### Updating and Deleting

`document-dataply` provides flexible ways to update or delete documents matching a query. All these operations are performed in a memory-efficient streaming manner.

#### Partial Update
Updates only specified fields of the matching documents.

```typescript
// Using an object to merge
const count = await db.partialUpdate(
  { name: 'John Doe' },
  { status: 'active', updatedAt: Date.now() }
);

// Using a function for dynamic updates
const count = await db.partialUpdate(
  { age: { lt: 20 } },
  (doc) => ({ age: doc.age + 1 })
);
```

#### Full Update
Completely replaces the documents matching the query, while preserving their original `_id`.

```typescript
const count = await db.fullUpdate(
  { name: 'John Doe' },
  { name: 'John Smith', age: 31, location: 'New York' }
);
```

#### Delete
Removes documents matching the query from both the index and storage.

```typescript
const deletedCount = await db.delete({ status: 'inactive' });
```

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
`options.indices` is an object where keys are field names and values are booleans indicating whether to index.

### `db.init()`
Initializes the database, sets up internal metadata, and prepares indices.

### `db.insert(document, tx?)`
Inserts a single document. Returns the `_id` (`number`) of the document.

### `db.insertBatch(documents, tx?)`
Inserts multiple documents efficiently. Returns an array of `_ids` (`number[]`).

### `db.select(query, options?, tx?)`
Searches for documents matching the query.
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

## License

MIT
