# Document-Dataply

> [!WARNING]
> **This project is currently in the Alpha stage.**
> APIs and internal structures may change significantly between versions. Use with caution in production environments.

`document-dataply` is a high-performance, document-oriented database library built on top of the [`dataply`](https://github.com/izure1/dataply) record storage engine. It provides a structured way to store, index, and query JSON-like documents with support for transactions and complex field indexing.

## Features

- **Document-Oriented**: Store and retrieve JSON-like documents.
- **B+Tree Indexing**: High-performance lookups using B+Tree indexing engine.
- **Deep Indexing**: Index nested object fields and specific array elements (e.g., `user.profile.name` or `tags.0`).
- **Flexible Indexing Policy**: Support for full re-indexing of existing data or incremental indexing for future data.
- **Transactions**: ACID-compliant transactions for atomic operations.
- **Rich Querying**: Support for comparison operators (`lt`, `gt`, `equal`, etc.) and pattern matching (`like`).

## Installation

```bash
npm install document-dataply
# or
yarn add document-dataply
```

## Quick Start

```typescript
import { DocumentDataply } from 'document-dataply';

async function main() {
  const db = new DocumentDataply<{
    name: string;
    age: number;
    tags: string[];
  }>('my-database.db', {
    wal: 'my-database.wal',
    indices: {
      name: true, // Index existing and new data
      age: false, // Index only new data
      'tags.0': true // Index the first element of the 'tags' array
    }
  });

  // Initialize the database
  await db.init();

  // Insert a document
  const id = await db.insert({
    name: 'John Doe',
    age: 30,
    tags: ['admin', 'developer']
  });

  // Query documents
  const results = await db.select({
    name: 'John Doe',
    age: { gte: 25 }
  }).drain();

  console.log(results);

  // Close the database
  await db.close();
}

main();
```

## Advanced Usage

### Indexing Policies

When defining indices in the constructor, you can specify a boolean value:

- `true`: The library will index all existing documents for this field during `init()` and all subsequent insertions.
- `false`: The library will only index documents inserted after this configuration.

> [!NOTE]
> `db.init()` automatically performs the backfilling process for fields marked as `true`.

### Batch Insertion

For inserting multiple documents efficiently:

```typescript
const ids = await db.insertBatch([
  { name: 'Alice', age: 25, tags: ['user'] },
  { name: 'Bob', age: 28, tags: ['moderator'] }
]);
```

### Querying

`document-dataply` supports various comparison operators:

| Operator | Description |
| :--- | :--- |
| `lt` | Less than |
| `lte` | Less than or equal to |
| `gt` | Greater than |
| `gte` | Greater than or equal to |
| `equal` | Equal to |
| `notEqual` | Not equal to |
| `like` | SQL-like pattern matching (e.g., `Jo%`) |
| `or` | Array of value where at least one must be met |

Example of a complex query:
```typescript
const users = await db.select({
  age: { gt: 18, lt: 65 },
  'address.city': 'Seoul',
  tags: { or: ['vip', 'premium'] }
}).drain();
```

> [!IMPORTANT]
> **Query Constraints**: You can only use query conditions (`lt`, `gt`, `equal`, etc.) on fields that have been explicitly indexed in the constructor. 
> 
> **If a field in the query is not indexed, its condition will be ignored.**
> 
> If you need to filter by a non-indexed field, you must retrieve the documents first and then use JavaScript's native `.filter()` method:
```typescript
const results = await db.select({ /* indexed fields only */ }).drain();
const filtered = results.filter(doc => doc.unindexedField === 'some-value');
```

### Transactions

Use transactions to ensure atomicity for multiple operations:

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

## API Reference

### `new DocumentDataply<T>(file, options)`
Creates a new database instance. `T` defines the document structure.
`options.indices` is an object where keys are field names and values are booleans indicating whether to index the field.

### `db.init()`
Initializes the database, sets up internal metadata, and prepares indices.

### `db.insert(document, tx?)`
Inserts a single document. Returns the document's `_id` (`number`).

### `db.insertBatch(documents, tx?)`
Inserts multiple documents efficiently. Returns an array of `_id`s (`number[]`).

### `db.select(query, options?, tx?)`
Retrieves documents matching the query.
Returns an object `{ stream, drain }`.
- `stream`: An async iterator to iterate over results one by one.
- `drain()`: A promise that resolves to an array of all matching documents.

### `db.getMetadata(tx?)`
Returns physical storage information (page count, row count, etc.).

### `db.createTransaction()`
Returns a new `Transaction` object.

### `db.close()`
Flushes changes and closes the database file.

## License

MIT
