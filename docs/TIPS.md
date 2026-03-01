# User Tips and Advanced Guide (TIPS.md)

This document covers advanced features and performance optimization tips for using `document-dataply` more effectively.

## 1. Query Optimization and Sorting

- **Automatic Index Selection**: The engine automatically selects the index with the highest selectivity.
- **Sorting Constraints**: Fields used in `orderBy` must be indexed.

For more details, see the [Query Guide (QUERY.md)](./QUERY.md).

## 2. Memory Management (Stream vs drain)

- **Small Datasets**: Using `drain()` to retrieve an array is convenient.
- **Large Datasets**: Use `stream` to minimize memory usage.

For more details, see the [Stream Guide (STREAM.md)](./STREAM.md).

## 3. Transactional Operations

When using transactions, you can pass a transaction object (`tx`) to various database methods. This ensures atomicity for a series of operations. Index management operations are also transactional.

Supported methods within a transaction:
- `db.insert(doc, tx)`
- `db.insertBatch(docs, tx)`
- `db.select(query, options, tx)`
- `db.partialUpdate(query, updates, tx)`
- `db.fullUpdate(query, doc, tx)`
- `db.delete(query, tx)`
- `db.createIndex(name, options, tx)`
- `db.dropIndex(name, tx)`
- `db.getMetadata(tx)`

## 4. Bulk Insertion Performance (Massive Insertion)

Using `insertBatch()` is much faster than calling a single `insert()` in a loop.

- **Reason**: `insert()` updates and commits metadata and B+Tree heads for every call, whereas `insertBatch()` groups transactions internally to minimize IO operations.

```typescript
// Recommended approach
const docs = Array.from({ length: 1000 }, (_, i) => ({ id: i, data: '...' }));
await db.insertBatch(docs);
```

## 5. Dynamic Index Management and Backfilling

You can add a new index at any time using the `createIndex()` method. If you add an index to a database that already contains data, the library will automatically perform **Backfilling** (scanning existing data and populating the index).

```typescript
// Example: Adding an index for a previously non-existent 'email' field
const db = DocumentDataply.Define<MyDoc>()
  .Options({ wal: 'my-database.db.wal' })
  .Open('my-database.db');

await db.init();

// Even after init, you can create a new index.
// This will popluate existing data into the index automatically.
await db.createIndex('idx_email', { type: 'btree', fields: ['email'] });

// You can also drop an index manually at any time.
// This will clean up both internal metadata and physical index files.
await db.dropIndex('idx_email');
```

## 6. Composite Index Tips

- **Field Order Matters**: A composite index on `['a', 'b']` is different from `['b', 'a']`. Choose the order based on your most frequent query filters and sort requirements.
- **Leading Field**: To use a composite index for range queries, the query must include the first field of the index.
- **Selectivity**: Composite indexes often have higher selectivity than single-field indexes, which can significantly speed up complex queries.

