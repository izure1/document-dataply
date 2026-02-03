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

## 4. Bulk Insertion Performance (Massive Insertion)

Using `insertBatch()` is much faster than calling a single `insert()` in a loop.

- **Reason**: `insert()` updates and commits metadata and B+Tree heads for every call, whereas `insertBatch()` groups transactions internally to minimize IO operations.

```typescript
// Recommended approach
const docs = Array.from({ length: 1000 }, (_, i) => ({ id: i, data: '...' }));
await db.insertBatch(docs);
```

## 5. Dynamic Index Management and Backfilling

If you add a new field to the `indices` option during database initialization, the `init()` method will automatically scan existing data and populate the index when it runs (Backfilling).

```typescript
// Example: Adding an index for a previously non-existent 'email' field
const db = DocumentDataply.Define<MyDoc>().Options({
  indices: {
    name: true,
    email: true // Newly added. Automatically creates an index for existing data when init() is called.
  }
}).Open(file);
await db.init();
```

> [!NOTE]
> Setting it to `true` re-indexes all existing data, while setting it to `false` only indexes data that comes in from that point forward.
