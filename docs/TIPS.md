# User Tips and Advanced Guide (TIPS.md)

This document covers advanced features and performance optimization tips for using `document-dataply` more effectively.

## 1. Query Optimization

### Automatic Index Selection (Selectivity)
When performing queries on multiple fields, the engine internally calculates **Selectivity**.
It automatically selects the index that can reduce the result set the most when filtered first (i.e., the field with the most unique values) as the driver to start the lookup.

> [!TIP]
> Users do not need to worry about the order of conditions in the query. The engine automatically finds the most efficient path.

## 2. Sorting & Pagination

You can control sorting and pagination through the `options` argument, which is the second parameter of the `select` function.

```typescript
const results = await db.select(
  { category: 'electronics' },
  {
    limit: 10,           // Retrieve a maximum of 10 results
    orderBy: 'price',    // Sort based on the 'price' field (requires index)
    sortOrder: 'desc'    // Descending order ('asc' or 'desc')
  }
).drain();
```

> The field used in `orderBy` must have an index created during initialization. If you attempt to sort by a field without an index, it will default to sorting by `_id`.

## 3. Memory Management: `stream` vs `drain()`

- **`drain()`**: Loads all results into memory as an array. It's convenient when the amount of data is small, but fetching tens of thousands of documents at once can lead to Out Of Memory (OOM) issues.
- **`stream`**: Processes documents one by one as they are found via an async iterator. This is highly efficient for batch processing large datasets or streaming data over a network.

```typescript
// Recommended approach for large data processing
for await (const doc of db.select({ status: 'active' }).stream) {
  // Only one document is kept in memory at a time
  await processLargeData(doc);
}
```

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
