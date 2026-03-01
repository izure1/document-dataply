# Query Guide (QUERY.md)

`document-dataply` provides high-performance search capabilities based on B+Tree indexing. This document details how to write queries, supported operators, and how to use indices for optimal performance.

## 1. Basic Query

The simplest form of a query is directly matching field names with values.

```typescript
const results = await db.select({
  category: 'electronics',
  status: 'available'
}).drain();
```

This query finds all documents where `category` is 'electronics' and `status` is 'available'.

> [!TIP]
> **Query Shortcut**: The syntax `{ [field]: value }` is a shortcut for `{ [field]: { equal: value } }`. Both produce the same result.

> [!TIP]
> **Full Scan (Select All)**: If you pass an empty object (`{}`) or no values in the query object, the database will retrieve **all documents**.

## 2. Supported Query Operators

In addition to simple matching, various comparison operators are available.

| Operator | Description | Example |
| :--- | :--- | :--- |
| `lt` | Less Than | `age: { lt: 20 }` |
| `lte` | Less Than or Equal | `price: { lte: 1000 }` |
| `gt` | Greater Than | `score: { gt: 80 }` |
| `gte` | Greater Than or Equal | `views: { gte: 100 }` |
| `equal` | Equal To | `name: { equal: 'John' }` (or `name: 'John'`) |
| `notEqual` | Not Equal To | `role: { notEqual: 'admin' }` |
| `like` | Pattern Matching (SQL style) | `title: { like: 'Node%' }` (`%` is a wildcard) |
| `or` | Matches any value in the array | `tags: { or: ['news', 'tech'] }` |
| `match` | Full-text search (Requires FTS Index) | `content: { match: 'search' }` |

### Complex Condition Example
You can combine multiple operators for range queries.

```typescript
const items = await db.select({
  price: { gt: 100, lte: 500 }, // Greater than 100 and less than or equal to 500
  brand: { or: ['Apple', 'Samsung'] }
}).drain();
```

## 3. Important: Index-Based Query Constraints

All query filtering in `document-dataply` only works on **indexed fields**.

- **Indexed Fields**: Conditions passed to `db.select()` will be used as actual search filters.
- **Unindexed Fields**: Conditions on fields without an index will be **ignored**.

> [!IMPORTANT]
> If you need to filter by unindexed fields, you should first narrow down the results using indexed fields and then use JavaScript's `.filter()` method.

```typescript
// Valid approach
const results = await db.select({ 
  indexedField: 'value' // This field has an index
}).drain();

const filtered = results.filter(doc => doc.nonIndexedField === 'other-value');
```

## 4. Full-Text Search (FTS) Indexing

To use the `match` operator, the field must be explicitly configured as an **FTS Index**. A standard boolean index (`true`/`false`) is not sufficient for full-text search.

### FTS Configuration

Use the `createIndex` method to define FTS indices:

```typescript
const db = DocumentDataply.Define<MyDocument>()
  .Options({ wal: 'my-database.db.wal' })
  .Open('my-database.db');

// Standard Index: Supports equal, lt, gt, gte, lte, like, or
await db.createIndex('idx_category', { type: 'btree', fields: ['category'] }); 

// FTS Index: Supports ONLY 'match'
// Whitespace tokenizer: Splits by whitespace
await db.createIndex('idx_content', { type: 'fts', fields: 'content', tokenizer: 'whitespace' });

// N-gram tokenizer: Splits into chunks of 'gramSize'
await db.createIndex('idx_title', { type: 'fts', fields: 'title', tokenizer: 'ngram', gramSize: 2 });

await db.init();
```

> [!IMPORTANT]
> **FTS Index Constraints**:
> 1. Fields indexed with `type: 'fts'` **ONLY support the `match` operator**. 
> 2. You **cannot** use `equal`, `lt`, `gt`, `lte`, `gte`, `like`, or `or` on FTS-indexed fields.
> 3. If you need both equality checks and full-text search on the same field, you must choose one or handle the other via manual filtering after the query.

## 5. Sorting and Pagination (Options)

You can control the order and number of results using the `options` argument (the second parameter of `db.select()`).

```typescript
const results = await db.select(
  { status: 'active' },
  {
    limit: 20,           // Fetch a maximum of 20 results
    orderBy: 'createdAt', // Sort by 'createdAt' (requires an index)
    sortOrder: 'desc'    // Descending order ('asc' or 'desc')
  }
).drain();
```

> [!NOTE]
> The field(s) specified in `orderBy` must be indexed (either as a single-field index or the **first field** of a composite index). If you attempt to sort by an unindexed field or do not specify `orderBy`, documents will be returned based on internal rules and the order is not guaranteed.

## 5. Nested Fields and the Default Field (`_id`)

- **`_id` Field**: All documents are automatically assigned a unique, numeric `_id` field by the system upon insertion. This field always exists and can be used for querying. **Note: The `_id` field is system-managed; users cannot set it manually or modify it later.**
- **Nested Fields**: Fields deep within a document or specific array elements can be queried if they are indexed.

```typescript
// When defining indices
await db.createIndex('idx_user_name', { type: 'btree', fields: ['user.profile.name'] });
await db.createIndex('idx_tags_0', { type: 'btree', fields: ['tags.0'] });

// When querying
const user = await db.select({
  'user.profile.name': 'Alice'
}).drain();
```

## 7. Composite Indexes

`document-dataply` supports indexing multiple fields in a single B+Tree. This is highly efficient for complex queries that involve multiple conditions or specific sorting requirements.

### Creating a Composite Index

```typescript
await db.createIndex('idx_category_price', {
  type: 'btree',
  fields: ['category', 'price']
});
```

### How it Works

1. **Sorting**: Values are sorted by the first field, then the second, and so on.
2. **Querying**: The engine can use a composite index if the query includes the **leading field(s)** of the index.
3. **Selectivity**: The query engine automatically evaluates composite indexes to determine if they are the most efficient option for a given query.

## 8. Performance Optimization: Selectivity

When multiple fields are included in a query, the engine calculates **Selectivity**. It analyzes data distribution to prioritize the index that best reduces the result set (i.e., fields with more unique values) to maximize search efficiency.
