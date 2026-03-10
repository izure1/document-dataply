# 🔍 Query & Index Guide

To retrieve data quickly and efficiently, it is essential to understand **Query** syntax and configure **Indices** to accelerate search speeds.

---

## 1. The Necessity of an Index

When searching for specific conditions among tens of thousands of records, this database strictly does not support the common Full Scan method. **Therefore, querying or sorting operations are fundamentally impossible on fields without an index.**

Creating an index on frequently queried fields (e.g., email, nickname) enables instant (O(log N)) search performance utilizing the internal B+Tree structure. 
However, registering unnecessarily many indices can slow down data insertion speeds, so declare them only on essential fields.

> **💡 Built-in Default Index (`_id`)**
> When a document is first saved to the database, a unique integer property `_id` is automatically assigned, and the system automatically creates a dedicated index based on this `_id`. Therefore, the `_id` field can always be used as a criterion for immediate, high-speed operations without any separate creation process.

---

## 2. Index Creation and Deletion (Create / Drop)

Indices can be registered at any time during initialization or runtime.

```typescript
// Apply an index named 'idx_category' to the 'category' field
await db.createIndex('idx_category', { type: 'btree', fields: ['category'] });
```

### Deleting Unnecessary Indices (`dropIndex`)
Unused indices should be deleted to save tree update resources wasted during data insertion/modification and to free up disk space. (Note: The system's built-in `_id` index cannot be deleted.)

```typescript
await db.dropIndex('idx_category');
```

### Nested Property Indexing
Indices can be easily assigned to properties deep within JSON files or array elements.

```typescript
// Accessing deeply nested objects (name inside the profile object within user)
await db.createIndex('idx_user_name', { type: 'btree', fields: ['user.profile.name'] });

// Accessing the first (0th) element in an array
await db.createIndex('idx_first_tag', { type: 'btree', fields: ['tags.0'] });
```

---

## 3. Operator Queries (Query Options)

Beyond simple value comparisons, powerful operators can be used.

| Operator | Function | Example Usage | Description |
| :--- | :--- | :--- | :--- |
| `equal` | Equal To (==) | `age: { equal: 20 }` | Perfectly matches the given value |
| `notEqual`| Not Equal (!=) | `role: { notEqual: 'admin' }` | Excludes the matched category |
| `gt` | Greater Than (>) | `score: { gt: 80 }` | Strictly greater than the baseline (e.g., from 81) |
| `gte` | Greater/Eq (>=) | `views: { gte: 100 }` | Greater than or equal to the baseline (includes 100) |
| `lt` / `lte` | Less Than / Eq | `price: { lte: 1000 }` | Less than or equal to the baseline |
| `or` | Multiple Conditions| `tags: { or: ['IT', 'Game'] }` | Satisfies if at least one in the array matches |
| `like` | Pattern Matching | `name: { like: 'kim%jin' }` | Search utilizing `%` (0 or more) and `_` (exactly 1) wildcards |

> **🚨 Mandatory Requirement (Index Enforcement)**
> The fields used as search conditions in the `select` query or sorting criteria in `orderBy` **must have an index created beforehand.** Requesting a query on a field without an index will result in a failure as the query is impossible.
> If you wish to narrow down data using secondary fields without indices, you must first retrieve the data using essential indexed fields, and then manually filter them using the JavaScript built-in `.filter()` method on the resulting array (`drain()`).

---

## 4. Full-Text Search (FTS)

This is a dedicated index feature for finding words within long text bodies. It uses a text fragmentation matching method rather than simple comparison.

### Creating a Full-Text Index
```typescript
// Set type to 'fts'. (whitespace: splits text based on spaces)
await db.createIndex('idx_content', { type: 'fts', fields: 'content', tokenizer: 'whitespace' });
```

### Utilizing the `match` Operator
Fields declared with Full-Text Search (FTS) cannot use size comparisons (`gt`, `lt`), etc., and can only use the **`match`** operator to check for the presence of a string.

```typescript
const posts = await db.select({
  content: { match: 'javascript' } // Retrieves documents containing the word
}).drain();
```

---

## 5. Sort and Limit Processing

You can control the extraction count of query results and designate sorting methods.

```typescript
const results = await db.select(
  { status: 'active' }, // Basic condition to find
  {
    limit: 10,           // Receive only 10 extracted results
    orderBy: 'createdAt', // Sort based on a specific field
    sortOrder: 'desc'    // Descending (desc) or ascending (asc) order
  }
).drain();
```

**💡 Optimizer Acceleration**  
The engine does not search everything and then cut off 10 items. It evaluates conditions and sort directions as it proceeds, and as soon as 10 items are fulfilled, it forces an Early Exit of the remaining search logic, bringing a massive speed advantage.
