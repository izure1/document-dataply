# Schema Migration Guide (MIGRATION.md)

As your application evolves, you may need to update your database schema—adding new indices, transforming data, or cleaning up old records. `document-dataply` provides a built-in migration system to handle these transitions safely.

## 1. How Migration Works

The migration system is based on a `schemeVersion` stored in the database's internal metadata.

1. Each database starts with `schemeVersion: 0`.
2. When you call `db.migration(version, callback)`, the engine compares the current `schemeVersion` with the target `version`.
3. If `currentVersion < targetVersion`, the `callback` is executed.
4. After the `callback` completes successfully, the database's `schemeVersion` is updated to the target `version`.
5. If the current version is already equal to or higher than the target version, the callback is skipped.

## 2. Basic Usage

The most common use case is adding a new index to an existing database.

```typescript
import { DocumentDataply } from 'document-dataply';

const db = DocumentDataply.Define<MyDoc>()
  .Options({ wal: 'data.wal' })
  .Open('data.db');

await db.init();

// Migration to version 1: Add a new index
await db.migration(1, async (tx) => {
  console.log('Migrating to version 1...');
  await db.createIndex('idx_category', { type: 'btree', fields: ['category'] }, tx);
});

// Migration to version 2: Add another index
await db.migration(2, async (tx) => {
  console.log('Migrating to version 2...');
  await db.createIndex('idx_tags', { type: 'btree', fields: ['tags.0'] }, tx);
});
```

## 3. The Migration Callback

The callback receives a `Transaction` object (`tx`). You should pass this transaction to any database methods called within the migration to ensure atomicity.

Inside the callback, you can:
- **Create Indices**: `await db.createIndex(name, options, tx)`
- **Drop Indices**: `await db.dropIndex(name, tx)`
- **Query and Update Data**: You can perform complex data transformations by selecting documents and updating them within the same transaction.

## 4. Checking the Current Version

You can check the current `schemeVersion` using `db.getMetadata()`.

```typescript
const metadata = await db.getMetadata();
console.log(`Current Schema Version: ${metadata.schemeVersion}`);
```

## 5. Best Practices

- **Incremental Versions**: Always use increasing version numbers (1, 2, 3...).
- **Idempotency**: While the engine ensures the callback runs only once per version, ensure your migration logic is safe to run.
- **Transactions**: Always use the provided `tx` object for all operations inside the callback.
- **Backup**: It's always a good practice to back up your `.db` file before running significant data transformations.
