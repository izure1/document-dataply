# Transaction Guide (TRANSACTION.md)

`document-dataply` supports robust transaction features to ensure data consistency and integrity. All write operations can be performed atomically.

## 1. Why Use Transactions?

Using transactions provides several key advantages:
- **Atomicity**: Groups multiple operations into a single logical unit that either entirely succeeds or entirely fails.
- **Consistency**: Ensures the database remains in a valid state after any transaction.
- **Isolation**: Prevents concurrently executing transactions from interfering with each other.

## 2. Basic Usage Pattern

Create a transaction object by calling `db.createTransaction()` and pass it as the last argument to database methods.

```typescript
const tx = db.createTransaction();

try {
  // Insert operations within a transaction
  await db.insert({ name: 'Alice', age: 25 }, tx);
  await db.insert({ name: 'Bob', age: 28 }, tx);

  // Commit if all operations succeed
  await tx.commit();
  console.log('Transaction committed successfully.');
} catch (error) {
  // Roll back on error (reverts all operations within the transaction)
  await tx.rollback();
  console.error('Transaction rolled back due to error:', error);
}
```

## 3. Supported Methods

The following methods accept a transaction object as an argument:

- `db.insert(doc, tx)`
- `db.insertBatch(docs, tx)`
- `db.select(query, options, tx)`
- `db.partialUpdate(query, updates, tx)`
- `db.fullUpdate(query, doc, tx)`
- `db.delete(query, tx)`

## 4. Concurrency Control and Isolation

`document-dataply` leverages the **MVCC (Multi-Version Concurrency Control)** mechanism provided by the underlying `dataply` engine.

- **Snapshot Isolation**: Read operations view a snapshot of the database at the time the transaction started, ensuring they are not affected by concurrent write operations.
- **Write Conflict**: If two transactions attempt to modify the same data simultaneously, a conflict may occur; the transaction that commits first generally takes precedence.

## 5. Precautions and Tips

- **Keep Transactions Short**: Holding transactions for too long can block resources and impact performance. Aim to group only necessary operations and complete them quickly.
- **Explicit Termination**: Either `commit()` or `rollback()` must be called. Failure to do so may result in resource leaks.
- **Retry Logic**: For transient write conflicts, it is recommended to implement retry logic using an exponential backoff strategy.

```typescript
// Simple retry example (pseudo-code)
async function performWithRetry(task) {
  for (let i = 0; i < 3; i++) {
    const tx = db.createTransaction();
    try {
      await task(tx);
      await tx.commit();
      return;
    } catch (e) {
      await tx.rollback();
      if (i === 2) throw e;
      await delay(Math.pow(2, i) * 100);
    }
  }
}
```
