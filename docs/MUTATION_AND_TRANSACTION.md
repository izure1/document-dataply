# ✨ Data Mutation & Transaction Guidelines

This document guides you through efficient methods for inserting or updating documents in the database, and how to use Transactions to prevent data inconsistency.

> **💡 Core Concept: `_id` Unique Identifier**
> Every document, upon insertion into the database, is **automatically assigned** a unique integer property `_id` by the system. This value cannot be explicitly designated or modified by the user, and it is permanently maintained without ever changing, even if operations entirely overwrite (Update) the document's contents.

> **💡 Concurrency Rules (Parallel Reads / Serialized Writes)**
> Query operations reading multiple documents (like `select`) can execute simultaneously in parallel. Conversely, **all write operations** covered in this document—such as data addition, modification, and deletion—are **internally serialized**. Even if called simultaneously, they execute strictly sequentially. The engine forcibly blocks concurrency issues where data might become tangled.

---

## 1. Single Insert vs. Batch Insert

When pushing massive amounts of data initially, repeatedly calling individual functions causes disk I/O to skyrocket, severely degrading performance. Using **Batch Insert** is highly recommended whenever possible.

**❌ Discouraged Approach (Repeated individual `insert`)**
```typescript
// Causes heavy operations by opening, writing, and closing files every loop iteration
for(let i=0; i<10000; i++){
  await db.insert({ id: i, data: 'hello' });
}
```
**✅ Recommended Approach (Batch Processing `insertBatch`)**
```typescript
// Passes a single array, updating 10,000 trees with just 1 I/O operation, making process dramatically faster
const docs = [ ... 10,000 logic data items ... ];
await db.insertBatch(docs);
```

---

## 2. Work Safety Net: Transaction Control

If the server crashes while deducting points from A and adding them to B, fatal logical errors (integrity violations) like money vanishing occur.

A transaction is a safety rule stating: **"All specified tasks must either be fully completed, or entirely rolled back as if they were never attempted."**

### Controlling Transactions (`withWriteTransaction` & `withReadTransaction`)

Use the `withWriteTransaction` method to encapsulate your database operations within a callback. If the callback finishes successfully, the transaction is automatically committed. If an error is thrown inside the callback, the transaction is automatically rolled back.

**💡 Concurrency and Parallel Processing Rules:**
- Even if `withWriteTransaction` is called concurrently from multiple places, they are internally **automatically serialized** and executed sequentially to ensure safety.
- If you only perform pure read operations that do not modify state, use `withReadTransaction`. `withReadTransaction` can be executed **concurrently in parallel** without bottlenecks alongside other `withWriteTransaction` or `withReadTransaction` operations.
- The core reason to use `withReadTransaction` is to guarantee a consistent **point-in-time snapshot**. Because it reads data based on the exact moment the read transaction begins, the data being read will not be altered or contaminated, even if other write operations are completed while it is running.

```typescript
try {
  // Pass a callback function to withWriteTransaction
  await db.withWriteTransaction(async (tx) => {
    // Attach the provided tx object at the end of each operation
    await db.insert({ name: 'A-Payment Deduction Reflected' }, tx);
    await db.insert({ name: 'B-Deposit Charge Reflected' }, tx);
    
    // If the callback finishes without errors, it automatically commits!
  });
} catch (error) {
  // If an error occurs midway, it automatically discards all ongoing changes (Rollback).
  console.log('Error defended, task rolled back.', error);
}
```

---

## 3. Update Control: Full Overwrite, Partial Update, and Deletion

There are two ways to modify documents according to your needs.

- **Full Replace (`fullUpdate`)**  
  Erases the existing document and replaces its form with entirely new data provided by the user. As explained earlier, the document's unique identifier `_id` is never overwritten and is safely preserved.

- **Partial Update (`partialUpdate`)**  
  Used when you only want to change a specific individual field value (e.g., updating only `lastLogin`) in a document with hundreds of properties. Unspecified original properties are preserved.
  ```typescript
  // Merges (updates) only the lastLogin time of the document with the 'ironid_x22' ID
  await db.partialUpdate(
    { id: 'ironid_x22' }, 
    { lastLogin: Date.now() }         
  );
  ```

- **Document Deletion (`delete`)**  
  Permanently removes documents that fit specific search conditions.
  ```typescript
  // Delete all users with 'inactive' status
  await db.delete({ status: 'inactive' });
  ```

---

## 4. Structural Change Management: Migration (`migration`)

A system used when services are updated, requiring changes to the format of stored data or the addition of new indices. Each database engine records its own version (`schemeVersion`).

```typescript
// Executes automatically only the first time if the schema version is less than 1 (e.g., 0) to upgrade the form.
await db.migration(1, async (tx) => {
  await db.createIndex('idx_recently_add', { type: 'btree', fields: ['recently_add'] }, tx);
});
```
A database that has already undergone the latest migration will simply ignore this code and pass over it.

---

## 5. Lifecycle and Other APIs (Lifecycle & Meta)

These are utilities that take final responsibility for database integrity or query internal states.

- **Closing the Database (`close`)**
  An essential defensive method that must be called before the application terminates completely. It releases disk I/O locks, stops periodic analyzers (Interval Providers), and perfectly synchronizes (Flushes) background logs (WAL) remaining in memory to the file for error recovery to prevent loss.

  ```typescript
  // Essential: Flush logs and release locks
  await db.close();

  // Advanced: Handling process termination
  process.on('SIGINT', async () => {
    console.log('Closing database safely...');
    await db.close();
    process.exit(0);
  });
  ```

- **Querying Metadata (`getMetadata`)**
  Allows you to see internal system information at a glance, such as the opened database's current schema version (`schemeVersion`), the last inserted unique identifier (`lastInsertId`), and the list of registered indices (`indices`).
  ```typescript
  const meta = await db.getMetadata();
  console.log('Schema Version:', meta.schemeVersion);
  console.log('Total Indices:', meta.indices.length);
  console.log('Registered Indices:', meta.indices.map(i => i.name).join(', '));
  ```
