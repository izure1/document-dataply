```typescript

await db.migration(1, async (tx) => {
  await tx.createIndex('age', { type: 'btree', fields: ['age'] })
})

```