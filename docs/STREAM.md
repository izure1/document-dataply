# 🌊 Seamless Massive Data Processing (Stream Architecture)

When reading a few hundred pieces of data, there's no need to worry about server memory shortages. However, processing queries where the count is unknown or reaches millions simultaneously poses the risk of exceeding physical memory (RAM) limits and causing a forced server shutdown (OOM).

This document explains the **Stream** processing solution that defends against this.

---

## 1. Limitations of Drain() Mode
`drain()` is the operation of bringing fetched results into a commonly used JavaScript Array all at once.
While easy to handle, if it derives 1 million results, all data pours into server variables simultaneously, posing the risk of immediate memory exhaustion.

## 2. Recommended Solution: Stream-based Supply

To prevent server crashes and handle millions of queries, the standard approach is to use a **Stream** that supplies results lazily.
A stream functions as an asynchronous iterator (`Iterator`), allowing only 1 document in memory per cycle (1 `for` loop iteration). Once used, the engine immediately discards the data (GC), keeping memory occupancy consistently light from beginning to end.

```typescript
// 1. Traversal processing using the stream iterator
const usersQuery = db.select({});

for await (const user of usersQuery.stream()) {
  // Even if there are a million results, only 1 item passes through this block and is discarded.
  await sendBatchMailApi(user.email);
}
```

---

## 3. Chunking Control Logic: I/O Optimization

You might wonder, "If I fetch them one by one, won't 1 million disk accesses (I/O) occur and make it too slow?" `document-dataply` cleverly shatters this bottleneck.

The following mechanism operates completely hidden within the engine:
1. **Memory Detection**: Upon startup, the engine gauges the server's safely available free memory (`os.freemem()`) in real time.
2. **Dynamic Chunk Loading**: While the front-end developer receives them 1 by 1, the back-end engine calculates the amount its free memory can withstand and safely chunks that calculated amount from the disk at once.
3. When the fetched chunk array runs out, it triggers disk I/O once again to fill the next chunk.

Ultimately, the operator can enjoy the full performance without needing to design any disk I/O or set up memory leak defenses.

---

## Engine Processing Choice Guideline (Summary)
- **When the result volume is small (hundreds/thousands or less)** 👉 Use array conversion (`drain()`) for convenience.
- **When the result count is unknown or exceeds tens of thousands** 👉 Unconditionally enforce the delayed pipeline (`stream()`) structure to prevent server crashes.
