import { DocumentDataply } from '../src/core/document';
// import { Dataply } from '../src/index'; // Unused and not exported
import * as fs from 'fs';
import * as path from 'path';

const TEST_DIR = path.join(__dirname, 'tmp_doc_index');
const DB_PATH = path.join(TEST_DIR, 'test.ply');

type TestDoc = {
  name: string;
  age: number;
  tags?: string[];
}

describe('Document Indexing Options', () => {
  beforeEach(() => {
    if (fs.existsSync(TEST_DIR)) {
      fs.rmSync(TEST_DIR, { recursive: true, force: true });
    }
    fs.mkdirSync(TEST_DIR, { recursive: true });
  });

  afterEach(async () => {
    // Clean up
  });

  test('Should insert and query with enabled index', async () => {
    const db = new DocumentDataply<TestDoc>(DB_PATH, {
      indecies: {
        name: true
      }
    });
    await db.init();

    await db.insert({ name: 'Alice', age: 30 });
    await db.insert({ name: 'Bob', age: 25 });

    const results = await db.select({ name: 'Alice' });
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Alice');

    // Age has no index, so querying by age should return empty (current limitation/design)
    const ageResults = await db.select({ age: 30 });
    expect(ageResults).toHaveLength(0);

    await db.close();
  });

  test('Should backfill index when option is enabled later', async () => {
    // 1. Start without index
    let db = new DocumentDataply<TestDoc>(DB_PATH, {
      indecies: {
        name: false // Explicitly false or omit
      }
    });
    await db.init();
    await db.insert({ name: 'Charlie', age: 40 });
    await db.insert({ name: 'David', age: 45 });

    // confirm no index usage
    let results = await db.select({ name: 'Charlie' });
    expect(results).toHaveLength(0);
    await db.close();

    // 2. Restart with index enabled -> Backfill should trigger
    db = new DocumentDataply<TestDoc>(DB_PATH, {
      indecies: {
        name: true
      }
    });
    await db.init();

    // Now query should work
    results = await db.select({ name: 'Charlie' });
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Charlie');

    // Insert new data
    await db.insert({ name: 'Eve', age: 20 });
    results = await db.select({ name: 'Eve' });
    expect(results).toHaveLength(1);

    await db.close();
  });

  test('Should disable index and stop using it', async () => {
    // 1. Start with index
    let db = new DocumentDataply<TestDoc>(DB_PATH, {
      indecies: {
        name: true
      }
    });
    await db.init();
    await db.insert({ name: 'Frank', age: 50 });
    await db.close();

    // 2. Restart with index disabled
    db = new DocumentDataply<TestDoc>(DB_PATH, {
      indecies: {
        name: false
      }
    });
    await db.init();

    // Query should fail/empty because index is not loaded
    const results = await db.select({ name: 'Frank' });
    expect(results).toHaveLength(0);

    // New insert
    await db.insert({ name: 'Grace', age: 55 });

    // Even if we query Grace, it shouldn't find it via index (since tree not loaded)
    const results2 = await db.select({ name: 'Grace' });
    expect(results2).toHaveLength(0);

    await db.close();
  });
});
