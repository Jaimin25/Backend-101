const fs = require("fs");
const path = require("path");

class KVStore {
  constructor(dataDir = "./data", flushLimit = 5) {
    this.dataDir = dataDir;
    this.flushLimit = flushLimit;
    this.memtable = new Map();
    this.walPath = path.join(this.dataDir, "wal.log");

    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir);
    }

    this.recover();
  }

  recover() {
    if (!fs.existsSync(this.walPath)) return;

    const lines = fs.readFileSync(this.walPath, "utf8").split("\n");

    for (let line of lines) {
      if (!line) continue;
      const record = JSON.parse(line);

      if (record.deleted) {
        this.memtable.set(record.key, { value: null, deleted: true });
      } else {
        this.memtable.set(record.key, { value: record.value, deleted: false });
      }
    }
    console.log("Recovery complete");
  }

  appendToWAL(record) {
    fs.appendFileSync(this.walPath, JSON.stringify(record) + "\n");
  }

  set(key, value) {
    const record = { key, value, deleted: false };

    this.appendToWAL(record);
    this.memtable.set(key, { value, deleted: false });

    if (this.memtable.size >= this.flushLimit) {
      this.flush();
    }
  }

  delete(key) {
    const record = { key, value: null, deleted: true };

    this.appendToWAL(record);
    this.memtable.set(key, { value: null, deleted: true });

    if (this.memtable.size >= this.flushLimit) {
      this.flush();
    }
  }

  get(key) {
    if (this.memtable.has(key)) {
      const entry = this.memtable.get(key);
      return entry.deleted ? null : entry.value;
    }

    const files = fs
      .readdirSync(this.dataDir)
      .filter((f) => f.startsWith("sstable"))
      .sort()
      .reverse();

    for (let file of files) {
      const filePath = path.join(this.dataDir, file);
      const data = JSON.parse(fs.readFileSync(filePath));

      for (let [k, v] of data) {
        if (k === key) {
          return v.deleted ? null : v.value;
        }
      }
    }

    return null;
  }

  flush() {
    const sorted = Array.from(this.memtable.entries()).sort((a, b) =>
      a[0].localeCompare(b[0]),
    );

    const filename = `sstable-${Date.now()}.json`;
    const filePath = path.join(this.dataDir, filename);

    fs.writeFileSync(filePath, JSON.stringify(sorted, null, 2));

    this.memtable.clear();
    fs.writeFileSync(this.walPath, "");

    console.log("Flushed to", filename);
  }

  compact() {
    const files = fs
      .readdirSync(this.dataDir)
      .filter((f) => f.startsWith("sstable"))
      .sort();

    const merged = new Map();

    for (let file of files.reverse()) {
      const filePath = path.join(this.dataDir, file);
      const data = JSON.parse(fs.readFileSync(filePath));

      for (let [k, v] of data) {
        if (!merged.has(k)) {
          merged.set(k, v);
        }
      }
    }

    const finalData = Array.from(merged.entries())
      .filter(([_, v]) => !v.deleted)
      .sort((a, b) => a[0].localeCompare(b[0]));

    const compactFile = path.join(
      this.dataDir,
      `sstable-compacted-${Date.now()}.json`,
    );
    fs.writeFileSync(compactFile, JSON.stringify(finalData, null, 2));

    for (let file of files) {
      fs.unlinkSync(path.join(this.dataDir, file));
    }

    console.log("Compaction complete.");
  }
}

module.exports = KVStore;
