const KVStore = require("./kv");

const db = new KVStore("./data", 3);

db.set("user1", "CJ");
db.set("user2", "Backend");
db.set("user3", "Engineer");

console.log("Get user1:", db.get("user1"));

db.delete("user2");

console.log("Get user2:", db.get("user2"));

db.set("user1", "Vibe Coder");

console.log("Get updated user1:", db.get("user1"));

db.compact();
