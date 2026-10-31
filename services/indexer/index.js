const { Kafka } = require("kafkajs");
const { Client } = require("@opensearch-project/opensearch");
const { Pool } = require("pg");

const kafka = new Kafka({ brokers: [(process.env.KAFKA_BROKER || "localhost:9092")] });
const os = new Client({ node: process.env.OPENSEARCH_URL || "http://localhost:9200" });
const pg = new Pool({ connectionString: process.env.PG_URL || "postgresql://search:search@localhost:5432/tenants" });

// Cache tenant configs
const tenantCache = new Map();
async function getTenant(tenantId) {
  if (tenantCache.has(tenantId)) return tenantCache.get(tenantId);
  const { rows } = await pg.query("SELECT * FROM tenants WHERE tenant_id = $1", [tenantId]);
  if (rows.length) { tenantCache.set(tenantId, rows[0]); return rows[0]; }
  return { index_name: "products_shared" }; // default
}

function toIndexDoc(event) {
  const p = event.payload;
  return {
    tenant_id:   event.tenantId,
    doc_type:    event.docType,
    doc_id:      event.docId,
    version:     event.version,
    name:        p.name,
    description: p.description,
    category:    p.category,
    brand:       p.brand,
    price:       p.price,
    in_stock:    p.in_stock,
    tags:        p.tags || [],
    created_at:  p.created_at,
    updated_at:  p.updated_at,
    cf_kw_1:     p.cf_kw_1 || null,
    cf_text_1:   p.cf_text_1 || null,
  };
}

async function processBatch(messages, resolveOffsets) {
  if (!messages.length) return;
  const ops = [];
  for (const msg of messages) {
    let event;
    try { event = JSON.parse(msg.value.toString()); } catch { continue; }
    const tenant = await getTenant(event.tenantId);
    const index = tenant.index_name;
    if (event.op === "DELETE") {
      ops.push({ delete: { _index: index, _id: event.docId, routing: event.tenantId } });
    } else {
      ops.push({ index: { _index: index, _id: event.docId, routing: event.tenantId } });
      ops.push(toIndexDoc(event));
    }
  }
  if (ops.length) {
    const resp = await os.bulk({ body: ops, refresh: "wait_for" });
    const errors = resp.body.items.filter(i => (i.index || i.delete)?.error);
    if (errors.length) console.error("⚠️ Bulk errors:", JSON.stringify(errors.slice(0, 3)));
    console.log(`✅ Indexed ${messages.length} docs`);
  }
}

async function run() {
  // Wait for OpenSearch
  for (let i = 0; i < 20; i++) {
    try { await os.cluster.health({}); break; }
    catch { console.log(`⏳ Waiting for OpenSearch (${i+1}/20)...`); await new Promise(r => setTimeout(r, 3000)); }
  }

  const consumer = kafka.consumer({ groupId: process.env.CONSUMER_GROUP || "indexer-rt" });
  await consumer.connect();
  await consumer.subscribe({ topic: process.env.KAFKA_TOPIC || "doc-changes", fromBeginning: true });

  console.log("🔄 Indexer listening on doc-changes");

  let batch = [], timer;
  const FLUSH_SIZE = parseInt(process.env.FLUSH_SIZE || "50");
  const FLUSH_MS   = parseInt(process.env.FLUSH_MS   || "1000");

  const flush = async () => {
    const toProcess = batch.splice(0);
    if (toProcess.length) await processBatch(toProcess);
  };

  await consumer.run({
    eachMessage: async ({ message }) => {
      batch.push(message);
      clearTimeout(timer);
      if (batch.length >= FLUSH_SIZE) { await flush(); }
      else { timer = setTimeout(flush, FLUSH_MS); }
    },
  });
}

run().catch(console.error);
