const { Kafka } = require("kafkajs");
const { v4: uuidv4 } = require("uuid");

const kafka = new Kafka({ brokers: [(process.env.KAFKA_BROKER || "localhost:9092")] });
const producer = kafka.producer();

const TENANTS = ["tenant_acme", "tenant_globex", "tenant_initech"];
const CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Toys", "Food", "Automotive"];
const BRANDS = ["BrandAlpha", "BrandBeta", "BrandGamma", "BrandDelta", "BrandEpsilon"];
const TAGS_POOL = ["sale", "new", "bestseller", "limited", "clearance", "premium", "eco-friendly"];

function randomItem(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randomTags() {
  const n = Math.floor(Math.random() * 3) + 1;
  return [...new Set(Array.from({ length: n }, () => randomItem(TAGS_POOL)))];
}

function makeProduct(tenantId, docId, version) {
  const adjectives = ["Amazing", "Pro", "Ultra", "Super", "Eco", "Smart", "Premium", "Slim"];
  const nouns = ["Widget", "Gadget", "Gizmo", "Device", "Tool", "Kit", "Bundle", "Set"];
  const name = `${randomItem(adjectives)} ${randomItem(nouns)} ${Math.floor(Math.random() * 9000) + 1000}`;
  return {
    tenantId,
    docType: "product",
    docId: docId || uuidv4(),
    version: version || Date.now(),
    op: "UPSERT",
    payload: {
      name,
      description: `High quality ${name.toLowerCase()} for everyday use. Durable, reliable, and efficient.`,
      category: randomItem(CATEGORIES),
      brand: randomItem(BRANDS),
      price: parseFloat((Math.random() * 500 + 10).toFixed(2)),
      in_stock: Math.random() > 0.2,
      tags: randomTags(),
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
  };
}

async function run() {
  await producer.connect();
  console.log("🚀 Producer started — emitting RT events every 2 seconds");

  // Continuous real-time events
  setInterval(async () => {
    const tenant = randomItem(TENANTS);
    const event = makeProduct(tenant);
    await producer.send({
      topic: "doc-changes",
      messages: [{ key: `${event.tenantId}:${event.docId}`, value: JSON.stringify(event) }],
    });
    console.log(`📤 RT event → ${tenant} | ${event.payload.name} | $${event.payload.price}`);
  }, 2000);
}

run().catch(console.error);
