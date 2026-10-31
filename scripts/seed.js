// Bulk import simulation — sends 500 products for a tenant to the bulk-import Kafka topic
const { Kafka } = require("kafkajs");
const { v4: uuidv4 } = require("uuid");

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || "localhost:9092"] });
const producer = kafka.producer();

const TENANT_ID = process.argv[2] || "tenant_acme";
const COUNT = parseInt(process.argv[3] || "500");

const CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Toys", "Food", "Automotive"];
const BRANDS = ["BrandAlpha", "BrandBeta", "BrandGamma", "BrandDelta", "BrandEpsilon"];

async function main() {
  await producer.connect();
  console.log(`📦 Bulk-importing ${COUNT} products for ${TENANT_ID}...`);

  const messages = [];
  for (let i = 0; i < COUNT; i++) {
    const docId = uuidv4();
    const category = CATEGORIES[i % CATEGORIES.length];
    const brand = BRANDS[i % BRANDS.length];
    const price = parseFloat((Math.random() * 500 + 5).toFixed(2));
    const event = {
      tenantId: TENANT_ID,
      docType: "product",
      docId,
      version: Date.now() + i,
      op: "UPSERT",
      payload: {
        name: `Bulk Product ${i + 1} - ${category} ${brand}`,
        description: `Bulk imported product number ${i + 1} in category ${category} from ${brand}.`,
        category,
        brand,
        price,
        in_stock: Math.random() > 0.15,
        tags: ["bulk", i % 3 === 0 ? "sale" : "new"],
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      },
    };
    messages.push({ key: `${TENANT_ID}:${docId}`, value: JSON.stringify(event) });
    if (messages.length === 50) {
      await producer.send({ topic: "doc-changes", messages: messages.splice(0) });
      process.stdout.write(`\r⚡ ${i + 1}/${COUNT} sent`);
      await new Promise(r => setTimeout(r, 50)); // slight throttle
    }
  }
  if (messages.length) await producer.send({ topic: "doc-changes", messages });
  console.log(`\n✅ Done! ${COUNT} products queued for ${TENANT_ID}`);
  await producer.disconnect();
}

main().catch(console.error);
