// Runs once at startup to create index templates & seed data
const { Client } = require("@opensearch-project/opensearch");
const { Pool } = require("pg");

const os = new Client({ node: process.env.OPENSEARCH_URL || "http://localhost:9200" });
const pg = new Pool({ connectionString: process.env.PG_URL || "postgresql://search:search@localhost:5432/tenants" });

const INDEX_TEMPLATE = {
  index_patterns: ["products_*"],
  settings: {
    number_of_shards: 3,
    number_of_replicas: 1,
    analysis: {
      analyzer: {
        autocomplete_analyzer: {
          type: "custom",
          tokenizer: "standard",
          filter: ["lowercase", "autocomplete_filter"],
        },
        autocomplete_search_analyzer: {
          type: "custom",
          tokenizer: "standard",
          filter: ["lowercase"],
        },
      },
      filter: {
        autocomplete_filter: {
          type: "edge_ngram",
          min_gram: 2,
          max_gram: 20,
        },
      },
    },
  },
  mappings: {
    properties: {
      tenant_id:   { type: "keyword" },
      doc_type:    { type: "keyword" },
      doc_id:      { type: "keyword" },
      version:     { type: "long" },
      name:        { type: "text",    analyzer: "autocomplete_analyzer", search_analyzer: "autocomplete_search_analyzer", copy_to: "all_text" },
      description: { type: "text",    analyzer: "standard", copy_to: "all_text" },
      category:    { type: "keyword", copy_to: "all_text" },
      brand:       { type: "keyword" },
      price:       { type: "float" },
      in_stock:    { type: "boolean" },
      tags:        { type: "keyword" },
      all_text:    { type: "text",    analyzer: "standard" },
      created_at:  { type: "date" },
      updated_at:  { type: "date" },
      // custom fields – typed slots avoid mapping explosion
      cf_text_1:   { type: "text" },
      cf_kw_1:     { type: "keyword" },
      cf_kw_2:     { type: "keyword" },
      cf_date_1:   { type: "date" },
      cf_float_1:  { type: "float" },
    },
  },
};

async function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  // Wait for OpenSearch to be ready
  for (let i = 0; i < 30; i++) {
    try {
      await os.cluster.health({});
      console.log("✅ OpenSearch ready");
      break;
    } catch (e) {
      console.log(`⏳ Waiting for OpenSearch... (${i+1}/30)`);
      await wait(3000);
    }
  }

  // Put index template
  await os.indices.putIndexTemplate({ name: "products_template", body: INDEX_TEMPLATE });
  console.log("✅ Index template created");

  // Create indices from tenant config
  const { rows: tenants } = await pg.query("SELECT DISTINCT index_name FROM tenants");
  for (const { index_name } of tenants) {
    const exists = await os.indices.exists({ index: index_name });
    if (!exists.body) {
      await os.indices.create({ index: index_name });
      console.log(`✅ Index created: ${index_name}`);
    }
  }

  console.log("✅ Init complete");
  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
