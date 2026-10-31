const express = require("express");
const cors    = require("cors");
const { Client } = require("@opensearch-project/opensearch");
const Redis   = require("ioredis");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());

const os = new Client({ node: process.env.OPENSEARCH_URL || "http://localhost:9200" });
const redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");
const pg = new Pool({ connectionString: process.env.PG_URL || "postgresql://search:search@localhost:5432/tenants" });

const tenantCache = new Map();
async function getTenant(tenantId) {
  if (tenantCache.has(tenantId)) return tenantCache.get(tenantId);
  const { rows } = await pg.query("SELECT * FROM tenants WHERE tenant_id = $1", [tenantId]);
  const tenant = rows[0] || { tenant_id: tenantId, index_name: "products_shared", boost_title: 3, boost_skills: 2 };
  tenantCache.set(tenantId, tenant);
  return tenant;
}

function buildQuery(q, filters, tenantId, from, size) {
  const must = [];
  const filter = [{ term: { tenant_id: tenantId } }]; // ALWAYS injected — tenant isolation

  if (q && q.trim()) {
    must.push({
      multi_match: {
        query: q,
        fields: ["name^4", "description^2", "category^2", "brand^1.5", "tags^1", "all_text"],
        type: "best_fields",
        fuzziness: "AUTO",
      },
    });
  } else {
    must.push({ match_all: {} });
  }

  if (filters.category)  filter.push({ term: { category: filters.category } });
  if (filters.brand)     filter.push({ term: { brand: filters.brand } });
  if (filters.in_stock !== undefined) filter.push({ term: { in_stock: filters.in_stock } });
  if (filters.tags)      filter.push({ term: { tags: filters.tags } });
  if (filters.min_price !== undefined || filters.max_price !== undefined) {
    const range = {};
    if (filters.min_price !== undefined) range.gte = parseFloat(filters.min_price);
    if (filters.max_price !== undefined) range.lte = parseFloat(filters.max_price);
    filter.push({ range: { price: range } });
  }

  return {
    from,
    size,
    query: { bool: { must, filter } },
    aggs: {
      categories: { terms: { field: "category", size: 20 } },
      brands:     { terms: { field: "brand",    size: 20 } },
      in_stock:   { terms: { field: "in_stock", size: 2  } },
      tags:       { terms: { field: "tags",     size: 30 } },
      price_stats:{ stats: { field: "price" } },
    },
    highlight: {
      fields: { name: {}, description: { fragment_size: 150 } },
    },
  };
}

// GET /search?q=...&tenant_id=...&category=...&brand=...&in_stock=...&page=1&size=10
app.get("/search", async (req, res) => {
  const { q = "", tenant_id, category, brand, in_stock, tags, min_price, max_price, page = 1, size = 10 } = req.query;
  if (!tenant_id) return res.status(400).json({ error: "tenant_id required" });

  const cacheKey = `search:${tenant_id}:${JSON.stringify(req.query)}`;
  const cached = await redis.get(cacheKey);
  if (cached) return res.json({ ...JSON.parse(cached), cached: true });

  const tenant = await getTenant(tenant_id);
  const from = (parseInt(page) - 1) * parseInt(size);

  const filters = { category, brand, in_stock: in_stock !== undefined ? in_stock === "true" : undefined, tags, min_price, max_price };
  const dslBody = buildQuery(q, filters, tenant_id, from, parseInt(size));

  try {
    const { body } = await os.search({ index: tenant.index_name, routing: tenant_id, body: dslBody });
    const result = {
      total:  body.hits.total.value,
      hits:   body.hits.hits.map(h => ({ id: h._id, score: h._score, ...h._source, highlight: h.highlight })),
      facets: {
        categories: body.aggregations.categories.buckets,
        brands:     body.aggregations.brands.buckets,
        in_stock:   body.aggregations.in_stock.buckets,
        tags:       body.aggregations.tags.buckets,
        price_stats:body.aggregations.price_stats,
      },
      cached: false,
    };
    await redis.setex(cacheKey, 15, JSON.stringify(result)); // 15s TTL
    res.json(result);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// GET /typeahead?q=...&tenant_id=...
app.get("/typeahead", async (req, res) => {
  const { q = "", tenant_id } = req.query;
  if (!tenant_id || q.length < 2) return res.json({ suggestions: [] });

  const cacheKey = `ta:${tenant_id}:${q}`;
  const cached = await redis.get(cacheKey);
  if (cached) return res.json(JSON.parse(cached));

  const tenant = await getTenant(tenant_id);
  const body = {
    size: 8,
    _source: ["name", "category", "brand"],
    query: {
      bool: {
        must: [{ match: { name: { query: q, analyzer: "autocomplete_search_analyzer" } } }],
        filter: [{ term: { tenant_id } }],   // tenant isolation enforced
      },
    },
  };
  try {
    const { body: r } = await os.search({ index: tenant.index_name, routing: tenant_id, body });
    const result = { suggestions: r.hits.hits.map(h => ({ name: h._source.name, category: h._source.category, brand: h._source.brand })) };
    await redis.setex(cacheKey, 10, JSON.stringify(result));
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /tenants - list tenants for UI selector
app.get("/tenants", async (_req, res) => {
  const { rows } = await pg.query("SELECT tenant_id, name, tier FROM tenants ORDER BY name");
  res.json(rows);
});

// Health
app.get("/health", (_req, res) => res.json({ status: "ok" }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`🔍 Query API listening on :${PORT}`));
