#!/usr/bin/env bash
set -e
echo "=== Multi-Tenant Search MVP Demo ==="
echo ""
echo "1. Starting bulk import for tenant_acme (1000 products)..."
node seed.js tenant_acme 1000 &

echo "2. Starting bulk import for tenant_globex (500 products)..."
node seed.js tenant_globex 500 &

echo "3. Starting bulk import for tenant_initech (2000 products)..."
node seed.js tenant_initech 2000 &

wait
echo ""
echo "✅ All bulk imports complete!"
echo "→ Open http://localhost:3000 to see the results"
