CREATE TABLE IF NOT EXISTS tenants (
  tenant_id   TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  tier        TEXT NOT NULL DEFAULT 'shared',   -- 'shared' | 'dedicated'
  index_name  TEXT NOT NULL,
  boost_title FLOAT NOT NULL DEFAULT 3.0,
  boost_skills FLOAT NOT NULL DEFAULT 2.0,
  created_at  TIMESTAMPTZ DEFAULT now()
);

INSERT INTO tenants (tenant_id, name, tier, index_name)
VALUES
  ('tenant_acme',    'Acme Corp',      'shared',    'products_shared'),
  ('tenant_globex',  'Globex Inc',     'shared',    'products_shared'),
  ('tenant_initech', 'Initech (Large)','dedicated', 'products_initech')
ON CONFLICT DO NOTHING;
