CREATE TABLE IF NOT EXISTS raw.raw_orders (
  id BIGSERIAL PRIMARY KEY,
  kafka_topic TEXT NOT NULL,
  kafka_partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,
  kafka_key TEXT,
  payload JSONB NOT NULL,
  event_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS raw.raw_categories (
  category_id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS raw.raw_products (
  product_id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  price NUMERIC(10, 2) NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  category_id INT,
  UNIQUE (name),
  CHECK (price >= 0)
);

CREATE TABLE IF NOT EXISTS raw.raw_customers (
  customer_id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (email)
);