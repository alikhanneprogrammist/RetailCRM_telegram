-- Schema for ETL pipeline: orders.raw -> dim/fact -> mart_hypotheses_signals
-- Run in Supabase SQL Editor before `npm run etl:marts`.

create table if not exists public.dim_products (
  product_key text primary key,
  canonical_name text not null,
  display_name text,
  aliases jsonb not null default '[]'::jsonb,
  category text,
  updated_at timestamptz not null default now()
);

create table if not exists public.dim_customers (
  customer_key text primary key,
  phone text,
  email text,
  first_name text,
  last_name text,
  country text,
  first_order_date date,
  last_order_date date,
  updated_at timestamptz not null default now()
);

create table if not exists public.dim_channels (
  channel_key text primary key,
  source text not null,
  campaign text,
  medium text,
  updated_at timestamptz not null default now()
);

create table if not exists public.fact_orders (
  order_key text primary key,
  retailcrm_id bigint,
  order_number text,
  order_date timestamptz not null,
  status text,
  status_funnel_step text,
  source text,
  channel_key text references public.dim_channels(channel_key),
  customer_key text references public.dim_customers(customer_key),
  total_sum numeric not null default 0,
  item_count integer not null default 0,
  payment_type text,
  payment_status text,
  synced_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists idx_fact_orders_order_date on public.fact_orders(order_date);
create index if not exists idx_fact_orders_status on public.fact_orders(status);
create index if not exists idx_fact_orders_source on public.fact_orders(source);
create index if not exists idx_fact_orders_customer_key on public.fact_orders(customer_key);

create table if not exists public.fact_order_items (
  fact_item_key text primary key,
  retailcrm_id bigint,
  order_key text not null references public.fact_orders(order_key) on delete cascade,
  product_key text not null references public.dim_products(product_key),
  qty numeric not null default 0,
  price numeric not null default 0,
  line_revenue numeric not null default 0,
  order_date timestamptz not null,
  updated_at timestamptz not null default now()
);

create index if not exists idx_fact_order_items_order_date on public.fact_order_items(order_date);
create index if not exists idx_fact_order_items_product_key on public.fact_order_items(product_key);

create table if not exists public.mart_hypotheses_signals (
  signal_key text primary key,
  signal_date timestamptz not null,
  signal_type text not null,
  entity_type text not null,
  entity_key text not null,
  metric text not null,
  period_a text,
  period_b text,
  delta numeric,
  z_score numeric,
  confidence numeric,
  status text,
  evidence text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_mart_hyp_signals_date on public.mart_hypotheses_signals(signal_date desc);
create index if not exists idx_mart_hyp_signals_type on public.mart_hypotheses_signals(signal_type);
create index if not exists idx_mart_hyp_signals_entity on public.mart_hypotheses_signals(entity_type, entity_key);
