require("dotenv").config();
const crypto = require("crypto");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Не заполнены SUPABASE_URL или SUPABASE_SERVICE_ROLE_KEY в .env");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const SOURCE_TABLE = "orders";
const DIM_PRODUCTS = "dim_products";
const DIM_CUSTOMERS = "dim_customers";
const DIM_CHANNELS = "dim_channels";
const FACT_ORDERS = "fact_orders";
const FACT_ORDER_ITEMS = "fact_order_items";
const MART_HYPOTHESES_SIGNALS = "mart_hypotheses_signals";

function hashKey(prefix, value) {
  const digest = crypto.createHash("sha1").update(String(value)).digest("hex").slice(0, 16);
  return `${prefix}_${digest}`;
}

function normalizeText(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text || null;
}

function normalizeEmail(value) {
  const text = normalizeText(value);
  return text ? text.toLowerCase() : null;
}

function normalizePhone(value) {
  const text = normalizeText(value);
  if (!text) return null;

  const hasPlus = text.startsWith("+");
  const digits = text.replace(/\D/g, "");
  if (!digits) return null;
  return hasPlus ? `+${digits}` : digits;
}

function toNumber(value, fallback = 0) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toIsoDate(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function toIsoDateTime(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function extractOrderDate(order) {
  return (
    order.raw?.createdAt ||
    order.raw?.created_at ||
    order.created_at ||
    order.raw?.updatedAt ||
    order.synced_at ||
    null
  );
}

function extractItems(order) {
  if (Array.isArray(order.raw?.items)) return order.raw.items;
  const name = normalizeText(order.product_name);
  if (!name) return [];
  return [
    {
      productName: name,
      quantity: toNumber(order.quantity, 1),
      initialPrice: order.item_price ?? null,
      price: order.item_price ?? null,
    },
  ];
}

function statusToFunnelStep(status) {
  const s = normalizeText(status)?.toLowerCase() || "";
  if (!s) return "created";
  if (/(cancel|canceled|cancelled|отмен)/.test(s)) return "cancelled";
  if (/(complete|delivered|done|выдан|достав)/.test(s)) return "delivered";
  if (/(paid|payment|оплачен)/.test(s)) return "paid";
  return "created";
}

function mean(values) {
  if (!values.length) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function stdDev(values) {
  if (values.length < 2) return 0;
  const m = mean(values);
  const variance = values.reduce((acc, v) => acc + (v - m) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function computeDeltaPercent(current, baseline) {
  if (!baseline && !current) return 0;
  if (!baseline) return 100;
  return ((current - baseline) / baseline) * 100;
}

async function fetchAllOrders(batchSize = 1000) {
  let from = 0;
  const rows = [];

  while (true) {
    const to = from + batchSize - 1;
    const { data, error } = await supabase
      .from(SOURCE_TABLE)
      .select("*")
      .order("retailcrm_id", { ascending: true })
      .range(from, to);

    if (error) throw error;
    if (!data || data.length === 0) break;

    rows.push(...data);
    if (data.length < batchSize) break;
    from += batchSize;
  }

  return rows;
}

function buildWarehouseLayers(rawOrders) {
  const products = new Map();
  const customers = new Map();
  const channels = new Map();
  const factOrders = [];
  const factItems = [];
  const orderRevenueByDate = new Map();

  for (const order of rawOrders) {
    const source = normalizeText(order.source) || "unknown";
    const channelKey = hashKey("chn", source.toLowerCase());
    if (!channels.has(channelKey)) {
      channels.set(channelKey, {
        channel_key: channelKey,
        source,
        campaign: null,
        medium: null,
        updated_at: new Date().toISOString(),
      });
    }

    const phone = normalizePhone(order.phone);
    const email = normalizeEmail(order.email);
    const identity = phone || email || `retailcrm:${order.retailcrm_id}`;
    const customerKey = hashKey("cus", identity);
    const orderDateIso = toIsoDate(extractOrderDate(order));

    if (!customers.has(customerKey)) {
      customers.set(customerKey, {
        customer_key: customerKey,
        phone,
        email,
        first_name: normalizeText(order.first_name),
        last_name: normalizeText(order.last_name),
        country: normalizeText(order.country),
        first_order_date: orderDateIso,
        last_order_date: orderDateIso,
        updated_at: new Date().toISOString(),
      });
    } else if (orderDateIso) {
      const customer = customers.get(customerKey);
      if (!customer.first_order_date || orderDateIso < customer.first_order_date) {
        customer.first_order_date = orderDateIso;
      }
      if (!customer.last_order_date || orderDateIso > customer.last_order_date) {
        customer.last_order_date = orderDateIso;
      }
      customer.updated_at = new Date().toISOString();
    }

    const orderId = order.retailcrm_id ?? hashKey("ord", JSON.stringify(order));
    const orderDateTime = toIsoDateTime(extractOrderDate(order)) || order.synced_at || new Date().toISOString();
    const revenue = toNumber(order.total_sum, 0);
    const items = extractItems(order);

    factOrders.push({
      order_key: hashKey("ford", orderId),
      retailcrm_id: order.retailcrm_id ?? null,
      order_number: normalizeText(order.order_number),
      order_date: orderDateTime,
      status: normalizeText(order.order_status),
      status_funnel_step: statusToFunnelStep(order.order_status),
      source,
      channel_key: channelKey,
      customer_key: customerKey,
      total_sum: revenue,
      item_count: items.length,
      payment_type: normalizeText(order.payment_type),
      payment_status: normalizeText(order.payment_status),
      synced_at: toIsoDateTime(order.synced_at) || new Date().toISOString(),
      updated_at: new Date().toISOString(),
    });

    const orderDay = toIsoDate(orderDateTime);
    if (orderDay) {
      const prev = orderRevenueByDate.get(orderDay) || 0;
      orderRevenueByDate.set(orderDay, prev + revenue);
    }

    items.forEach((item, idx) => {
      const rawName = normalizeText(item.productName || item.offer?.name || item.product?.name) || "unknown";
      const canonicalName = rawName.toLowerCase();
      const productKey = hashKey("prd", canonicalName);
      const qty = toNumber(item.quantity, 1);
      const price = toNumber(item.initialPrice ?? item.price, 0);
      const lineRevenue = qty * price;

      if (!products.has(productKey)) {
        products.set(productKey, {
          product_key: productKey,
          canonical_name: canonicalName,
          display_name: rawName,
          aliases: [rawName],
          category: normalizeText(item.product?.groups?.[0]?.name),
          updated_at: new Date().toISOString(),
        });
      } else {
        const product = products.get(productKey);
        if (!product.aliases.includes(rawName)) {
          product.aliases.push(rawName);
        }
        product.updated_at = new Date().toISOString();
      }

      factItems.push({
        fact_item_key: hashKey("foi", `${orderId}:${productKey}:${idx}`),
        retailcrm_id: order.retailcrm_id ?? null,
        order_key: hashKey("ford", orderId),
        product_key: productKey,
        qty,
        price,
        line_revenue: lineRevenue,
        order_date: orderDateTime,
        updated_at: new Date().toISOString(),
      });
    });
  }

  for (const product of products.values()) {
    product.aliases = Array.from(new Set(product.aliases));
  }

  return {
    dimProducts: Array.from(products.values()),
    dimCustomers: Array.from(customers.values()),
    dimChannels: Array.from(channels.values()),
    factOrders,
    factItems,
    martSignals: buildMartHypothesisSignals(factOrders, factItems, orderRevenueByDate),
  };
}

function buildMartHypothesisSignals(factOrders, factItems, orderRevenueByDate) {
  const byProduct = new Map();
  for (const item of factItems) {
    if (!item.order_date) continue;
    const date = toIsoDate(item.order_date);
    if (!date) continue;
    const key = `${item.product_key}::${date}`;
    const entry = byProduct.get(key) || {
      product_key: item.product_key,
      date,
      revenue: 0,
      qty: 0,
      orders: new Set(),
    };
    entry.revenue += toNumber(item.line_revenue);
    entry.qty += toNumber(item.qty);
    entry.orders.add(item.order_key);
    byProduct.set(key, entry);
  }

  const byPair = new Map();
  const itemsPerOrder = new Map();
  for (const item of factItems) {
    const list = itemsPerOrder.get(item.order_key) || [];
    list.push(item.product_key);
    itemsPerOrder.set(item.order_key, list);
  }
  for (const [orderKey, productKeys] of itemsPerOrder.entries()) {
    const unique = Array.from(new Set(productKeys)).sort();
    for (let i = 0; i < unique.length; i += 1) {
      for (let j = i + 1; j < unique.length; j += 1) {
        const pairKey = `${unique[i]}::${unique[j]}`;
        const prev = byPair.get(pairKey) || 0;
        byPair.set(pairKey, prev + 1);
      }
    }
    if (!byPair.has(`__orders__${orderKey}`)) {
      byPair.set(`__orders__${orderKey}`, 1);
    }
  }

  const signals = [];
  const now = new Date().toISOString();

  const allDates = Array.from(orderRevenueByDate.keys()).sort();
  const revSeries = allDates.map((d) => orderRevenueByDate.get(d) || 0);
  const m = mean(revSeries);
  const sd = stdDev(revSeries);
  const lastDay = allDates[allDates.length - 1];
  if (lastDay) {
    const lastVal = orderRevenueByDate.get(lastDay) || 0;
    const z = sd === 0 ? 0 : (lastVal - m) / sd;
    if (Math.abs(z) >= 2) {
      signals.push({
        signal_key: hashKey("sig", `outlier:${lastDay}`),
        signal_date: `${lastDay}T00:00:00.000Z`,
        signal_type: "outlier_revenue",
        entity_type: "global",
        entity_key: "all",
        metric: "daily_revenue",
        period_a: "last_day",
        period_b: "history",
        delta: computeDeltaPercent(lastVal, m),
        z_score: z,
        confidence: Math.min(1, Math.abs(z) / 3),
        status: z > 0 ? "growth" : "decline",
        evidence: `Дневная выручка ${lastVal.toFixed(2)} vs средняя ${m.toFixed(2)}`,
        created_at: now,
        updated_at: now,
      });
    }
  }

  const productRows = Array.from(byProduct.values());
  const groupedByProduct = new Map();
  for (const row of productRows) {
    const list = groupedByProduct.get(row.product_key) || [];
    list.push(row);
    groupedByProduct.set(row.product_key, list);
  }

  for (const [productKey, rows] of groupedByProduct.entries()) {
    const sorted = rows.sort((a, b) => a.date.localeCompare(b.date));
    if (sorted.length < 8) continue;
    const last7 = sorted.slice(-7);
    const prev7 = sorted.slice(-14, -7);
    const lastRevenue = last7.reduce((acc, x) => acc + x.revenue, 0);
    const prevRevenue = prev7.reduce((acc, x) => acc + x.revenue, 0);
    const delta = computeDeltaPercent(lastRevenue, prevRevenue);
    if (Math.abs(delta) < 20) continue;

    signals.push({
      signal_key: hashKey("sig", `trend:${productKey}:${sorted[sorted.length - 1].date}`),
      signal_date: `${sorted[sorted.length - 1].date}T00:00:00.000Z`,
      signal_type: "product_trend",
      entity_type: "product",
      entity_key: productKey,
      metric: "weekly_revenue",
      period_a: "last_7d",
      period_b: "prev_7d",
      delta,
      z_score: null,
      confidence: Math.min(1, Math.abs(delta) / 100),
      status: delta > 0 ? "growth" : "decline",
      evidence: `7д=${lastRevenue.toFixed(2)}, пред.7д=${prevRevenue.toFixed(2)}`,
      created_at: now,
      updated_at: now,
    });
  }

  const totalOrders = factOrders.length || 1;
  for (const [pairKey, count] of byPair.entries()) {
    if (pairKey.startsWith("__orders__")) continue;
    const support = count / totalOrders;
    if (support < 0.05) continue;
    const [a, b] = pairKey.split("::");
    signals.push({
      signal_key: hashKey("sig", `basket:${pairKey}`),
      signal_date: now,
      signal_type: "basket_mix",
      entity_type: "pair",
      entity_key: pairKey,
      metric: "pair_support",
      period_a: "all_time",
      period_b: "all_time",
      delta: support * 100,
      z_score: null,
      confidence: Math.min(1, support * 3),
      status: "stable",
      evidence: `Пара ${a}+${b} встречается в ${count} заказах`,
      created_at: now,
      updated_at: now,
    });
  }

  return signals;
}

async function clearWarehouseTables() {
  const clearOrder = [
    MART_HYPOTHESES_SIGNALS,
    FACT_ORDER_ITEMS,
    FACT_ORDERS,
    DIM_PRODUCTS,
    DIM_CUSTOMERS,
    DIM_CHANNELS,
  ];

  for (const table of clearOrder) {
    const { error } = await supabase.from(table).delete().not("updated_at", "is", null);
    if (error) throw error;
  }
}

async function upsertTable(table, rows, uniqueColumn) {
  if (!rows.length) return;

  const chunkSize = 500;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const { error } = await supabase.from(table).upsert(chunk, { onConflict: uniqueColumn });
    if (error) throw error;
  }
}

async function run() {
  console.log("ETL marts: начало");
  const rawOrders = await fetchAllOrders();
  console.log(`Загружено orders.raw: ${rawOrders.length}`);

  const layers = buildWarehouseLayers(rawOrders);
  console.log(
    `Слои: products=${layers.dimProducts.length}, customers=${layers.dimCustomers.length}, channels=${layers.dimChannels.length}, fact_orders=${layers.factOrders.length}, fact_items=${layers.factItems.length}, signals=${layers.martSignals.length}`
  );

  await clearWarehouseTables();

  await upsertTable(DIM_PRODUCTS, layers.dimProducts, "product_key");
  await upsertTable(DIM_CUSTOMERS, layers.dimCustomers, "customer_key");
  await upsertTable(DIM_CHANNELS, layers.dimChannels, "channel_key");
  await upsertTable(FACT_ORDERS, layers.factOrders, "order_key");
  await upsertTable(FACT_ORDER_ITEMS, layers.factItems, "fact_item_key");
  await upsertTable(MART_HYPOTHESES_SIGNALS, layers.martSignals, "signal_key");

  console.log("ETL marts: успешно завершен");
}

module.exports = { runEtlMarts: run };

if (require.main === module) {
  run().catch((error) => {
    console.error("ETL marts: ошибка", error.message || error);
    process.exit(1);
  });
}
