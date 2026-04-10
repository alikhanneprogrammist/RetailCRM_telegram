require("dotenv").config();
const http = require("http");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const PORT = Number(process.env.PORT || 3000);

const dashboardPath = path.join(__dirname, "orders-dashboard.html");
let syncJobInProgress = false;

let supabase = null;
function getSupabase() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("Не заданы SUPABASE_URL или SUPABASE_SERVICE_ROLE_KEY");
  }
  if (!supabase) {
    supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  }
  return supabase;
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload));
}

function sendHtml(res, filePath) {
  fs.readFile(filePath, "utf8", (err, html) => {
    if (err) {
      sendJson(res, 500, { error: "Не удалось открыть HTML файл" });
      return;
    }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(html);
  });
}

async function runSyncAndRefresh() {
  const { syncOrders } = require("./sync-orders");
  const { runEtlMarts } = require("./etl-marts");
  await syncOrders();
  await runEtlMarts();
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function formatDateOnly(dateValue) {
  if (!dateValue) return null;
  const d = new Date(dateValue);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function getOrderDate(order) {
  return (
    order.created_at ||
    order.synced_at ||
    order.raw?.createdAt ||
    order.raw?.updatedAt ||
    null
  );
}

function getOrderCountry(order) {
  return order.country || "Не указано";
}

function getOrderSource(order) {
  return order.source || "Не указано";
}

function getOrderStatus(order) {
  return order.order_status || "Не указано";
}

function getPaymentType(order) {
  return order.payment_type || "Не указано";
}

function getProductName(order) {
  return order.product_name || "Не указано";
}

function getRevenue(order) {
  return toNumber(order.total_sum);
}

function buildGroupedStats(items, keyGetter, revenueGetter = null) {
  const map = new Map();

  for (const item of items) {
    const label = keyGetter(item) || "Не указано";
    if (!map.has(label)) {
      map.set(label, { label, count: 0, revenue: 0 });
    }
    const row = map.get(label);
    row.count += 1;
    if (revenueGetter) {
      row.revenue += toNumber(revenueGetter(item));
    }
  }

  return Array.from(map.values()).sort((a, b) => b.count - a.count);
}

function buildTimeline(items, days) {
  const today = new Date();
  const start = new Date();
  start.setHours(0, 0, 0, 0);
  start.setDate(today.getDate() - (days - 1));

  const map = new Map();

  for (let i = 0; i < days; i += 1) {
    const d = new Date(start);
    d.setDate(start.getDate() + i);
    const key = d.toISOString().slice(0, 10);
    map.set(key, { date: key, orders: 0, revenue: 0 });
  }

  for (const order of items) {
    const key = formatDateOnly(getOrderDate(order));
    if (!key || !map.has(key)) continue;
    const row = map.get(key);
    row.orders += 1;
    row.revenue += getRevenue(order);
  }

  return Array.from(map.values());
}

function percentChange(current, previous) {
  if (!previous && !current) return "0%";
  if (!previous) return "+100%";
  const diff = ((current - previous) / previous) * 100;
  const sign = diff >= 0 ? "+" : "";
  return `${sign}${diff.toFixed(1)}%`;
}

async function getOrders(days) {
  const sb = getSupabase();
  const from = new Date();
  from.setHours(0, 0, 0, 0);
  from.setDate(from.getDate() - (days - 1));
  const fromIso = from.toISOString();

  const { data, error } = await sb
    .from("orders")
    .select("*")
    .gte("synced_at", fromIso)
    .order("synced_at", { ascending: false });

  if (error) {
    throw error;
  }

  return data || [];
}

async function buildAnalytics(days) {
  const orders = await getOrders(days);
  const timeline = buildTimeline(orders, days);

  const totalOrders = orders.length;
  const totalRevenue = orders.reduce((sum, order) => sum + getRevenue(order), 0);
  const avgCheck = totalOrders ? totalRevenue / totalOrders : 0;

  const todayKey = new Date().toISOString().slice(0, 10);
  const todayOrders = timeline.find((x) => x.date === todayKey)?.orders || 0;

  const statusesRaw = buildGroupedStats(orders, getOrderStatus);
  const sourcesRaw = buildGroupedStats(orders, getOrderSource);
  const paymentsRaw = buildGroupedStats(orders, getPaymentType);
  const countriesRaw = buildGroupedStats(orders, getOrderCountry, getRevenue);
  const productsRaw = buildGroupedStats(orders, getProductName, getRevenue);

  const statuses = statusesRaw.map((item) => ({
    ...item,
    share: totalOrders ? Number(((item.count / totalOrders) * 100).toFixed(1)) : 0,
  }));

  const sources = sourcesRaw.slice(0, 10);
  const payments = paymentsRaw.slice(0, 10);
  const countries = countriesRaw.slice(0, 10);
  const products = productsRaw.slice(0, 10);

  let peakDay = null;
  let peakOrders = 0;
  for (const point of timeline) {
    if (point.orders > peakOrders) {
      peakOrders = point.orders;
      peakDay = point.date;
    }
  }

  const half = Math.floor(days / 2);
  const previousOrders = timeline.slice(0, half).reduce((sum, x) => sum + x.orders, 0);
  const currentOrders = timeline.slice(half).reduce((sum, x) => sum + x.orders, 0);

  const recentOrders = orders.slice(0, 20).map((order) => ({
    order_number: order.order_number,
    first_name: order.first_name,
    last_name: order.last_name,
    product_name: order.product_name,
    order_status: order.order_status,
    payment_type: order.payment_type,
    total_sum: order.total_sum,
    synced_at: order.synced_at,
    created_at: order.created_at || order.raw?.createdAt || null,
    updated_at: order.updated_at || order.raw?.updatedAt || null,
  }));

  const startDate = timeline[0]?.date || null;
  const endDate = timeline[timeline.length - 1]?.date || null;

  return {
    summary: {
      totalOrders,
      totalRevenue,
      avgCheck,
      todayOrders,
      statusCount: statuses.length,
      startDate,
      endDate,
      peakDay,
      growthText: `${percentChange(currentOrders, previousOrders)} к предыдущему периоду`,
      generatedAt: new Date().toLocaleString("ru-RU"),
    },
    timeline,
    statuses,
    sources,
    payments,
    countries,
    products,
    recentOrders,
  };
}

async function requestHandler(req, res) {
  try {
    const reqUrl = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    if (reqUrl.pathname === "/") {
      return sendHtml(res, dashboardPath);
    }

    if (reqUrl.pathname === "/api/orders-analytics" && req.method === "GET") {
      const daysParam = Number(reqUrl.searchParams.get("days") || 30);
      const days = Number.isFinite(daysParam) && daysParam > 0 ? daysParam : 30;

      const analytics = await buildAnalytics(days);
      return sendJson(res, 200, analytics);
    }

    if (reqUrl.pathname === "/api/sync-and-refresh" && req.method === "POST") {
      if (syncJobInProgress) {
        return sendJson(res, 409, { error: "Синхронизация уже выполняется" });
      }

      syncJobInProgress = true;
      const startedAt = new Date();
      try {
        await runSyncAndRefresh();
        const finishedAt = new Date();
        return sendJson(res, 200, {
          ok: true,
          message: "Синхронизация и пересчёт аналитики завершены",
          startedAt: startedAt.toISOString(),
          finishedAt: finishedAt.toISOString(),
          durationSec: Math.round((finishedAt.getTime() - startedAt.getTime()) / 1000),
        });
      } finally {
        syncJobInProgress = false;
      }
    }

    sendJson(res, 404, { error: "Маршрут не найден" });
  } catch (error) {
    console.error("Ошибка сервера:", error);
    sendJson(res, 500, {
      error: error.message || "Внутренняя ошибка сервера",
    });
  }
}

module.exports = requestHandler;

if (!process.env.VERCEL) {
  const server = http.createServer(requestHandler);
  server.listen(PORT, () => {
    console.log(`Dashboard запущен: http://localhost:${PORT}`);
  });
}