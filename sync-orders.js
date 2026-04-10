require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");

const RETAILCRM_URL = (process.env.RETAILCRM_URL || "").replace(/\/+$/, "");
const RETAILCRM_API_KEY = process.env.RETAILCRM_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";
const TELEGRAM_NOTIFY_SUM_THRESHOLD = Number(process.env.TELEGRAM_NOTIFY_SUM_THRESHOLD || 50000);

if (!RETAILCRM_URL || !RETAILCRM_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Не заполнены переменные окружения в .env");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function retailcrmGet(path, params = {}) {
  const cleanPath = path.startsWith("/") ? path : `/${path}`;
  const url = new URL(`${RETAILCRM_URL}${cleanPath}`);

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }

  console.log("REQUEST URL:", url.toString());

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "X-API-KEY": RETAILCRM_API_KEY,
      Accept: "application/json",
    },
  });

  const data = await response.json();

  console.log("STATUS:", response.status);

  if (!response.ok || data.success === false) {
    throw new Error(`Ошибка RetailCRM: ${JSON.stringify(data)}`);
  }

  return data;
}

function getFirstItem(order) {
  if (!Array.isArray(order.items) || order.items.length === 0) {
    return null;
  }
  return order.items[0];
}

function getProductName(order) {
  if (!Array.isArray(order.items) || order.items.length === 0) {
    return null;
  }

  const names = order.items
    .map((item) => item.productName || item.offer?.name || null)
    .filter(Boolean);

  return names.length ? names.join(", ") : null;
}

function mapOrder(order) {
  const firstItem = getFirstItem(order);

  const quantity = firstItem?.quantity != null ? Number(firstItem.quantity) : null;
  const itemPrice =
    firstItem?.initialPrice != null
      ? Number(firstItem.initialPrice)
      : firstItem?.price != null
      ? Number(firstItem.price)
      : null;

  const deliveryAddress =
    order.delivery?.address?.text ||
    order.delivery?.address?.index ||
    order.delivery?.address?.city ||
    null;

  return {
    retailcrm_id: order.id ?? null,
    order_number: order.number ?? null,
    order_status: order.status ?? null,
    order_type: order.orderType ?? order.orderMethod ?? null,

    first_name: order.firstName ?? null,
    last_name: order.lastName ?? null,
    phone: order.phone ?? null,
    email: order.email ?? null,
    country: order.countryIso ?? order.delivery?.address?.countryIso ?? null,

    product_name: getProductName(order),
    quantity,
    item_price: itemPrice,

    delivery_type:
      order.delivery?.code ??
      order.delivery?.serviceName ??
      null,

    delivery_address: deliveryAddress,

    payment_type:
      Array.isArray(order.payments) && order.payments[0]
        ? order.payments[0].type || order.payments[0].externalId || null
        : null,

    payment_status:
      Array.isArray(order.payments) && order.payments[0]
        ? order.payments[0].status || null
        : null,

    source:
      order.source?.source ??
      order.source?.medium ??
      order.source?.campaign ??
      null,

    total_sum:
      order.totalSumm != null
        ? Number(order.totalSumm)
        : quantity != null && itemPrice != null
        ? quantity * itemPrice
        : null,

    raw: order,
    synced_at: new Date().toISOString(),
  };
}

async function getExistingOrderByRetailcrmId(retailcrmId) {
  if (!retailcrmId) return null;
  const { data, error } = await supabase
    .from("orders")
    .select("retailcrm_id")
    .eq("retailcrm_id", retailcrmId)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data || null;
}

async function saveOrder(orderRow) {
  const { error } = await supabase
    .from("orders")
    .upsert(orderRow, { onConflict: "retailcrm_id" });

  if (error) {
    throw error;
  }
}

function formatMoneyKzt(value) {
  return new Intl.NumberFormat("ru-KZ", {
    style: "currency",
    currency: "KZT",
    maximumFractionDigits: 0,
  }).format(Number(value || 0));
}

async function sendTelegramNotification(orderRow) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.log("Telegram уведомления отключены: заполните TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID в .env");
    return;
  }

  const text = [
    "Новый заказ с высокой суммой",
    `Заказ: ${orderRow.order_number || orderRow.retailcrm_id || "—"}`,
    `Сумма: ${formatMoneyKzt(orderRow.total_sum)}`,
    `Клиент: ${[orderRow.first_name, orderRow.last_name].filter(Boolean).join(" ") || orderRow.phone || "—"}`,
    `Статус: ${orderRow.order_status || "—"}`,
  ].join("\n");

  const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text,
    }),
  });

  const data = await response.json();
  if (!response.ok || data.ok === false) {
    throw new Error(`Ошибка Telegram: ${JSON.stringify(data)}`);
  }
}

async function getLastSyncDate() {
  const { data, error } = await supabase
    .from("sync_state")
    .select("value")
    .eq("key", "retailcrm_orders_updated_at")
    .maybeSingle();

  if (error) {
    throw error;
  }

  return data?.value || null;
}

async function setLastSyncDate(value) {
  const { error } = await supabase
    .from("sync_state")
    .upsert(
      {
        key: "retailcrm_orders_updated_at",
        value,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "key" }
    );

  if (error) {
    throw error;
  }
}

async function syncOrders() {
  const lastSyncDate = await getLastSyncDate();

  console.log("Старт sync");
  console.log("Последняя дата синхронизации:", lastSyncDate || "нет");

  let page = 1;
  let totalPages = 1;
  let maxLoadedDate = lastSyncDate;

  do {
    const params = {
      page,
      limit: 50,
    };

    // В этом проекте endpoint /api/v5/orders не принимает updatedAtFrom.
    // Синхронизация выполняется полным проходом с безопасным upsert по retailcrm_id.

    const data = await retailcrmGet("/api/v5/orders", params);
    const orders = data.orders || [];
    totalPages = data.pagination?.totalPageCount || 1;

    console.log(`Страница ${page}/${totalPages}, заказов: ${orders.length}`);

    for (const order of orders) {
      try {
        const mapped = mapOrder(order);
        const existing = await getExistingOrderByRetailcrmId(mapped.retailcrm_id);
        await saveOrder(mapped);

        const isNewOrder = !existing;
        const orderTotal = Number(mapped.total_sum || 0);
        if (isNewOrder && orderTotal >= TELEGRAM_NOTIFY_SUM_THRESHOLD) {
          await sendTelegramNotification(mapped);
          console.log(
            `Отправлено Telegram уведомление по заказу retailcrm_id=${mapped.retailcrm_id}, сумма=${orderTotal}`
          );
        }

        const orderUpdatedAt = order.updatedAt || order.createdAt || null;
        if (orderUpdatedAt && (!maxLoadedDate || new Date(orderUpdatedAt) > new Date(maxLoadedDate))) {
          maxLoadedDate = orderUpdatedAt;
        }

        console.log(`Синхронизирован заказ retailcrm_id=${mapped.retailcrm_id}`);
      } catch (err) {
        console.error(`Ошибка на заказе ${order.id}:`, err.message);
      }

      await sleep(120);
    }

    page += 1;
    await sleep(250);
  } while (page <= totalPages);

  if (maxLoadedDate) {
    await setLastSyncDate(maxLoadedDate);
    console.log("Обновлена дата синхронизации:", maxLoadedDate);
  }

  console.log("Sync завершён");
}

module.exports = { syncOrders };

if (require.main === module) {
  syncOrders().catch((err) => {
    console.error("Критическая ошибка:", err.message || err);
    process.exit(1);
  });
}