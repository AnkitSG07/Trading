const HOLDINGS_LIMIT = 10;
const TRANSACTIONS_LIMIT = 5;

const SUMMARY_PREFETCH_GLOBALS = (() => {
  if (typeof window === "undefined") {
    return {
      key: "page:summary-data",
      ttl: 3 * 60 * 1000,
      keys: {}
    };
  }
  const keys = window.PAGE_PREFETCH_KEYS || {};
  const defaultTtl = Number.isFinite(window.PAGE_PREFETCH_DEFAULT_TTL)
    ? window.PAGE_PREFETCH_DEFAULT_TTL
    : 3 * 60 * 1000;
  const baseTtl = Math.min(defaultTtl, 5 * 60 * 1000);
  return {
    key: keys.summary || "page:summary-data",
    ttl: baseTtl,
    keys
  };
})();

const SUMMARY_PREFETCH_KEY = SUMMARY_PREFETCH_GLOBALS.key;
const SUMMARY_PREFETCH_TTL = SUMMARY_PREFETCH_GLOBALS.ttl;
const CRITICAL_PREFETCH_KEYS = [
  SUMMARY_PREFETCH_KEY,
  SUMMARY_PREFETCH_GLOBALS.keys.accountInfo,
  SUMMARY_PREFETCH_GLOBALS.keys.dashboard,
  SUMMARY_PREFETCH_GLOBALS.keys.notifications
].filter((key, index, list) => key && list.indexOf(key) === index);

const SUMMARY_LAST_FETCH_KEY = (() => {
  if (typeof document === "undefined") return "summary:last-fetch";
  const userId = document.body?.dataset?.userId || "guest";
  return `summary:last-fetch:${userId}`;
})();

let summaryPrefetchRegistered = false;
let criticalPrefetchPrimed = false;
let summaryRefreshInFlight = false;

function readSummaryPrefetchCache() {
  if (typeof window === "undefined" || typeof window.getPagePrefetchData !== "function") {
    return undefined;
  }
  return window.getPagePrefetchData(SUMMARY_PREFETCH_KEY);
}

function readLastSummaryFetchTimestamp() {
  if (typeof sessionStorage === "undefined") return null;
  try {
    const raw = sessionStorage.getItem(SUMMARY_LAST_FETCH_KEY);
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : null;
  } catch (error) {
    return null;
  }
}

function markSummaryFetchTimestamp(timestamp = Date.now()) {
  if (typeof sessionStorage === "undefined") return;
  try {
    sessionStorage.setItem(SUMMARY_LAST_FETCH_KEY, String(timestamp));
  } catch (error) {
    // Ignore storage errors (private mode, etc.)
  }
}

function hasRecentSummaryFetch() {
  const timestamp = readLastSummaryFetchTimestamp();
  if (!timestamp) return false;
  return (Date.now() - timestamp) < SUMMARY_PREFETCH_TTL;
}

function storeSummaryPrefetch(payload) {
  if (typeof window === "undefined" || typeof window.setPagePrefetchData !== "function") {
    return;
  }
  window.setPagePrefetchData(SUMMARY_PREFETCH_KEY, payload, { ttl: SUMMARY_PREFETCH_TTL });
  markSummaryFetchTimestamp();
}

async function fetchSummaryPayload(forceNetwork = false) {
  const url = forceNetwork ? "/api/summary?refresh=1" : "/api/summary";
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error("Failed to load summary data");
  }
  return response.json();
}

async function requestSummaryData(forceNetwork = false) {
  const shouldForce = forceNetwork || !hasRecentSummaryFetch();

  if (!shouldForce) {
    const cached = readSummaryPrefetchCache();
    if (cached !== undefined && cached !== null) {
      return cached;
    }
  }

  if (typeof window !== "undefined" && typeof window.runPagePrefetch === "function") {
    try {
      const prefetched = await window.runPagePrefetch(SUMMARY_PREFETCH_KEY, { force: shouldForce });
      if (prefetched !== undefined && prefetched !== null) {
        if (shouldForce) {
          markSummaryFetchTimestamp();
        }
        return prefetched;
      }
    } catch (error) {
      console.warn("Summary prefetch runner failed", error);
    }
  }

  const payload = await fetchSummaryPayload(shouldForce);
  storeSummaryPrefetch(payload);
  return payload;
}

function ensureSummaryPrefetcher() {
  if (summaryPrefetchRegistered) return;
  if (typeof window === "undefined" || typeof window.registerPagePrefetcher !== "function") {
    return;
  }
  summaryPrefetchRegistered = true;
  window.registerPagePrefetcher(SUMMARY_PREFETCH_KEY, async ({ setCached }) => {
    const data = await fetchSummaryPayload();
    setCached(data, { ttl: SUMMARY_PREFETCH_TTL });
    markSummaryFetchTimestamp();
    return data;
  });
}

function primeCriticalPrefetchers() {
  if (criticalPrefetchPrimed) return;
  if (typeof window === "undefined" || typeof window.runPagePrefetch !== "function") {
    return;
  }
  criticalPrefetchPrimed = true;
  CRITICAL_PREFETCH_KEYS.forEach((key, index) => {
    setTimeout(() => {
      window.runPagePrefetch(key).catch(() => {});
    }, index * 120);
  });
}

ensureSummaryPrefetcher();

function normalizeBrokerName(value) {
  if (!value) return "";
  return String(value).toLowerCase().trim();
}

function buildBrokerKey(label) {
  if (!label) return "";
  return String(label)
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[-_]/g, "")
    .replace(/&/g, "and")
    .replace(/\./g, "");
}

function getBrokerIcon(label) {
  const icons = window.__BROKER_ICONS__ || {};
  const key = buildBrokerKey(label);
  return key && icons[key] ? icons[key] : undefined;
}

function formatCurrency(value) {
  const numeric = Number(value) || 0;
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  }).format(numeric);
}

function formatPercentage(value) {
  const numeric = Number(value) || 0;
  return `${numeric.toFixed(2)}%`;
}

function formatQuantity(value) {
  const numeric = Number(value) || 0;
  if (Number.isInteger(numeric)) {
    return numeric.toLocaleString();
  }
  return numeric.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatTimestamp(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  });
}

function renderOverview(overview = {}) {
  const fields = {
    total_portfolio_value: formatCurrency(overview.total_portfolio_value),
    total_investment: formatCurrency(overview.total_investment),
    total_gain_loss: formatCurrency(overview.total_gain_loss),
    total_gain_loss_percent: formatPercentage(overview.total_gain_loss_percent),
    today_gain_loss: formatCurrency(overview.today_gain_loss),
    today_gain_loss_percent: formatPercentage(overview.today_gain_loss_percent),
  };

  Object.entries(fields).forEach(([field, display]) => {
    document.querySelectorAll(`[data-summary-field="${field}"]`).forEach((element) => {
      element.textContent = display;
    });
  });

  const gainValueElements = document.querySelectorAll("[data-summary-field='total_gain_loss']");
  gainValueElements.forEach((element) => {
    const isPositive = Number(overview.total_gain_loss) >= 0;
    element.classList.toggle("summary-stat-positive", isPositive);
    element.classList.toggle("summary-stat-negative", !isPositive);
  });

  const gainPercentElements = document.querySelectorAll("[data-summary-field='total_gain_loss_percent']");
  gainPercentElements.forEach((element) => {
    const isPositive = Number(overview.total_gain_loss_percent) >= 0;
    element.classList.toggle("summary-stat-positive", isPositive);
    element.classList.toggle("summary-stat-negative", !isPositive);
  });

  const todayValueElements = document.querySelectorAll("[data-summary-field='today_gain_loss']");
  todayValueElements.forEach((element) => {
    const isPositive = Number(overview.today_gain_loss) >= 0;
    element.classList.toggle("summary-stat-positive", isPositive);
    element.classList.toggle("summary-stat-negative", !isPositive);
  });

  const todayPercentElements = document.querySelectorAll("[data-summary-field='today_gain_loss_percent']");
  todayPercentElements.forEach((element) => {
    const isPositive = Number(overview.today_gain_loss_percent) >= 0;
    element.classList.toggle("summary-stat-positive", isPositive);
    element.classList.toggle("summary-stat-negative", !isPositive);
  });
}

function buildHoldingRow(holding) {
  const brokers = Array.isArray(holding.brokers) ? holding.brokers : [];
  const brokerLabel = holding.primary_broker || brokers[0] || "";
  const brokerIcon = getBrokerIcon(brokerLabel) || "/static/images/logo.png";
  const brokerAlt = `${brokerLabel || "Broker"} logo`;
  const symbol = holding.symbol || "—";
  const avatarText = symbol.slice(0, 2).toUpperCase();
  const productLabel = holding.product || "Normal";
  const exchangeBadge = holding.exchange ? String(holding.exchange).toUpperCase() : "";
  const pnl = Number(holding.pnl) || 0;
  const pnlPositive = pnl >= 0;
  const buyAvg =
    holding.buy_avg_price ??
    (holding.quantity > 0 && holding.cost
      ? Number(holding.cost) / Number(holding.quantity || 1)
      : null);
  const sellAvg =
    holding.sell_avg_price ??
    (holding.quantity < 0 && holding.cost
      ? Number(holding.cost) / Number(-holding.quantity || 1)
      : null);

  return `
    <tr data-brokers='${JSON.stringify(brokers)}'>
      <td>
        <div style="display: flex; align-items: center; gap: 8px;">
          <div style="height: 28px; width: 28px; border-radius: 6px; background-color: #f3f4f6; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 600; color:#111827;">
            ${avatarText}
          </div>
          <div style="display: flex; align-items: center; gap: 6px; flex-wrap: wrap;">
            <span style="font-weight: 500; color:#111827;">${symbol}</span>
            ${exchangeBadge ? `<span style="border: 1px solid rgba(107,114,128,0.3); font-size: 9px; line-height: 1; padding: 2px 4px; height: 16px; border-radius: 4px; color:#374151; background-color:#ffffff;">${exchangeBadge}</span>` : ""}
            ${holding.sector ? `<span style="border: 1px solid #fde68a; background-color:#fffbeb; color:#92400e; font-size: 9px; line-height:1; padding:2px 4px; height:16px; border-radius:4px; font-weight:500;">${String(holding.sector).slice(0,1).toUpperCase()}</span>` : ""}
          </div>
        </div>
      </td>
      <td><span style="border:1px solid #10b981; background-color:#ecfdf5; color:#047857; font-size:10px; line-height:1; padding:2px 6px; border-radius:4px; font-weight:500; display:inline-block;">${productLabel}</span></td>
      <td>${formatQuantity(holding.quantity)}</td>
      <td>${buyAvg !== null && buyAvg !== undefined ? formatCurrency(buyAvg) : "—"}</td>
      <td>${sellAvg !== null && sellAvg !== undefined ? formatCurrency(sellAvg) : "—"}</td>
      <td>${formatCurrency(holding.ltp)}</td>
      <td><span style="color:${pnlPositive ? "#16a34a" : "#dc2626"}; font-weight:600;">${formatCurrency(pnl)}</span></td>
      <td><img src="${brokerIcon}" alt="${brokerAlt}" title="${brokers.join(", ")}" style="height: 28px; width: 28px; display: block; object-fit: contain; margin-left: auto; margin-right: auto; border-radius: 6px; background: transparent;" /></td>
    </tr>
  `;
}

function buildHoldingCard(holding) {
  const brokers = Array.isArray(holding.brokers) ? holding.brokers : [];
  const brokerLabel = holding.primary_broker || brokers[0] || "";
  const brokerIcon = getBrokerIcon(brokerLabel) || "/static/images/logo.png";
  const brokerAlt = `${brokerLabel || "Broker"} logo`;
  const symbol = holding.symbol || "—";
  const avatarText = symbol.slice(0, 2).toUpperCase();
  const productLabel = holding.product || "Normal";
  const exchangeBadge = holding.exchange ? String(holding.exchange).toUpperCase() : "";
  const pnl = Number(holding.pnl) || 0;
  const pnlPositive = pnl >= 0;
  const cost = Number(holding.cost) || 0;
  const pnlPercent = Number.isFinite(holding.pnl_percent)
    ? Number(holding.pnl_percent)
    : cost
      ? (pnl / cost) * 100
      : 0;
  const marketValue =
    holding.market_value !== undefined && holding.market_value !== null
      ? holding.market_value
      : cost + pnl;
  const buyAvg =
    holding.buy_avg_price ??
    (holding.quantity > 0 && cost ? cost / Number(holding.quantity || 1) : null);
  const sellAvg =
    holding.sell_avg_price ??
    (holding.quantity < 0 && cost ? cost / Number(-holding.quantity || 1) : null);

  return `
    <div class="top-holdings-card" data-brokers='${JSON.stringify(brokers)}'>
      <div class="top-holdings-card__header">
        <div class="top-holdings-card__identity">
          <div class="top-holdings-card__avatar">${avatarText}</div>
          <div>
            <p class="top-holdings-card__symbol">${symbol}</p>
            <p class="top-holdings-card__subtitle">${holding.sector || "Uncategorized"}</p>
          </div>
        </div>
        <div class="top-holdings-card__value">
          ${formatCurrency(marketValue)}
          <span class="top-holdings-card__value-delta ${pnlPositive ? "is-positive" : "is-negative"}">
            ${formatCurrency(pnl)} (${formatPercentage(pnlPercent)})
          </span>
        </div>
      </div>
      <div class="top-holdings-card__chips">
        <span class="top-holdings-chip top-holdings-chip--product">${productLabel}</span>
        ${exchangeBadge ? `<span class="top-holdings-chip">${exchangeBadge}</span>` : ""}
        <span class="top-holdings-card__broker">
          <img src="${brokerIcon}" alt="${brokerAlt}" title="${brokers.join(", ")}">
        </span>
      </div>
      <div class="top-holdings-card__meta">
        <div class="top-holdings-card__qty">
          <span>Qty.</span>
          <strong>${formatQuantity(holding.quantity)}</strong>
          ${buyAvg !== null && buyAvg !== undefined ? `<span>&times; ${formatCurrency(buyAvg)}</span>` : ""}
          ${sellAvg !== null && sellAvg !== undefined ? `<span>(Sell ${formatCurrency(sellAvg)})</span>` : ""}
        </div>
        <div class="top-holdings-card__ltp">LTP ${formatCurrency(holding.ltp)}</div>
      </div>
    </div>
  `;
}

function updateBrokerFilterOptions(brokerNames = []) {
  const brokerFilter = document.getElementById("top-holdings-broker-filter");
  if (!brokerFilter || !Array.isArray(brokerNames)) return;
  const currentValue = brokerFilter.value;
  brokerFilter.innerHTML = brokerNames
    .map((name) => `<option value="${name}">${name}</option>`)
    .join("");
  if (brokerNames.includes(currentValue)) {
    brokerFilter.value = currentValue;
  }
}

function renderTopHoldings(topHoldings = [], brokerNames = []) {
  const root = document.getElementById("top-holdings-root");
  const holdingsToggle = document.getElementById("top-holdings-toggle");
  updateBrokerFilterOptions(brokerNames);

  if (!root) return;
  if (!Array.isArray(topHoldings) || !topHoldings.length) {
    root.innerHTML = `<div style="padding: 24px 16px; text-align: center; color: #6b7280; font-size: 12px;">No holdings available. Connect an account or refresh your positions.</div>`;
    if (holdingsToggle) holdingsToggle.hidden = true;
    return;
  }

  const rows = topHoldings.map((holding) => buildHoldingRow(holding)).join("");
  const cards = topHoldings.map((holding) => buildHoldingCard(holding)).join("");

  root.innerHTML = `
    <div class="top-holdings-table-container">
      <table class="top-holdings-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Product</th>
            <th>Qty</th>
            <th>Buy Avg Price</th>
            <th>Sell Avg Price</th>
            <th>LTP</th>
            <th>P&amp;L</th>
            <th></th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    <div class="top-holdings-card-list">${cards}</div>
    <div id="top-holdings-empty-state" class="top-holdings-empty" hidden>No holdings match the selected broker.</div>
  `;

  if (holdingsToggle) holdingsToggle.hidden = true;
  setupHoldingsFilter();
}

function renderRecentTransactions(transactions = []) {
  const container = document.getElementById("recent-transactions-root");
  if (!container) return;

  if (!Array.isArray(transactions) || !transactions.length) {
    container.innerHTML = '<p class="summary-stat-footnote" style="margin:0;">No recent transactions found.</p>';
    return;
  }

  const items = transactions
    .map((txn) => {
      const action = (txn.action || "").toString().toUpperCase();
      const isBuy = action === "BUY";
      const isSell = action === "SELL";
      const isDividend = !isBuy && !isSell;
      const iconBg = isBuy
        ? "rgba(5,150,105,0.10)"
        : isSell
          ? "rgba(220,38,38,0.10)"
          : "rgba(37,99,235,0.10)";
      const iconColor = isBuy
        ? "var(--summary-green)"
        : isSell
          ? "var(--summary-red)"
          : "var(--summary-blue)";
      const pillClass = isSell
        ? "summary-pill summary-pill--sell"
        : isDividend
          ? "summary-pill summary-pill--dividend"
          : "summary-pill";
      const hasPrice = Number(txn.price) > 0;
      const hasValue = Number(txn.value) > 0;
      const timestamp = txn.timestamp_iso || txn.timestamp;
      return `
        <div class="summary-transaction">
          <div class="info">
            <div style="width:40px; height:40px; border-radius:999px; background:${iconBg}; display:flex; align-items:center; justify-content:center; flex-shrink:0;">
              ${isSell
                ? `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="${iconColor}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><line x1="7" y1="17" x2="17" y2="7"></line><polyline points="7 7 17 7 17 17"></polyline></svg>`
                : isBuy
                  ? `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="${iconColor}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><line x1="7" y1="7" x2="17" y2="17"></line><polyline points="17 7 17 17 7 17"></polyline></svg>`
                  : `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="${iconColor}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 3h12"></path><path d="M6 8h12"></path><path d="M6 13a5 5 0 0 0 5 5h1l-6 3"></path></svg>`}
            </div>
            <div style="min-width:0;">
              <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                <p style="margin:0; font-weight:700;">${txn.symbol || "—"}</p>
                <span class="${pillClass}">${action ? action[0] + action.slice(1).toLowerCase() : "Trade"}</span>
              </div>
              <p style="margin:2px 0 0 0; color:var(--summary-muted); font-size:12px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">
                ${formatQuantity(txn.quantity)} shares${hasPrice ? ` @ ${formatCurrency(txn.price)}` : ""} • ${txn.broker || "—"}
              </p>
            </div>
          </div>
          <div class="timestamp">
            <time datetime="${timestamp || ""}">${formatTimestamp(timestamp)}</time>
          </div>
          <p class="summary-value">${hasValue ? formatCurrency(txn.value) : "—"}</p>
        </div>
      `;
    })
    .join("");

  container.innerHTML = items;
}

function extractBrokers(element, cache) {
  if (!element) return [];
  if (cache.has(element)) return cache.get(element);
  const rawValue = element.dataset?.brokers;
  let brokerList = [];
  if (rawValue) {
    try {
      const parsed = JSON.parse(rawValue);
      if (Array.isArray(parsed)) {
        brokerList = parsed.map((item) => String(item));
      }
    } catch (error) {
      brokerList = String(rawValue)
        .split(/\s*,\s*/)
        .map((item) => item.trim())
        .filter(Boolean);
    }
  }
  cache.set(element, brokerList);
  return brokerList;
}

function setupHoldingsFilter() {
  const brokerCache = new WeakMap();
  const brokerFilter = document.getElementById("top-holdings-broker-filter");
  const holdingsToggle = document.getElementById("top-holdings-toggle");
  const holdingsEmptyState = document.getElementById("top-holdings-empty-state");
  const holdingsTableContainer = document.querySelector(".top-holdings-table-container");
  const holdingsCardList = document.querySelector(".top-holdings-card-list");

  let holdingsVisibleCount = HOLDINGS_LIMIT;

  const getTableRows = () =>
    holdingsTableContainer ? Array.from(holdingsTableContainer.querySelectorAll("tbody tr")) : [];
  const getCardItems = () =>
    holdingsCardList ? Array.from(holdingsCardList.querySelectorAll(".top-holdings-card")) : [];

  function matchesBroker(element, normalizedSelection, showAllSelection) {
    if (!element) return false;
    if (showAllSelection) return true;
    const brokers = extractBrokers(element, brokerCache)
      .map((name) => normalizeBrokerName(name))
      .filter(Boolean);
    if (!brokers.length) return false;
    return brokers.includes(normalizedSelection);
  }

  function applyBrokerFilter(selectedBroker) {
    const normalizedSelection = normalizeBrokerName(
      typeof selectedBroker === "string" ? selectedBroker : "",
    );
    const showAllSelection = !normalizedSelection || normalizedSelection === "all";

    const holdingsTableRows = getTableRows();
    const holdingsCardItems = getCardItems();

    let visibleRows = 0;
    let visibleCards = 0;
    let tableMatchCount = 0;
    let cardMatchCount = 0;

    holdingsTableRows.forEach((row) => {
      const isMatch = matchesBroker(row, normalizedSelection, showAllSelection);
      if (isMatch) {
        const shouldShow = tableMatchCount < holdingsVisibleCount;
        row.style.display = shouldShow ? "" : "none";
        tableMatchCount += 1;
        if (shouldShow) visibleRows += 1;
      } else {
        row.style.display = "none";
      }
    });

    holdingsCardItems.forEach((card) => {
      const isMatch = matchesBroker(card, normalizedSelection, showAllSelection);
      if (isMatch) {
        const shouldShow = cardMatchCount < holdingsVisibleCount;
        card.style.display = shouldShow ? "" : "none";
        cardMatchCount += 1;
        if (shouldShow) visibleCards += 1;
      } else {
        card.style.display = "none";
      }
    });

    if (holdingsTableContainer) {
      holdingsTableContainer.hidden = holdingsTableRows.length > 0 && visibleRows === 0;
    }
    if (holdingsCardList) {
      holdingsCardList.hidden = holdingsCardItems.length > 0 && visibleCards === 0;
    }
    if (holdingsEmptyState) {
      const hasVisible = visibleRows + visibleCards > 0;
      holdingsEmptyState.hidden = hasVisible;
    }

    const totalMatches = Math.max(tableMatchCount, cardMatchCount);
    if (totalMatches < holdingsVisibleCount) {
      holdingsVisibleCount = Math.max(HOLDINGS_LIMIT, totalMatches);
    }

    if (holdingsToggle) {
      const hasMoreToShow = totalMatches > holdingsVisibleCount;
      const canShowLess = holdingsVisibleCount > HOLDINGS_LIMIT && totalMatches > HOLDINGS_LIMIT;
      const shouldShowToggle = totalMatches > HOLDINGS_LIMIT && (hasMoreToShow || canShowLess);
      holdingsToggle.hidden = !shouldShowToggle;
      if (!holdingsToggle.hidden) {
        holdingsToggle.textContent = hasMoreToShow ? "Load more" : "Show less";
      }
      holdingsToggle.dataset.state = hasMoreToShow ? "more" : "less";
    }
  }

  function handleHoldingsLoadMore() {
    const totalHoldings = Math.max(getTableRows().length, getCardItems().length);
    if (totalHoldings <= HOLDINGS_LIMIT) return;

    const shouldReset = holdingsToggle && holdingsToggle.dataset.state === "less";
    if (shouldReset) {
      holdingsVisibleCount = HOLDINGS_LIMIT;
    } else {
      holdingsVisibleCount = Math.min(totalHoldings, holdingsVisibleCount + HOLDINGS_LIMIT);
    }

    applyBrokerFilter(brokerFilter ? brokerFilter.value : "");
  }

  if (brokerFilter) {
    applyBrokerFilter(brokerFilter.value);
    if (brokerFilter.dataset.bound !== "true") {
      brokerFilter.addEventListener("change", () => {
        holdingsVisibleCount = HOLDINGS_LIMIT;
        applyBrokerFilter(brokerFilter.value);
      });
      brokerFilter.dataset.bound = "true";
    }
  } else {
    applyBrokerFilter("");
  }

  if (holdingsToggle && holdingsToggle.dataset.bound !== "true") {
    holdingsToggle.addEventListener("click", (event) => {
      event.preventDefault();
      handleHoldingsLoadMore();
    });
    holdingsToggle.dataset.bound = "true";
  }
}  

function setupTransactionsToggle() {
  const transactionsToggle = document.getElementById("recent-transactions-toggle");
  let transactionsShowAll = false;

  function getTransactionItems() {
    return Array.from(document.querySelectorAll(".summary-transaction"));
  }

  function updateTransactionsVisibility() {
    const transactionItems = getTransactionItems();
    if (!transactionItems.length) {
      if (transactionsToggle) transactionsToggle.hidden = true;
      return;
    }
    const shouldCollapse = transactionItems.length > TRANSACTIONS_LIMIT;
    if (!shouldCollapse && transactionsShowAll) {
      transactionsShowAll = false;
    }
    transactionItems.forEach((item, index) => {
      const shouldShow = transactionsShowAll || index < TRANSACTIONS_LIMIT;
      item.style.display = shouldShow ? "" : "none";
    });
    if (transactionsToggle) {
      transactionsToggle.hidden = !shouldCollapse;
      if (shouldCollapse) {
        transactionsToggle.textContent = transactionsShowAll ? "View Less" : "View All";
      }
    }
  }

  updateTransactionsVisibility();
  if (transactionsToggle && transactionsToggle.dataset.bound !== "true") {
    transactionsToggle.addEventListener("click", (event) => {
      event.preventDefault();
      const transactionItems = getTransactionItems();
      if (transactionItems.length <= TRANSACTIONS_LIMIT) return;
      transactionsShowAll = !transactionsShowAll;
      updateTransactionsVisibility();
    });
    transactionsToggle.dataset.bound = "true";
  }
}

function applySummaryPayload(payload) {
  if (!payload || typeof payload !== "object") return;
  renderOverview(payload.overview || {});
  renderTopHoldings(payload.top_holdings || [], payload.broker_names || []);
  renderRecentTransactions(payload.recent_transactions || []);
  setupTransactionsToggle();
}

async function runSummaryRefresh({ forceNetwork = true, silent = false } = {}) {
  if (summaryRefreshInFlight) return null;
  summaryRefreshInFlight = true;
  try {
    const payload = await requestSummaryData(forceNetwork);
    applySummaryPayload(payload);
    document.dispatchEvent(new CustomEvent("summary:refresh", { detail: { payload } }));
    return payload;
  } catch (error) {
    if (!silent) {
      console.error("Failed to refresh summary", error);
    }
    return null;
  } finally {
    summaryRefreshInFlight = false;
  }
}

function setupSummaryRefresh() {
  const refreshButton = document.getElementById("summary-refresh-button");
  if (!refreshButton) return;

  const labelEl = refreshButton.querySelector("span");
  const defaultLabel = labelEl ? labelEl.textContent : refreshButton.textContent;

  refreshButton.addEventListener("click", async () => {
    if (summaryRefreshInFlight) return;
    refreshButton.disabled = true;
    if (labelEl) labelEl.textContent = "Refreshing...";

    await runSummaryRefresh({ forceNetwork: true, silent: false });
    refreshButton.disabled = false;
    if (labelEl) labelEl.textContent = defaultLabel;
  });
}

function setupBackgroundSummaryUpdates() {
  if (typeof document === "undefined") return;

  document.addEventListener("accounts:changed", () => {
    runSummaryRefresh({ forceNetwork: true, silent: true });
  });

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState !== "visible") return;
    runSummaryRefresh({ forceNetwork: true, silent: true });
  });
}

export function initSummaryIslands() {
  setupHoldingsFilter();
  setupTransactionsToggle();
  setupSummaryRefresh();
  setupBackgroundSummaryUpdates();
  primeCriticalPrefetchers();
}
