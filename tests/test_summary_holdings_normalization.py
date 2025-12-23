import os
from collections import defaultdict

import pytest
import sqlalchemy

os.environ.setdefault("SECRET_KEY", "test-key")
os.environ.setdefault("DATABASE_URL", "postgresql://localhost/test")
os.environ.setdefault("ADMIN_EMAIL", "admin@example.com")
os.environ.setdefault("ADMIN_PASSWORD", "secret")

class _FakeInspector:
    def get_table_names(self):
        return []

    def get_columns(self, _table_name):
        return []


sqlalchemy.inspect = lambda *args, **kwargs: _FakeInspector()

import app


def _build_holdings_snapshot(holdings: list[dict]):
    aggregated_holdings = {}
    sector_map = defaultdict(float)

    for holding in holdings:
        symbol = holding["symbol"] or "Unknown"
        entry = aggregated_holdings.setdefault(
            symbol,
            {
                "symbol": symbol,
                "quantity": 0.0,
                "market_value": 0.0,
                "cost": 0.0,
                "pnl": 0.0,
                "ltp": holding["ltp"],
                "sector": holding["sector"],
                "product": holding.get("product"),
                "exchange": holding.get("exchange"),
                "brokers": set(),
                "buy_total_cost": 0.0,
                "buy_total_qty": 0.0,
                "sell_total_cost": 0.0,
                "sell_total_qty": 0.0,
                "day_change": 0.0,
                "day_change_value": 0.0,
            },
        )
        entry["quantity"] += holding["quantity"]
        entry["market_value"] += holding["market_value"]
        entry["cost"] += holding["cost"]
        entry["pnl"] += holding["pnl"]
        entry["ltp"] = holding["ltp"] or entry.get("ltp") or 0.0
        entry["sector"] = holding["sector"] or entry.get("sector") or "Uncategorized"
        if holding.get("product"):
            entry["product"] = holding.get("product")
        if holding.get("exchange"):
            entry["exchange"] = holding.get("exchange")
        broker_label = holding.get("broker") or holding.get("account_client_id")
        if broker_label:
            entry["brokers"].add(str(broker_label))
        entry["day_change"] += app._summary_safe_float(holding.get("day_change"), 0.0)
        entry["day_change_value"] += app._summary_safe_float(
            holding.get("day_change_value"), 0.0
        )
        qty = holding["quantity"]
        if qty > 0 and holding.get("buy_avg_price"):
            entry["buy_total_cost"] += holding["buy_avg_price"] * qty
            entry["buy_total_qty"] += qty
        elif qty < 0 and holding.get("sell_avg_price"):
            qty_abs = abs(qty)
            entry["sell_total_cost"] += holding["sell_avg_price"] * qty_abs
            entry["sell_total_qty"] += qty_abs
        sector_map[entry["sector"]] += max(holding["market_value"], 0.0)

    processed_holdings = []
    for holding in aggregated_holdings.values():
        buy_qty = holding.pop("buy_total_qty", 0.0)
        buy_cost = holding.pop("buy_total_cost", 0.0)
        sell_qty = holding.pop("sell_total_qty", 0.0)
        sell_cost = holding.pop("sell_total_cost", 0.0)
        holding["buy_avg_price"] = (buy_cost / buy_qty) if buy_qty else None
        holding["sell_avg_price"] = (sell_cost / sell_qty) if sell_qty else None
        brokers = holding.pop("brokers", set())
        brokers_list = sorted(brokers)
        holding["brokers"] = brokers_list
        holding["primary_broker"] = brokers_list[0] if brokers_list else None
        processed_holdings.append(holding)

    combined_holdings = sorted(
        processed_holdings, key=lambda h: h["market_value"], reverse=True
    )
    for holding in combined_holdings:
        cost = holding.get("cost", 0.0)
        pnl = holding.get("pnl", 0.0)
        holding["pnl_percent"] = (pnl / cost * 100.0) if cost else 0.0

    top_holdings = combined_holdings[:5]
    return combined_holdings, top_holdings


def test_alice_blue_holding_maps_prices_and_quantities():
    alice_holding = {
        "Nsetsym": "SBIN-EQ",
        "Holdqty": "15",
        "SellableQty": "15",
        "Price": "525.5",
        "Ltp": "540.2",
    }

    normalized = app._normalize_holding_for_summary(alice_holding)
    assert normalized["tradingSymbol"] == "SBIN-EQ"
    assert normalized["netQty"] == 15.0
    assert normalized["buyAvg"] == 525.5
    assert normalized["ltp"] == 540.2

    metrics, holdings = app._compute_account_metrics([normalized])
    assert metrics["portfolio_value"] > 0

    holding = holdings[0]
    expected_pnl = (540.2 - 525.5) * 15
    assert holding["symbol"] == "SBIN-EQ"
    assert holding["quantity"] == 15.0
    assert holding["buy_avg_price"] == 525.5
    assert holding["ltp"] == 540.2
    assert holding["pnl"] == pytest.approx(expected_pnl)

    combined_holdings, top_holdings = _build_holdings_snapshot(holdings)
    assert combined_holdings[0]["symbol"] == "SBIN-EQ"
    assert combined_holdings[0]["quantity"] == 15.0
    assert combined_holdings[0]["buy_avg_price"] == 525.5
    assert combined_holdings[0]["ltp"] == 540.2
    assert combined_holdings[0]["pnl"] == pytest.approx(expected_pnl)
    assert top_holdings[0] == combined_holdings[0]
