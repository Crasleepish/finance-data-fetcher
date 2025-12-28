from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    Sequence,
    String,
    Table,
    Text,
    UniqueConstraint,
    desc,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()

test_messages = Table(
    "test_messages",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("message", String(length=255), nullable=False),
    Column(
        "update_time",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

task_table = Table(
    "task_table",
    metadata,
    Column("task_id", BigInteger, primary_key=True, autoincrement=True),
    Column("idempotency_key", String(length=64), nullable=False),
    Column("spec", String(length=128), nullable=False),
    Column("state", String(length=16), nullable=False),
    Column("attempt", Integer, nullable=False),
    Column("progress", Numeric(precision=5, scale=2), nullable=False, server_default=text("0")),
    Column("error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("last_heartbeat_at", DateTime(timezone=True)),
    Column("task_payload", JSONB, nullable=False),
)

Index("task_table_idempotency_key_idx", task_table.c.idempotency_key)
Index("task_table_state_idx", task_table.c.state)
Index(
    "uq_task_table_active_key",
    task_table.c.idempotency_key,
    unique=True,
    postgresql_where=task_table.c.state.in_(["PENDING", "RUNNING"]),
)

adj_factor = Table(
    "adj_factor",
    metadata,
    Column("stock_code", String(length=10), primary_key=True),
    Column("date", Date, primary_key=True),
    Column("adj_factor", Float(precision=53)),
    Index("adj_factor_date_idx", "date", postgresql_include=["adj_factor"]),
    Index(
        "adj_factor_stock_code_date_idx",
        "stock_code",
        desc("date"),
        postgresql_include=["adj_factor"],
    ),
)

etf_hist = Table(
    "etf_hist",
    metadata,
    Column("etf_code", String(length=20), primary_key=True),
    Column("date", Date, primary_key=True),
    Column("open", Float(precision=53)),
    Column("close", Float(precision=53)),
    Column("high", Float(precision=53)),
    Column("low", Float(precision=53)),
    Column("volume", BigInteger),
    Column("amount", Float(precision=53)),
    Column("change_percent", Float(precision=53)),
    Column("change", Float(precision=53)),
)

etf_info = Table(
    "etf_info",
    metadata,
    Column("etf_code", String(length=20), primary_key=True),
    Column("etf_name", String(length=50), nullable=False),
    Column("fund_type", String(length=20)),
    Column("invest_type", String(length=20)),
    Column("found_date", Date),
)

fund_beta = Table(
    "fund_beta",
    metadata,
    Column("code", String(length=20), primary_key=True),
    Column("date", Date, primary_key=True),
    Column("MKT", Float(precision=53)),
    Column("SMB", Float(precision=53)),
    Column("HML", Float(precision=53)),
    Column("QMJ", Float(precision=53)),
    Column("const", Float(precision=53)),
    Column("P_json", Text),
    Column("P_bin", LargeBinary),
    Column("log_nav_true", Float(precision=53)),
    Column("log_nav_fit", Float(precision=53)),
    Column("gamma", Float(precision=53)),
    Index("idx_fund_beta_date", "date"),
)

fund_hist = Table(
    "fund_hist",
    metadata,
    Column("fund_code", String(length=10), primary_key=True),
    Column("date", Date, primary_key=True),
    Column("value", Float(precision=53)),
    Column("net_value", Float(precision=53)),
    Column("change_percent", Float(precision=53)),
)

fund_info = Table(
    "fund_info",
    metadata,
    Column("fund_code", String(length=10), primary_key=True),
    Column("fund_name", String(length=50), nullable=False),
    Column("fund_type", String(length=20)),
    Column("invest_type", String(length=20)),
    Column("found_date", Date, nullable=False),
    Column("fee_rate", Float(precision=53)),
    Column("commission_rate", Float(precision=53)),
    Column("market", String(length=8)),
)

fundamental_data = Table(
    "fundamental_data",
    metadata,
    Column("stock_code", String(length=10), primary_key=True),
    Column("report_date", Date, primary_key=True),
    Column("total_equity", Float(precision=53)),
    Column("total_assets", Float(precision=53)),
    Column("current_liabilities", Float(precision=53)),
    Column("noncurrent_liabilities", Float(precision=53)),
    Column("net_profit", Float(precision=53)),
    Column("operating_profit", Float(precision=53)),
    Column("total_revenue", Float(precision=53)),
    Column("total_cost", Float(precision=53)),
    Column("net_cash_from_operating", Float(precision=53)),
    Column("cash_for_fixed_assets", Float(precision=53)),
    Column("operating_profit_ttm", Float(precision=53)),
    Column("total_liabilities", Float(precision=53)),
)

gold_cftc_report = Table(
    "gold_cftc_report",
    metadata,
    Column(
        "id",
        Integer,
        Sequence("gold_cftc_report_id_seq"),
        primary_key=True,
    ),
    Column("market_name", String(length=128), nullable=False),
    Column("as_of_date", Date, nullable=False),
    Column("report_date", Date, nullable=False),
    Column("contract_market_code", String(length=16)),
    Column("market_code", String(length=8)),
    Column("region_code", String(length=8)),
    Column("commodity_code", String(length=8)),
    Column("futonly_or_combined", String(length=16)),
    Column("open_interest_all", BigInteger),
    Column("prod_merc_long_all", BigInteger),
    Column("prod_merc_short_all", BigInteger),
    Column("swap_long_all", BigInteger),
    Column("swap_short_all", BigInteger),
    Column("swap_spread_all", BigInteger),
    Column("m_money_long_all", BigInteger),
    Column("m_money_short_all", BigInteger),
    Column("m_money_spread_all", BigInteger),
    Column("other_rept_long_all", BigInteger),
    Column("other_rept_short_all", BigInteger),
    Column("other_rept_spread_all", BigInteger),
    Column("tot_rept_long_all", BigInteger),
    Column("tot_rept_short_all", BigInteger),
    Column("nonrept_long_all", BigInteger),
    Column("nonrept_short_all", BigInteger),
    Column("noncomm_net_all", BigInteger),
    Column("created_at", DateTime(timezone=False), nullable=False),
    Column("updated_at", DateTime(timezone=False), nullable=False),
    UniqueConstraint(
        "report_date",
        "contract_market_code",
        "market_code",
        name="uq_gold_cftc_report_date_codes",
    ),
)

gold_future_curve = Table(
    "gold_future_curve",
    metadata,
    Column(
        "id",
        Integer,
        Sequence("gold_future_curve_id_seq"),
        primary_key=True,
    ),
    Column("trade_date", Date, nullable=False),
    Column("symbol", String(length=16), nullable=False),
    Column("contract_symbol", String(length=64)),
    Column("last_price", Numeric(precision=18, scale=4)),
    Column("price_change", Numeric(precision=18, scale=4)),
    Column("open_price", Numeric(precision=18, scale=4)),
    Column("high_price", Numeric(precision=18, scale=4)),
    Column("low_price", Numeric(precision=18, scale=4)),
    Column("previous_price", Numeric(precision=18, scale=4)),
    Column("volume", BigInteger),
    Column("open_interest", BigInteger),
    Column("trade_time_str", String(length=32)),
    Column("product_code", String(length=16)),
    Column("symbol_code", String(length=8)),
    Column("symbol_type", String(length=8)),
    Column("has_options", String(length=8)),
    Column("created_at", DateTime(timezone=False), nullable=False),
    Column("updated_at", DateTime(timezone=False), nullable=False),
    UniqueConstraint("trade_date", "symbol", name="uq_gold_future_curve_date_symbol"),
)

index_hist = Table(
    "index_hist",
    metadata,
    Column("index_code", String(length=20), primary_key=True),
    Column("date", Date, primary_key=True),
    Column("open", Float(precision=53)),
    Column("close", Float(precision=53)),
    Column("high", Float(precision=53)),
    Column("low", Float(precision=53)),
    Column("volume", BigInteger),
    Column("amount", Float(precision=53)),
    Column("change_percent", Float(precision=53)),
    Column("change", Float(precision=53)),
)

index_info = Table(
    "index_info",
    metadata,
    Column("index_code", String(length=20), primary_key=True),
    Column("index_name", String(length=50), nullable=False),
    Column("market", String(length=10), nullable=False),
)

market_factors = Table(
    "market_factors",
    metadata,
    Column("date", Date, primary_key=True),
    Column("MKT", Numeric(precision=10, scale=6)),
    Column("SMB", Numeric(precision=10, scale=6)),
    Column("HML", Numeric(precision=10, scale=6)),
    Column("QMJ", Numeric(precision=10, scale=6)),
    Column("VOL", Numeric(precision=10, scale=6)),
    Column("LIQ", Numeric(precision=10, scale=6)),
)

stock_hist_unadj = Table(
    "stock_hist_unadj",
    metadata,
    Column("stock_code", String(length=10), primary_key=True),
    Column("date", Date, primary_key=True),
    Column("open", Float(precision=53)),
    Column("close", Float(precision=53)),
    Column("high", Float(precision=53)),
    Column("low", Float(precision=53)),
    Column("volume", BigInteger),
    Column("amount", Float(precision=53)),
    Column("pre_close", Float(precision=53)),
    Column("change_percent", Float(precision=53)),
    Column("change", Float(precision=53)),
    Column("turnover_rate", Float(precision=53)),
    Column("turnover_rate_f", Float(precision=53)),
    Column("volume_ratio", Float(precision=53)),
    Column("pe", Float(precision=53)),
    Column("pe_ttm", Float(precision=53)),
    Column("pb", Float(precision=53)),
    Column("ps", Float(precision=53)),
    Column("ps_ttm", Float(precision=53)),
    Column("dv_ratio", Float(precision=53)),
    Column("dv_ttm", Float(precision=53)),
    Column("total_share", BigInteger),
    Column("float_share", BigInteger),
    Column("free_share", BigInteger),
    Column("mkt_cap", BigInteger),
    Column("circ_mv", BigInteger),
    Index("stock_hist_unadj_date_idx", "date"),
    Index(
        "stock_hist_unadj_stock_code_date_idx",
        "stock_code",
        desc("date"),
        postgresql_include=["close", "total_share"],
    ),
)

stock_info = Table(
    "stock_info",
    metadata,
    Column("stock_code", String(length=20), primary_key=True),
    Column("stock_name", String(length=50), nullable=False),
    Column("market", String(length=10)),
    Column("exchange", String(length=10)),
    Column("industry", String(length=50)),
    Column("listing_date", Date, nullable=False),
    Column("list_status", String(length=4)),
)

trade_calendar = Table(
    "trade_calendar",
    metadata,
    Column("date", Date, primary_key=True),
)

us_index = Table(
    "us_index",
    metadata,
    Column("ts_code", Text),
    Column("date", DateTime(timezone=False)),
    Column("bid_open", Float(precision=53)),
    Column("bid_close", Float(precision=53)),
    Column("bid_high", Float(precision=53)),
    Column("bid_low", Float(precision=53)),
    Column("ask_open", Float(precision=53)),
    Column("ask_close", Float(precision=53)),
    Column("ask_high", Float(precision=53)),
    Column("ask_low", Float(precision=53)),
    Column("tick_qty", BigInteger),
)
