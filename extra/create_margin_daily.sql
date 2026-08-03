CREATE TABLE IF NOT EXISTS public.margin_daily (
    trade_date date NOT NULL,
    exchange_id character varying(10) NOT NULL,
    rzye numeric(20, 2),
    rzmre numeric(20, 2),
    rzche numeric(20, 2),
    rqye numeric(20, 2),
    rqmcl numeric(20, 2),
    rzrqye numeric(20, 2),
    rqyl numeric(20, 2),
    PRIMARY KEY (trade_date, exchange_id)
);

CREATE INDEX IF NOT EXISTS margin_daily_exchange_id_idx ON public.margin_daily (exchange_id);

COMMENT ON TABLE public.margin_daily IS 'Tushare 融资融券每日交易汇总数据';

COMMENT ON COLUMN public.margin_daily.trade_date IS '交易日期';
COMMENT ON COLUMN public.margin_daily.exchange_id IS '交易所代码（SSE/SZSE/BSE）';
COMMENT ON COLUMN public.margin_daily.rzye IS '融资余额（元）';
COMMENT ON COLUMN public.margin_daily.rzmre IS '融资买入额（元）';
COMMENT ON COLUMN public.margin_daily.rzche IS '融资偿还额（元）';
COMMENT ON COLUMN public.margin_daily.rqye IS '融券余额（元）';
COMMENT ON COLUMN public.margin_daily.rqmcl IS '融券卖出量（股、份或手）';
COMMENT ON COLUMN public.margin_daily.rzrqye IS '融资融券余额（元）';
COMMENT ON COLUMN public.margin_daily.rqyl IS '融券余量（股、份或手）';
