CREATE TABLE IF NOT EXISTS public.index_daily_basic (
    ts_code character varying(20) NOT NULL,
    trade_date date NOT NULL,
    total_mv double precision,
    float_mv double precision,
    total_share double precision,
    float_share double precision,
    free_share double precision,
    turnover_rate double precision,
    turnover_rate_f double precision,
    pe double precision,
    pe_ttm double precision,
    pb double precision,
    PRIMARY KEY (ts_code, trade_date)
);

COMMENT ON TABLE public.index_daily_basic IS 'Tushare 大盘指数每日指标数据';

COMMENT ON COLUMN public.index_daily_basic.ts_code IS '指数代码';
COMMENT ON COLUMN public.index_daily_basic.trade_date IS '交易日期';
COMMENT ON COLUMN public.index_daily_basic.total_mv IS '总市值（万元）';
COMMENT ON COLUMN public.index_daily_basic.float_mv IS '流通市值（万元）';
COMMENT ON COLUMN public.index_daily_basic.total_share IS '总股本（万股）';
COMMENT ON COLUMN public.index_daily_basic.float_share IS '流通股本（万股）';
COMMENT ON COLUMN public.index_daily_basic.free_share IS '自由流通股本（万股）';
COMMENT ON COLUMN public.index_daily_basic.turnover_rate IS '换手率（%）';
COMMENT ON COLUMN public.index_daily_basic.turnover_rate_f IS '换手率（自由流通股本）（%）';
COMMENT ON COLUMN public.index_daily_basic.pe IS '市盈率（倍）';
COMMENT ON COLUMN public.index_daily_basic.pe_ttm IS '市盈率（TTM）（倍）';
COMMENT ON COLUMN public.index_daily_basic.pb IS '市净率（倍）';
