CREATE TABLE IF NOT EXISTS public.market_daily_info (
    trade_date date NOT NULL,
    ts_code character varying(20) NOT NULL,
    ts_name character varying(50),
    com_count integer,
    total_share numeric(20, 4),
    float_share numeric(20, 4),
    total_mv numeric(20, 4),
    float_mv numeric(20, 4),
    amount numeric(20, 4),
    vol numeric(20, 4),
    trans_count numeric(20, 4),
    pe numeric(20, 4),
    tr numeric(20, 4),
    exchange character varying(10),
    PRIMARY KEY (trade_date, ts_code)
);

COMMENT ON TABLE public.market_daily_info IS 'Tushare 市场交易统计（每日）数据';

COMMENT ON COLUMN public.market_daily_info.trade_date IS '交易日期';
COMMENT ON COLUMN public.market_daily_info.ts_code IS '市场代码';
COMMENT ON COLUMN public.market_daily_info.ts_name IS '市场名称';
COMMENT ON COLUMN public.market_daily_info.com_count IS '挂牌数';
COMMENT ON COLUMN public.market_daily_info.total_share IS '总股本（亿股）';
COMMENT ON COLUMN public.market_daily_info.float_share IS '流通股本（亿股）';
COMMENT ON COLUMN public.market_daily_info.total_mv IS '总市值（亿元）';
COMMENT ON COLUMN public.market_daily_info.float_mv IS '流通市值（亿元）';
COMMENT ON COLUMN public.market_daily_info.amount IS '交易金额（亿元）';
COMMENT ON COLUMN public.market_daily_info.vol IS '成交量（亿股）';
COMMENT ON COLUMN public.market_daily_info.trans_count IS '成交笔数（万笔）';
COMMENT ON COLUMN public.market_daily_info.pe IS '市盈率（倍）';
COMMENT ON COLUMN public.market_daily_info.tr IS '换手率（%）';
COMMENT ON COLUMN public.market_daily_info.exchange IS '交易所（SH/SZ）';
