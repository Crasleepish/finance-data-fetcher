CREATE TABLE IF NOT EXISTS public.stock_code_change (
    trade_code character varying(20) NOT NULL,
    code character varying(20) NOT NULL,
    start_date date NOT NULL,
    name character varying(50) NOT NULL,
    end_date date,
    CONSTRAINT stock_code_change_pkey PRIMARY KEY (trade_code, code, start_date),
    CONSTRAINT stock_code_change_date_order CHECK (end_date IS NULL OR end_date >= start_date),
    CONSTRAINT stock_code_change_code_format CHECK (code ~ '^[0-9]{6}\.(SH|SZ|BJ)$'),
    CONSTRAINT stock_code_change_trade_code_format CHECK (trade_code ~ '^[0-9]{6}\.(SH|SZ|BJ)$')
);

CREATE INDEX IF NOT EXISTS stock_code_change_code_idx
    ON public.stock_code_change USING btree (code);

CREATE INDEX IF NOT EXISTS stock_code_change_trade_code_idx
    ON public.stock_code_change USING btree (trade_code);

COMMENT ON TABLE public.stock_code_change IS 'A股股票代码变更区间（最新名称 + 历史实际代码区间）';
COMMENT ON COLUMN public.stock_code_change.trade_code IS '最新代码（带 .SH/.SZ/.BJ 后缀）';
COMMENT ON COLUMN public.stock_code_change.code IS '区间内实际代码（带后缀）';
COMMENT ON COLUMN public.stock_code_change.name IS '当前股票名称（非历史名称）';
COMMENT ON COLUMN public.stock_code_change.start_date IS '区间开始日期';
COMMENT ON COLUMN public.stock_code_change.end_date IS '区间结束日期；NULL 表示当前未结束区间';
