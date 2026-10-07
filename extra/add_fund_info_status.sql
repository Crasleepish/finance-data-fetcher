ALTER TABLE public.fund_info
    ADD COLUMN IF NOT EXISTS status character varying(8);

COMMENT ON COLUMN public.fund_info.status IS 'Tushare fund_basic 存续状态（D 摘牌 / I 发行 / L 上市中）；OTC 生命周期辅助信号，非 PIT 存续区间。';
