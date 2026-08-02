CREATE TABLE IF NOT EXISTS public.moneyflow_hsgt (
    date date PRIMARY KEY,
    ggt_ss numeric(20, 2),
    ggt_sz numeric(20, 2),
    hgt numeric(20, 2),
    sgt numeric(20, 2),
    north_money numeric(20, 2),
    south_money numeric(20, 2)
);
