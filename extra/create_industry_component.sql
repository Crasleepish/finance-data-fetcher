CREATE TABLE IF NOT EXISTS industry_component (
    stock_code          VARCHAR(10) NOT NULL,
    standard            VARCHAR(10) NOT NULL,
    begin_date          DATE        NOT NULL,
    end_date            DATE        NOT NULL,
    industry_level1_code VARCHAR(10),
    industry_level1_name VARCHAR(20),
    industry_level2_code VARCHAR(10),
    industry_level2_name VARCHAR(20),
    industry_level3_code VARCHAR(10),
    industry_level3_name VARCHAR(30),
    PRIMARY KEY (stock_code, standard, begin_date)
);

CREATE INDEX IF NOT EXISTS industry_component_stock_code_standard_idx
    ON industry_component (stock_code, standard);
