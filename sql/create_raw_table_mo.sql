CREATE TABLE IF NOT EXISTS gemstone.raw_mo_daily
(
    load_dttm DateTime DEFAULT now(),
    load_date Date DEFAULT toDate(load_dttm),
    rc_name String,
    log_region String,
    product_code String,
    product_name String,
    mo_location String,
    expiry_mo Nullable(Date),
    qty_sg Nullable(Float64),
    pe_code Nullable(String),
    last_slot_replenish_dttm Nullable(DateTime),
    last_slot_zero_dttm Nullable(DateTime),
    total_qty Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, rc_name, product_code, mo_location)