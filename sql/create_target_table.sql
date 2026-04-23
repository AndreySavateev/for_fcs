CREATE TABLE IF NOT EXISTS gemstone.dtl_selection_errors_weekly
(
    report_date Date,
    report_week String,
    product_code String,
    product_name String,
    rc_name String,
    expiry_mo Date,
    expiry_mx Date,
    qty_mo Float64,
    qty_mx Float64,
    inserted_at DateTime DEFAULT now()
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_week, rc_name, product_code)
