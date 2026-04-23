CREATE TABLE IF NOT EXISTS gemstone.raw_mx_daily
(
    load_dttm DateTime DEFAULT now(),
    load_date Date DEFAULT toDate(load_dttm),

    rc_code String,
    rc_name String,
    is_active Nullable(UInt8),
    report_dttm_bd_rc Nullable(DateTime),
    pallet_number String,
    pallet_type String,
    sscc_center_pe Nullable(String),
    stock_type Nullable(String),
    receipt_no Nullable(String),
    cfs Nullable(String),

    product_code String,
    product_name Nullable(String),
    mo_location Nullable(String),
    mp_location Nullable(String),

    qty Nullable(Float64),
    avg_box_weight Nullable(Float64),
    expiry_date Nullable(Date),
    production_date Nullable(Date),

    manufacturer Nullable(String),
    country Nullable(String),

    shelf_life_kt Nullable(Float64),
    shelf_life_entered Nullable(Float64),
    remaining_shelf_life Nullable(Float64),
    removal_from_selection Nullable(Float64),
    deviation_diff Nullable(Float64),

    deviation_comment Nullable(String),
    rc_needs Nullable(String),
    goods_hardness Nullable(Float64),
    pallet_height Nullable(Float64),
    pallet_base_type Nullable(String),

    install_dttm Nullable(DateTime),
    install_user Nullable(String),

    sample_qty Nullable(Float64),
    defect_sample_qty Nullable(Float64),
    defect_percent Nullable(Float64),

    defect_install_dttm Nullable(DateTime),
    defect_install_user Nullable(String),

    pct_barcodes_without_deviation Nullable(Float64),
    pct_barcodes_with_deviation Nullable(Float64),
    pct_unscanned_barcodes Nullable(Float64),

    note Nullable(String),
    gross_weight Nullable(Float64),
    receiving_task_no Nullable(String),
    created_manually Nullable(String),
    priority_flag Nullable(String),
    rework_flag Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, rc_code, pallet_number, product_code);