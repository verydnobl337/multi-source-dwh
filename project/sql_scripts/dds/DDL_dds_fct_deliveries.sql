CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id bigint generated always as identity not null,
    order_id varchar not null,
    courier_id varchar not null,
    delivery_id varchar not null,
    order_ts timestamp not null,
    delivery_ts timestamp not null,
    rate numeric(3, 2) not null,
    tip_sum numeric(14, 2) not null default 0,

    CONSTRAINT pk_fct_deliveries PRIMARY KEY (id),
    CONSTRAINT uq_delivery UNIQUE (delivery_id),
    CONSTRAINT chk_delivery_rate CHECK (rate >= 0 AND rate <= 5),
    CONSTRAINT chk_delivery_tip_sum CHECK (tip_sum >= 0)
);
