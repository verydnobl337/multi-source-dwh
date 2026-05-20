create table if not exists dds.fct_deliveries(
	id bigint generated always as identity not null,
	order_id varchar not null,
	courier_id varchar not null,
	delivery_id varchar not null,
	order_ts timestamp not null,
	delivery_ts timestamp not null,
	date int not null,
	tip_sum numeric(14, 2) not null default 0,
	constraint pk_fct_deliveries primary key (id),
	constraint uq_delivery unique (delivery_id)
);