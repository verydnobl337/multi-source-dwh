create table if not exists dds.dm_couriers (
	id bigint generated always as identity not null,
	courier_id varchar not null,
	courier_name varchar not null,
	constraint pk_dm_couriers primary key (id),
	constraint uq_courier_id unique (courier_id)
);