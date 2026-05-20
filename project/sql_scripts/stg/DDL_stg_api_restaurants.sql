create table if not exists stg.api_restaurants(
	id serial primary key not null,
	object_id varchar not null,
	object_value text not null, 
	update_ts timestamp default now(),
	constraint uq_api_restaurants_object_id unique (object_id)
);