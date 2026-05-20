# api_entities.md

## 1. Поля витрины выплат курьерам (CDM)

Для расчёта витрины необходимы следующие поля:

- id — идентификатор записи
- courier_id — идентификатор курьера
- courier_name — ФИО курьера
- settlement_year — год расчёта
- settlement_month — месяц расчёта
- orders_count — количество доставленных заказов за период
- orders_total_sum — общая сумма заказов
- rate_avg — средний рейтинг курьера за период
- order_processing_fee — комиссия компании (orders_total_sum * 0.25)
- courier_order_sum — начисления курьеру за заказы (зависит от рейтинга)
- courier_tips_sum — сумма чаевых
- courier_reward_sum — итоговая выплата курьеру  
  (courier_order_sum + courier_tips_sum * 0.95)

---

## 2. Таблицы DDS, используемые для построения витрины

### Уже существующие таблицы

#### dds.dm_orders
Используется для связи заказов с курьером и датой.

Поля:
- id
- user_id
- restaurant_id
- timestamp_id
- order_key
- order_status

---

#### dds.dm_timestamps
Используется для агрегации по периоду (месяц).

Поля:
- id
- ts
- year
- month
- day
- time
- date

---

#### dds.dm_restaurants
Используется как справочник ресторанов.

Поля:
- id
- restaurant_id
- restaurant_name

---

#### dds.fct_product_sales
Используется как источник финансовых показателей заказов.

Поля:
- order_id
- total_sum
- bonus_payment
- bonus_grant

---

### Недостающие таблицы (будут созданы)

#### dds.dm_couriers
Справочник курьеров (из API).

Поля:
- id
- courier_id
- courier_name

Источник: API /couriers

---

#### dds.fct_deliveries
Фактовая таблица доставок (из API).

Поля:
- order_id
- courier_id
- delivery_id
- order_ts
- delivery_ts
- rate
- tip_sum

Источник: API /deliveries

---

## 3. Сущности и данные из API

### 3.1 couriers

Источник:
GET /couriers

Используется для заполнения:
- dds.dm_couriers

Необходимые поля:
- _id → courier_id
- name → courier_name

---

### 3.2 deliveries

Источник:
GET /deliveries

Используется для заполнения:
- dds.fct_deliveries

Необходимые поля:
- order_id
- courier_id
- delivery_id
- order_ts
- delivery_ts
- rate
- tip_sum

---

## 4. Примечания по моделированию

- Факт расчётов строится по дате заказа (order_ts), а не доставки
- Данные о суммах заказов берутся из DDS слоя заказов и продаж
- API используется только для курьеров и доставок
- Все агрегации выполняются в CDM слое