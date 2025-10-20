-- DDL таблицы инкрементальных загрузок
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_craftsman_report_datamart_pk PRIMARY KEY (id)
);


WITH
-- определяем, какие данные были изменены в витрине или добавлены в DWH, формируем дельту изменений
dwh_delta AS (
    customer_id AS customer_id,
	customer_name AS customer_name,
	customer_address AS customer_address,
	customer_birthday AS customer_birthday,
	customer_email AS customer_email,
	 AS customer_costs,
	platform_money AS 
	total_order_count
	avg_order_price
	median_time_order_completed
	top_product_category
	top_craftsman_id
	count_order_created_per_month
	count_order_in_progress
	count_order_delivery
	count_order_done
	count_order_not_done
	report_period
),
dwh_update_delta AS (
    -- Шаг 3
),
dwh_delta_insert_result AS (
    -- Шаг 4
),
dwh_delta_update_result AS (
    -- Шаг 5
),
insert_delta AS (
    -- Шаг 6
),
update_delta AS (
    -- Шаг 7
),
insert_load_date AS (
    -- Шаг 8
)
SELECT 'increment datamart';