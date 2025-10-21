-- DDL таблицы инкрементальных загрузок
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart CASCADE;
CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);


WITH
-- определяем, какие данные были изменены в витрине или добавлены в DWH, формируем дельту изменений
dwh_delta AS (
	SELECT
	    fo.customer_id AS customer_id,
		customer_name AS customer_name,
		customer_address AS customer_address,
		customer_birthday AS customer_birthday,
		customer_email AS customer_email,
		load_dttm AS customer_load_dttm
	FROM dwh.f_order fo
	JOIN dwh.d_craftsman dcraft ON fo.craftsman_id = dcraft.craftsman_id
	JOIN dwh.d_customer dcust ON fo.customer_id = dcust.customer_id
	JOIN dwh.d_product dprod ON fo.product_id = dprod.product_id
	LEFT JOIN dwh.customer_report_datamart dcrm ON fo.customer_id = dcrm.customer_id
		WHERE 
			(fo.load_dttm > (SELECT COALESCE(MAX(load_dttm, '1900-01-01')) FROM dwh.load_dates_customer_report_datamart)) OR
			(dcraft.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
			(dcust.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
			(dprod.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart))
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