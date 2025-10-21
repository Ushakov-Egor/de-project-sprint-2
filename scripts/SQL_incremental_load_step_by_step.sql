/* ЧЕРНОВОЙ ФАЙЛ ДЛЯ ИНКРЕМЕНТАЛЬНОЙ ЗАГРУЗКИ */
/* ЗДЕСЬ ОТДЕЛЬНО ПРОПИСАНЫ ЭТАПЫ CTE */

-- DDL таблицы инкрементальных загрузок
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart CASCADE;
CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);

-- определяем, какие данные были изменены в витрине или добавлены в DWH, формируем дельту изменений
DROP TABLE IF EXISTS dwh_delta CASCADE;
CREATE TABLE IF NOT EXISTS dwh_delta AS (
	SELECT
	    fo.customer_id AS customer_id,
		dcust.customer_name AS customer_name,
		dcust.customer_address AS customer_address,
		dcust.customer_birthday AS customer_birthday,
		dcust.customer_email AS customer_email,
		dprod.load_dttm AS customer_load_dttm
	FROM dwh.f_order fo
	JOIN dwh.d_craftsman dcraft ON fo.craftsman_id = dcraft.craftsman_id
	JOIN dwh.d_customer dcust ON fo.customer_id = dcust.customer_id
	JOIN dwh.d_product dprod ON fo.product_id = dprod.product_id
	LEFT JOIN dwh.customer_report_datamart dcrm ON fo.customer_id = dcrm.customer_id
		WHERE 
			(fo.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
			(dcraft.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
			(dcust.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
			(dprod.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart))
);





