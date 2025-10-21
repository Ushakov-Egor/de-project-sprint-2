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
		dprod.load_dttm AS customer_load_dttm,
		dcrm.customer_id AS exist_customer_id
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


/* делаем выборку заказчкиов, по которым были изменения в DWH. 
 * По этим заказчикам данные в витрине нужно обновить */
DROP TABLE IF EXISTS dwh_update_delta CASCADE;
CREATE TABLE IF NOT EXISTS dwh_update_delta AS (
	SELECT customer_id AS customer_id
	FROM dwh_delta dd
		WHERE dd.exist_customer_id IS NOT NULL
);

/* Делаем расчёт витрины по новым данным. 
 * Этой информации по заказчикам в рамках расчётного периода раньше не было, это новые данные. 
 * Их можно просто вставить (insert) в витрину без обновления */
DROP TABLE IF EXISTS dwh_delta_insert_result CASCADE;
CREATE TABLE IF NOT EXISTS dwh_delta_insert_result AS (
	SELECT
		customer_id AS customer_id,
		customer_name AS customer_name,
		customer_address AS customer_address,
		customer_birthday AS customer_birthday,
		customer_email AS customer_email,
		customer_costs AS customer_costs,
		platform_money AS platform_money,
		total_order_count AS total_order_count,
		avg_order_price AS avg_order_price,
		median_time_order_completed AS median_time_order_completed,
		top_product_category AS top_product_category,
		top_craftsman_id AS top_craftsman_id,
		count_order_created_per_month AS count_order_created_per_month,
		count_order_in_progress AS count_order_in_progress,
		count_order_delivery AS count_order_delivery,
		count_order_done AS count_order_done,
		count_order_not_done AS count_order_not_done,
		report_period AS report_period
	FROM (
		/* В этой выборке объединяем две внутренние выборки по расчёту столбцов витрины 
		 * и применяем оконную функцию для определения самой популярного мастера у заказчика */
		SELECT * 
		FROM (
			/* В этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, 
			 * кроме столбца с самой популярной категорией товаров у мастера. 
			 * Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN */
			SELECT *
			FROM dwh_delta AS T1
				WHERE T1.exist_customer_id IS NULL
				GROUP BY 
						T1.customer_id, 
						T1.customer_name, 
						T1.customer_address, 
						T1.customer_birthday, 
						T1.customer_email, 
						T1.report_period
		) AS T2
			JOIN (
				SELECT 
					dd.customer_id, 
                    dd.craftsman_id, 
                    COUNT(dd.craftsman_id) AS count_craftsman
				FROM dwh_delta AS dd
					GROUP BY dd.customer_id, dd.craftsman_id
					ORDER BY count_craftsman DESC) AS T3 ON T2.customer_id = T3.customer_id
						
			)
	)
		
		
);



