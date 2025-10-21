SELECT * 
FROM (
	SELECT 
		T1.customer_id AS customer_id,
		COUNT(T1.customer_id) AS count
	FROM(
		select 
			fo.customer_id AS customer_id,
			dcust.customer_name AS customer_name,
			dcust.customer_address AS customer_address,
			dcust.customer_birthday AS customer_birthday,
			dcust.customer_email AS customer_email,
			dprod.load_dttm AS customer_load_dttm,
			dcrm.customer_id AS exist_customer_id
		from dwh.f_order fo 
			JOIN dwh.d_craftsman dcraft ON fo.craftsman_id = dcraft.craftsman_id
			JOIN dwh.d_customer dcust ON fo.customer_id = dcust.customer_id
			JOIN dwh.d_product dprod ON fo.product_id = dprod.product_id
			LEFT JOIN dwh.customer_report_datamart dcrm ON fo.customer_id = dcrm.customer_id
		ORDER BY fo.customer_id, dcust.customer_name
	) AS T1
	GROUP BY customer_id
) AS T2
WHERE count > 1;


CREATE TEMP TABLE temp AS (
	SELECT 
		fo.customer_id AS customer_id,
		dcrm.customer_id AS exist_customer_id,
		dcust.customer_name AS customer_name,
		dcust.customer_address AS customer_address,
		dcust.customer_birthday AS customer_birthday,
		dcust.customer_email AS customer_email,
		dprod.product_price AS product_price,
		dprod.product_type AS product_type,
		dprod.load_dttm AS customer_load_dttm,
		dcraft.craftsman_id AS craftsman_id
	FROM dwh.f_order fo 
		JOIN dwh.d_craftsman dcraft ON fo.craftsman_id = dcraft.craftsman_id
		JOIN dwh.d_customer dcust ON fo.customer_id = dcust.customer_id
		JOIN dwh.d_product dprod ON fo.product_id = dprod.product_id
		LEFT JOIN dwh.customer_report_datamart dcrm ON fo.customer_id = dcrm.customer_id
	ORDER BY fo.customer_id, dcust.customer_name
);

SELECT customer_id, COUNT(craftsman_id)
FROM temp
GROUP BY customer_id
ORDER BY COUNT(craftsman_id) DESC;


SELECT 
	T3.customer_id,
	T3.craftsman_id,
	DENSE_RANK() OVER (ORDER BY T3.craftsman_count DESC) AS rank_count_craftsman
FROM (
	SELECT 
		dd.customer_id,
		dd.craftsman_id,
		COUNT(dd.craftsman_id) AS craftsman_count
	FROM dwh_delta AS dd
	GROUP BY dd.customer_id, dd.craftsman_id
	ORDER BY craftsman_count DESC
) AS T3;


-- расчет по большинству количеству столбцов
SELECT 
	T5.customer_id
FROM ((
    SELECT 
        T1.customer_id AS customer_id,
        T1.customer_name AS customer_name,
        T1.customer_address AS customer_address,
        T1.customer_birthday AS customer_birthday,
        T1.customer_email AS customer_email,
        SUM(T1.product_price) AS customer_costs,
        SUM(T1.product_price)*0.1 AS platform_money,
        COUNT(order_id) AS total_order_count,
        AVG(T1.product_price) AS avg_order_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY T1.diff_order_date) AS median_time_order_completed,
        1 AS top_product_category,
        1 AS top_craftsman_category,
        SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
        SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
        SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
        SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
        SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
        T1.report_period AS report_period
        
    FROM dwh_delta AS T1
    WHERE T1.exist_customer_id IS NULL
    GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
) AS T2
JOIN (
    SELECT 
        T3.customer_id AS customer_id_T4,
        T3.craftsman_id,
        DENSE_RANK() OVER (PARTITION BY T3.customer_id ORDER BY T3.craftsman_count DESC) AS rank_count_craftsman
    FROM (
        SELECT 
            dd.customer_id,
            dd.craftsman_id,
            COUNT(dd.craftsman_id) AS craftsman_count
        FROM dwh_delta AS dd
        GROUP BY dd.customer_id, dd.craftsman_id
    ) AS T3
) AS T4 ON T2.customer_id = T4.customer_id_T4 AND T4.rank_count_craftsman = 1
) AS T5 ORDER BY report_period










