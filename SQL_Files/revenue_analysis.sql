
WITH payment AS (
    SELECT
        date(date_trunc('month', payment_date)) AS payment_month,
        user_id,
        game_name,
        SUM(revenue_amount_usd) AS total_revenue
    FROM games_payments gp
    GROUP BY 1, 2, 3
),
previous_next_months AS (
    SELECT
        p.*,
        date(p.payment_month - INTERVAL '1' month) AS previous_calendar_month,
        date(p.payment_month + INTERVAL '1' month) AS next_calendar_month,
        LAG(p.total_revenue) OVER (PARTITION BY p.user_id ORDER BY p.payment_month) AS previous_paid_month_revenue,
        LAG(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month) AS previous_paid_month, -- previous_paid_month
        LEAD(p.payment_month) OVER (PARTITION BY p.user_id ORDER BY p.payment_month) AS next_paid_month -- next_paid_month
    FROM payment AS p
),
distinct_users AS (
    SELECT
        payment_month,
        COUNT(DISTINCT CASE WHEN total_revenue > 0 THEN user_id END) AS paying_users,
        COUNT(DISTINCT CASE WHEN previous_paid_month IS NULL AND payment_month IS NOT NULL THEN user_id END) AS new_paying_users
    FROM previous_next_months
    GROUP BY payment_month
),
calculations AS (
	SELECT
		pm.payment_month,
		pm.user_id,
		pm.game_name,
		pm.total_revenue,
		pm.previous_paid_month, -- previous_paid_month eklendi
		pm.next_paid_month, -- next_paid_month eklendi
		CASE WHEN pm.previous_paid_month IS NULL THEN pm.total_revenue END AS new_mrr,
		CASE WHEN pm.previous_paid_month = pm.previous_calendar_month AND pm.total_revenue > pm.previous_paid_month_revenue THEN pm.total_revenue - pm.previous_paid_month_revenue END AS expansion_revenue,
		CASE WHEN pm.previous_paid_month = pm.previous_calendar_month AND pm.total_revenue < pm.previous_paid_month_revenue THEN pm.total_revenue - pm.previous_paid_month_revenue END AS contraction_revenue,
		CASE WHEN pm.previous_paid_month != pm.previous_calendar_month AND pm.previous_paid_month IS NOT NULL THEN pm.total_revenue END AS back_from_churn_revenue, -- Geri dönen kullanıcılar
		CASE WHEN pm.next_paid_month IS NULL OR pm.next_paid_month != pm.next_calendar_month THEN pm.total_revenue END AS churned_revenue, -- Churn edilen kullanıcılar
		CASE WHEN pm.next_paid_month IS NULL OR pm.next_paid_month != pm.next_calendar_month THEN pm.next_calendar_month END AS churn_month, -- Churn ayı
        du.paying_users, -- Ücretli kullanıcılar
        du.new_paying_users, -- Yeni ücretli kullanıcılar
        (SUM(pm.total_revenue) OVER (PARTITION BY pm.payment_month)) / du.paying_users AS arppu, -- ARPPU
        -- Churn hesaplaması: Churn olmuş kullanıcılar
        CASE 
            WHEN NOT EXISTS (
                SELECT 1 
                FROM payment p2 
                WHERE p2.user_id = pm.user_id 
                AND p2.payment_month = pm.payment_month + INTERVAL '1' MONTH -- Bir sonraki ayda ödeme yapılmamışsa churn oldu
            ) 
            THEN 'Churned'
            ELSE 'Active'
        END AS churn_status -- Churned veya aktif kullanıcılar
	FROM previous_next_months pm
    JOIN distinct_users du ON pm.payment_month = du.payment_month
)
SELECT 
    c.*,
    gu.language,
    gu.has_older_device_model,
    gu.age
FROM calculations c
LEFT JOIN games_paid_users gu ON c.user_id = gu.user_id AND c.game_name = gu.game_name;
