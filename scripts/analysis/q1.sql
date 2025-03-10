WITH CampaignInteractions AS (
    SELECT DISTINCT
        ms.client_id,
        m.campaign_id,
		c.campaign_type,
        mb.happened_first_time AS interaction_time, 
        mb.type AS interaction_type 
    FROM e_commerce.message_sent ms
    JOIN e_commerce.messages m ON ms.id = m.id
    JOIN e_commerce.message_behaviors mb ON ms.message_id = mb.message_id
    JOIN e_commerce.campaigns c ON m.campaign_id = c.id and m.message_type = c.campaign_type 
    LEFT JOIN e_commerce.campaign_bulks cb ON c.campaign_pk = cb.campaign_pk
    WHERE mb.happened_first_time BETWEEN ms.sent_at AND (ms.sent_at + INTERVAL '30 days') 
		AND (c.campaign_type <> 'bulk' OR
			  mb.happened_first_time BETWEEN cb.started_at AND COALESCE(cb.finished_at, NOW()))
),
PurchasingUsers AS (
    SELECT DISTINCT
        SUBSTRING(client_id::varchar(19), 10, 9)::int as user_id,
        campaign_id,
        campaign_type
    FROM CampaignInteractions
	WHERE interaction_type = 'purchased'
),
FriendPurchases AS (
    SELECT DISTINCT
        f.friend2 AS friend_user_id,
        pu.campaign_id,
        pu.campaign_type
    FROM PurchasingUsers pu
    JOIN e_commerce.friends f ON pu.user_id = f.friend1  
    JOIN PurchasingUsers pu2 ON f.friend2 = pu2.user_id AND pu.campaign_id = pu2.campaign_id  
)

SELECT
    c.id AS campaign_id,
    c.campaign_type,
    COUNT(DISTINCT ms.client_id) AS total_messages,
    COUNT(DISTINCT CASE WHEN ci.interaction_type IN ('clicked', 'opened') THEN ci.client_id END) AS clients_with_interaction,
    COUNT(DISTINCT pu.user_id) AS users_purchased,
    COUNT(DISTINCT fp.friend_user_id) AS friends_who_also_purchased,
	CASE
        WHEN COUNT(DISTINCT ms.client_id) > 0 THEN (COUNT(DISTINCT pu.user_id) * 100.0) / COUNT(DISTINCT ms.client_id)
        ELSE 0
    END AS conversion_rate
FROM e_commerce.campaigns c
JOIN e_commerce.messages m ON c.id = m.campaign_id AND c.campaign_type = m.message_type
JOIN e_commerce.message_sent ms ON m.id = ms.id
LEFT JOIN CampaignInteractions ci ON ms.client_id = ci.client_id AND m.campaign_id = ci.campaign_id
LEFT JOIN PurchasingUsers pu ON SUBSTRING(ms.client_id::varchar(19), 10, 9)::int = pu.user_id AND m.campaign_id = pu.campaign_id
LEFT JOIN FriendPurchases fp ON pu.user_id = fp.friend_user_id AND pu.campaign_id = fp.campaign_id
GROUP BY c.id, c.campaign_type
ORDER BY conversion_rate  DESC
LIMIT 10;