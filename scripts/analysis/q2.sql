SELECT 
    e.product_card_pk,
    p.brand,
    COUNT(*) AS popularity_score
FROM e_commerce.events e
JOIN e_commerce.product_cards p ON e.product_card_pk = p.product_card_pk
JOIN e_commerce.friends f ON e.user_id = f.friend2
WHERE f.friend1 = 123
    AND e.event_type = 'purchase'
GROUP BY e.product_card_pk, p.brand
ORDER BY popularity_score DESC
LIMIT 10;