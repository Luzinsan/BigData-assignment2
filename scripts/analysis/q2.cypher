MATCH (u:User {user_id: 123})-[:FRIENDSHIP]-(friend:User)-[:INTERACTED_WITH]->(p:Product)
RETURN p.product_pk, p.brand, COUNT(*) AS friend_interest
ORDER BY friend_interest DESC
LIMIT 10;