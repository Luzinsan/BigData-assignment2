// Path analysis: Campaign → Message → Client → Purchase <button class="citation-flag" data-index="10">
MATCH (c:Campaign {campaign_id: 57})<-[:BELONGS_TO]-(m:Message)-[:SENT_TO]->(cl:Client)<-[:OWNS]-(u:User)
MATCH (u)-[iw:INTERACTED_WITH {event_type: 'purchase'}]->(p:Product)
WHERE iw.event_time > m.sent_at
RETURN u.user_id, COUNT(DISTINCT p) AS purchased_products
ORDER BY purchased_products DESC;