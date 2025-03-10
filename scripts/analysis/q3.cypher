CALL db.index.fulltext.queryNodes("categoryIndex", "electronics.smartphone") YIELD node, score
RETURN node.product_pk, node.brand, score
ORDER BY score DESC;