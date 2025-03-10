SELECT product_id, brand, category_code
FROM e_commerce.products
WHERE to_tsvector(category_code) @@ to_tsquery('electronics.smartphone');