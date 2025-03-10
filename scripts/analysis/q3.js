db.products.createIndex({ category_code: "text" });
db.getCollection('products').find(
    { $text: { $search: "electronics.smartphone" } },
    { score: { $meta: "textScore" } }
).sort({ score: { $meta: "textScore" } });