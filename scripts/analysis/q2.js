db.getCollection('events').aggregate([
    {
        $match: {
            user_id: 123,
            event_type: 'view',
            user_session: "abc-123"
        }
    },
    {
        $lookup: {
            from: "products",
            localField: "product_pk",
            foreignField: "product_pk",
            as: "product_details"
        }
    },
    { $unwind: "$product_details" },
    {
        $group: {
            _id: "$product_details.product_id",
            brand: { $first: "$product_details.brand" },
            views: { $sum: 1 }
        }
    },
    { $sort: { views: -1 } }
]);