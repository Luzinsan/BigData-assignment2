db = db.getSiblingDB("ecommerce");
db.dropDatabase()

db.createCollection("events", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "events",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "event_time": {
                    "bsonType": "string"
                },
                "event_type": {
                    "bsonType": "string"
                },
                "product_pk": {
                    "bsonType": "objectId"
                },
                "user_id": {
                    "bsonType": "int"
                },
                "user_session": {
                    "bsonType": "string"
                },
                "price": {
                    "bsonType": "double"
                }
            },
            "additionalProperties": false,
            "required": [
                "event_time",
                "event_type",
                "product_pk",
                "user_id",
                "user_session",
                "price"
            ]
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});

db.events.createIndex({
    "event_time": 1,
    "product_pk": 1,
    "user_id": 1
},
{
    "name": "unique_event",
    "unique": true
});


db.createCollection("campaigns", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "campaigns",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "id": {
                    "bsonType": "long"
                },
                "campaign_type": {
                    "bsonType": "string"
                },
                "channel": {
                    "bsonType": "string"
                },
                "topic": {
                    "bsonType": "string"
                },
                "bulk_details": {
                    "bsonType": "object",
                    "properties": {
                        "started_at": {
                            "bsonType": "string"
                        },
                        "finished_at": {
                            "bsonType": "string"
                        },
                        "total_count": {
                            "bsonType": "int"
                        },
                        "warmup_mode": {
                            "bsonType": "bool"
                        },
                        "hour_limit": {
                            "bsonType": "int"
                        },
                        "ab_test": {
                            "bsonType": "bool"
                        }
                    },
                    "additionalProperties": false
                },
                "trigger_details": {
                    "bsonType": "object",
                    "properties": {
                        "position": {
                            "bsonType": "int"
                        }
                    },
                    "additionalProperties": false
                },
                "subject_details": {
                    "bsonType": "object",
                    "properties": {
                        "subject_length": {
                            "bsonType": "int"
                        },
                        "subject_with_personalization": {
                            "bsonType": "bool"
                        },
                        "subject_with_deadline": {
                            "bsonType": "bool"
                        },
                        "subject_with_emoji": {
                            "bsonType": "bool"
                        },
                        "subject_with_bonuses": {
                            "bsonType": "bool"
                        },
                        "subject_with_discount": {
                            "bsonType": "bool"
                        },
                        "subject_with_saleout": {
                            "bsonType": "bool"
                        }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false,
            "required": [
                "id",
                "campaign_type",
                "channel"
            ]
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});

db.campaigns.createIndex({
    "id": 1,
    "campaign_type": 1
},
{
    "name": "unique_campaign",
    "unique": true
});


db.createCollection("products", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "products",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "product_pk": {
                    "bsonType": "objectId"
                },
                "product_id": {
                    "bsonType": "int"
                },
                "brand": {
                    "bsonType": "string"
                },
                "category_id": {
                    "bsonType": "long"
                },
                "category_code": {
                    "bsonType": "string"
                }
            },
            "additionalProperties": false,
            "required": [
                "product_pk",
                "product_id",
                "category_id"
            ]
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});

db.products.createIndex({
    "product_id": 1,
    "brand": 1,
    "category_id": 1
},
{
    "name": "unique_product",
    "unique": true
});


db.createCollection("users", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "users",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "user_id": {
                    "bsonType": "int"
                },
                "devices": {
                    "bsonType": "array",
                    "additionalItems": true,
                    "items": {
                        "bsonType": "object",
                        "properties": {
                            "client_id": {
                                "bsonType": "long"
                            },
                            "client_device_id": {
                                "bsonType": "int"
                            },
                            "first_purchase_date": {
                                "bsonType": "string"
                            }
                        },
                        "additionalProperties": false,
                        "required": [
                            "client_id",
                            "client_device_id"
                        ]
                    }
                }
            },
            "additionalProperties": false,
            "required": [
                "user_id"
            ]
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});

db.users.createIndex({
    "user_id": 1
},
{
    "name": "user",
    "unique": true
});

db.users.createIndex({
    "devices.client_id": 1,
    "devices.client_device_id": 1
},
{
    "name": "client",
    "unique": true
});


db.createCollection("messages", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "messages",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "message_id": {
                    "bsonType": "string"
                },
                "campaign_id": {
                    "bsonType": "long"
                },
                "message_type": {
                    "bsonType": "string"
                },
                "client_id": {
                    "bsonType": "long"
                },
                "channel": {
                    "bsonType": "string"
                },
                "platform": {
                    "bsonType": "string"
                },
                "email_provider": {
                    "bsonType": "string"
                },
                "sent_at": {
                    "bsonType": "string"
                },
                "created_at": {
                    "bsonType": "string"
                },
                "updated_at": {
                    "bsonType": "string"
                },
                "behaviors": {
                    "bsonType": "array",
                    "additionalItems": true,
                    "items": {
                        "bsonType": "object",
                        "properties": {
                            "behavior_type": {
                                "bsonType": "string"
                            },
                            "first_time_at": {
                                "bsonType": "string"
                            },
                            "last_time_at": {
                                "bsonType": "string"
                            }
                        },
                        "additionalProperties": false
                    }
                }
            },
            "additionalProperties": false,
            "required": [
                "message_id",
                "campaign_id",
                "message_type",
                "client_id",
                "channel",
                "sent_at",
                "created_at",
                "updated_at"
            ]
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});

db.messages.createIndex({
    "message_id": 1
},
{
    "name": "message",
    "unique": true
});




db.createCollection("friends", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "friends",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "friend1": {
                    "bsonType": "int"
                },
                "friend2": {
                    "bsonType": "int"
                }
            },
            "additionalProperties": false,
            "required": [
                "friend1",
                "friend2"
            ]
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});

db.friends.createIndex({
    "friend1": 1,
    "friend2": 1
},
{
    "name": "friendship",
    "unique": true
});