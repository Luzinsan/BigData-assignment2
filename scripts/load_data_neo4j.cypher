:begin
// 1. Node Constraints
CREATE CONSTRAINT unique_message IF NOT EXISTS 
FOR (n:message) REQUIRE (n.message_id) IS NODE KEY;

CREATE CONSTRAINT unique_user IF NOT EXISTS 
FOR (n:user) REQUIRE (n.user_id) IS NODE KEY;

CREATE CONSTRAINT unique_client IF NOT EXISTS 
FOR (n:client) REQUIRE (n.client_id) IS NODE KEY;

CREATE CONSTRAINT unique_campaign IF NOT EXISTS 
FOR (n:campaign) REQUIRE (n.campaign_pk) IS NODE KEY;

CREATE CONSTRAINT unique_product IF NOT EXISTS 
FOR (n:product) REQUIRE (n.product_pk) IS NODE KEY;


// Required constraints
CREATE CONSTRAINT message_campaign_id_not_null IF NOT EXISTS 
FOR (n:message) REQUIRE n.campaign_id IS NOT NULL;
CREATE CONSTRAINT message_type_not_null IF NOT EXISTS 
FOR (n:message) REQUIRE n.message_type IS NOT NULL;
CREATE CONSTRAINT message_channel_not_null IF NOT EXISTS 
FOR (n:message) REQUIRE n.channel IS NOT NULL;

CREATE CONSTRAINT campaign_campaign_id_not_null IF NOT EXISTS 
FOR (n:campaign) REQUIRE n.campaign_id IS NOT NULL;
CREATE CONSTRAINT campaign_type_not_null IF NOT EXISTS 
FOR (n:campaign) REQUIRE n.campaign_type IS NOT NULL;
CREATE CONSTRAINT campaign_channel_not_null IF NOT EXISTS 
FOR (n:campaign) REQUIRE n.channel IS NOT NULL;

CREATE CONSTRAINT product_product_id_not_null IF NOT EXISTS 
FOR (n:product) REQUIRE n.product_id IS NOT NULL;
CREATE CONSTRAINT product_brand_not_null IF NOT EXISTS 
FOR (n:product) REQUIRE n.brand IS NOT NULL;
CREATE CONSTRAINT product_category_id_not_null IF NOT EXISTS 
FOR (n:product) REQUIRE n.category_id IS NOT NULL;

// Индексы
CREATE INDEX message_campaign_id_index IF NOT EXISTS 
FOR (n:message) ON (n.campaign_id, n.message_type, n.channel);

CREATE INDEX campaign_campaign_id_index IF NOT EXISTS 
FOR (n:campaign) ON (n.campaign_id, n.compaign_type, n.channel);

CREATE INDEX product_category_index IF NOT EXISTS 
FOR (n:product) ON (n.product_id, n.brand, n.category_id);
:commit