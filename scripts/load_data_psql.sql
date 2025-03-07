CREATE SCHEMA IF NOT EXISTS e_commerce;
SET search_path TO e_commerce, public;

CREATE TABLE IF NOT EXISTS e_commerce.products (
	product_pk serial PRIMARY KEY NOT NULL,
	product_id integer NOT NULL,
	category_id bigint NOT NULL,
	category_code varchar,
	CONSTRAINT unique_product UNIQUE (product_id, category_id)
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.product_cards (
	product_card_pk serial PRIMARY KEY NOT NULL,
	product_pk integer NOT NULL,
	brand varchar,
	UNIQUE (brand, product_pk),
	CONSTRAINT fk_products_product_pk_to_product_cards_product_pk FOREIGN KEY (product_pk) REFERENCES e_commerce.products (product_pk) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.users (
	user_id serial PRIMARY KEY NOT NULL
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.events (
	event_id serial PRIMARY KEY NOT NULL,
	product_card_pk integer NOT NULL,
	user_id integer NOT NULL,
	event_time timestamp WITH TIME ZONE NOT NULL,
	event_type varchar NOT NULL,
	user_session uuid NOT NULL,
	price real NOT NULL,
	CONSTRAINT unique_event UNIQUE (user_id, event_time, product_card_pk),
	CONSTRAINT fk_products_product_id_to_events_product_id FOREIGN KEY (product_card_pk) REFERENCES e_commerce.product_cards (product_card_pk) ON DELETE CASCADE,
	CONSTRAINT fk_user_user_id_to_events_user_id FOREIGN KEY (user_id) REFERENCES e_commerce.users (user_id) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.campaigns (
	campaign_pk bigserial PRIMARY KEY NOT NULL,
	id bigint NOT NULL,
	campaign_type varchar(52) NOT NULL,
	channel varchar(48) NOT NULL,
	topic varchar,
	CONSTRAINT unique_campaign UNIQUE (id, campaign_type),
	CONSTRAINT chk_campaign_type CHECK (campaign_type IN ('bulk', 'trigger', 'transactional'))
) TABLESPACE pg_default;


COMMENT ON COLUMN e_commerce.campaigns.id IS E'Campaign ID';

CREATE TABLE IF NOT EXISTS e_commerce.messages (
	id bigserial PRIMARY KEY NOT NULL,
	campaign_id bigint NOT NULL,
	message_type varchar(52) NOT NULL,
	CONSTRAINT unique_message UNIQUE (campaign_id, message_type),
	CONSTRAINT fk_campaigns_id_to_messages_campaign_id FOREIGN KEY (campaign_id, message_type) REFERENCES e_commerce.campaigns (id, campaign_type) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.clients (
	client_id bigserial PRIMARY KEY NOT NULL,
	first_purchase_date date
) TABLESPACE pg_default;


CREATE TABLE IF NOT EXISTS e_commerce.friends (
	friend1 integer NOT NULL,
	friend2 integer NOT NULL,
	CONSTRAINT friendship PRIMARY KEY (friend1, friend2),
	CONSTRAINT fk_user_user_id_to_friends_friend1 FOREIGN KEY (friend1) REFERENCES e_commerce.users (user_id) ON DELETE CASCADE,
	CONSTRAINT fk_user_user_id_to_friends_friend2 FOREIGN KEY (friend2) REFERENCES e_commerce.users (user_id) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.campaign_bulks (
	campaign_pk serial PRIMARY KEY NOT NULL,
	started_at timestamp NOT NULL,
	finished_at timestamp,
	total_count integer NOT NULL,
	warmup_mode boolean NOT NULL,
	hour_limit integer,
	ab_test boolean NOT NULL,
	CONSTRAINT fk_campaigns_compaign_pk_to_compaign_email_compaign_pk FOREIGN KEY (campaign_pk) REFERENCES e_commerce.campaigns (campaign_pk) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.campaign_subjects (
	campaign_pk serial PRIMARY KEY NOT NULL,
	subject_length smallint NOT NULL,
	subject_with_personalization boolean NOT NULL,
	subject_with_deadline boolean NOT NULL,
	subject_with_emoji boolean NOT NULL,
	subject_with_bonuses boolean NOT NULL,
	subject_with_discount boolean NOT NULL,
	subject_with_saleout boolean NOT NULL,
	CONSTRAINT fk_campaigns_campaign_pk_to_campaign_subject_campaign_pk FOREIGN KEY (campaign_pk) REFERENCES e_commerce.campaigns (campaign_pk) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.campaign_triggers (
	campaign_pk serial PRIMARY KEY NOT NULL,
	position smallint DEFAULT 1 NOT NULL,
	CONSTRAINT fk_campaigns_campaign_pk_to_campaign_trigger_campaign_pk FOREIGN KEY (campaign_pk) REFERENCES e_commerce.campaigns (campaign_pk) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.message_sent (
	message_id uuid PRIMARY KEY NOT NULL,
	id bigint NOT NULL,
	client_id bigint NOT NULL,
	email_provider varchar,
	platform varchar,
	channel varchar(48) NOT NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	sent_at timestamp NOT NULL,
	CONSTRAINT fk_client_client_id_to_message_send_client_id FOREIGN KEY (client_id) REFERENCES e_commerce.clients (client_id) ON DELETE CASCADE,
	CONSTRAINT fk_messages_id_to_message_sent_id FOREIGN KEY (id) REFERENCES e_commerce.messages (id) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.message_behaviors (
	message_id uuid NOT NULL,
	type varchar NOT NULL,
	happened_first_time timestamp NOT NULL,
	happened_last_time timestamp,
	CONSTRAINT unique_behavior PRIMARY KEY (message_id, type),
	CONSTRAINT fk_message_sent_message_id_to_message_behavior_message_id FOREIGN KEY (message_id) REFERENCES e_commerce.message_sent (message_id) ON DELETE CASCADE
) TABLESPACE pg_default;


\COPY users(user_id)  FROM 'output/psql/users.csv'  DELIMITER ','  CSV HEADER;
\COPY clients(client_id,first_purchase_date)  FROM 'output/psql/clients.csv'  DELIMITER ','  CSV HEADER;
\COPY friends(friend1, friend2)  FROM 'output/psql/friends.csv'  DELIMITER ','  CSV HEADER;
\COPY products(product_pk,product_id,category_id,category_code)  FROM 'output/psql/products.csv'  DELIMITER ','  CSV HEADER;
\COPY product_cards(product_card_pk,product_pk,brand)  FROM 'output/psql/product_cards.csv'  DELIMITER ','  CSV HEADER;
\COPY events(product_card_pk,user_id,event_time,event_type,user_session,price)  FROM 'output/psql/events.csv'  DELIMITER ','  CSV HEADER;
\COPY campaigns(campaign_pk, id, campaign_type, channel, topic)  FROM 'output/psql/campaigns.csv'  DELIMITER ','  CSV HEADER;
\COPY campaign_bulks(campaign_pk,started_at,finished_at,total_count,warmup_mode,hour_limit,ab_test)  FROM 'output/psql/campaign_bulks.csv'  DELIMITER ','  CSV HEADER;
\COPY campaign_subjects(campaign_pk,subject_length,subject_with_personalization,subject_with_deadline,subject_with_emoji,subject_with_bonuses,subject_with_discount,subject_with_saleout)  FROM 'output/psql/campaign_subjects.csv'  DELIMITER ','  CSV HEADER;
\COPY campaign_triggers(campaign_pk, position)  FROM 'output/psql/campaign_triggers.csv'  DELIMITER ','  CSV HEADER;
\COPY messages(id,campaign_id,message_type)  FROM 'output/psql/messages.csv'  DELIMITER ','  CSV HEADER;
\COPY message_sent(message_id,id,client_id,email_provider,platform,channel,created_at,updated_at,sent_at)  FROM 'output/psql/message_sent.csv'  DELIMITER ','  CSV HEADER;
\COPY message_behaviors(message_id,type,happened_first_time,happened_last_time)  FROM 'output/psql/message_behavior.csv'  DELIMITER ','  CSV HEADER;