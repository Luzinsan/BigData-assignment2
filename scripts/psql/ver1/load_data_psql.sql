-- Create Database
-- CREATE DATABASE ecommerce;

-- Switch to the Database
-- \c ecommerce;

CREATE SCHEMA IF NOT EXISTS e_commerce;
SET search_path TO e_commerce, public;

CREATE TABLE IF NOT EXISTS e_commerce.products (
	product_pk serial PRIMARY KEY NOT NULL,
	product_id integer NOT NULL,
	brand varchar,
	category_id bigint NOT NULL,
	category_code varchar,
	CONSTRAINT unique_product UNIQUE (product_id, brand, category_id)
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.users (
	user_id serial PRIMARY KEY NOT NULL
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.events (
	event_id serial PRIMARY KEY NOT NULL,
	event_time timestamp NOT NULL,
	product_pk integer NOT NULL,
	user_id integer NOT NULL,
	event_type varchar NOT NULL,
	user_session uuid NOT NULL,
	price real NOT NULL,
	CONSTRAINT unique_event UNIQUE (event_time, product_pk, user_id),
	CONSTRAINT fk_products_product_id_to_events_product_id FOREIGN KEY (product_pk) REFERENCES e_commerce.products (product_pk) ON DELETE CASCADE,
	CONSTRAINT fk_user_user_id_to_events_user_id FOREIGN KEY (user_id) REFERENCES e_commerce.users (user_id) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.campaigns (
	campaign_pk serial PRIMARY KEY NOT NULL,
	id bigint NOT NULL,
	campaign_type varchar(52) NOT NULL,
	channel varchar(48) NOT NULL,
	topic varchar,
	CONSTRAINT unique_campaign UNIQUE (id, campaign_type),
	CONSTRAINT chk_campaign_type CHECK (campaign_type IN ('bulk', 'trigger', 'transactional'))
) TABLESPACE pg_default;


COMMENT ON COLUMN e_commerce.campaigns.id IS E'Campaign ID';

CREATE TABLE IF NOT EXISTS e_commerce.clients (
	client_id bigserial PRIMARY KEY NOT NULL
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.messages (
	message_id uuid PRIMARY KEY NOT NULL,
	campaign_id bigint NOT NULL,
	message_type varchar(52) NOT NULL,
	client_id bigint NOT NULL,
	channel varchar(48) NOT NULL,
	platform varchar,
	email_provider varchar,
	sent_at timestamp NOT NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	CONSTRAINT fk_client_client_id_to_messages_client_id FOREIGN KEY (client_id) REFERENCES e_commerce.clients (client_id) ON DELETE CASCADE,
	CONSTRAINT fk_campaigns_compaign_pk_to_messages_campaign_id FOREIGN KEY (campaign_id, message_type) REFERENCES e_commerce.campaigns (id, campaign_type) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.client_first_purchase_date (
	client_id bigserial PRIMARY KEY NOT NULL,
	first_purchase_date date NOT NULL,
	CONSTRAINT fk_client_client_id_to_client_first_purchase_date_client_id FOREIGN KEY (client_id) REFERENCES e_commerce.clients (client_id) ON DELETE CASCADE
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
	started_at timestamp  NOT NULL,
	finished_at timestamp,
	total_count integer,
	warmup_mode boolean  NOT NULL,
	hour_limit integer,
	ab_test boolean NOT NULL,
	CONSTRAINT fk_campaigns_compaign_pk_to_compaign_bulks_compaign_pk FOREIGN KEY (campaign_pk) REFERENCES e_commerce.campaigns (campaign_pk) ON DELETE CASCADE
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
	CONSTRAINT fk_campaigns_compaign_pk_to_compaign_subjects_compaign_pk FOREIGN KEY (campaign_pk) REFERENCES e_commerce.campaigns (campaign_pk) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.campaign_triggers (
	campaign_pk serial PRIMARY KEY NOT NULL,
	position smallint DEFAULT 1 NOT NULL,
	CONSTRAINT fk_campaigns_campaign_pk_to_campaign_trigger_campaign_pk FOREIGN KEY (campaign_pk) REFERENCES e_commerce.campaigns (campaign_pk) ON DELETE CASCADE
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS e_commerce.message_behavior (
	message_id uuid NOT NULL,
	type varchar NOT NULL,
	happened_first_time timestamp NOT NULL,
	happened_last_time timestamp,
	CONSTRAINT unique_message_behavior PRIMARY KEY (message_id, type),
	CONSTRAINT fk_messages_message_id_to_message_behavior_message_id FOREIGN KEY (message_id) REFERENCES e_commerce.messages (message_id) ON DELETE CASCADE
) TABLESPACE pg_default;

\COPY users(user_id)  FROM 'cleaned_datasets/psql/ver1/users.csv'  DELIMITER ','  CSV HEADER;
\COPY clients(client_id)  FROM 'cleaned_datasets/psql/ver1/clients.csv'  DELIMITER ','  CSV HEADER;
\COPY friends(friend1, friend2)  FROM 'cleaned_datasets/psql/ver1/friends.csv'  DELIMITER ','  CSV HEADER;
\COPY products(product_id, brand, category_id, category_code, product_pk)  FROM 'cleaned_datasets/psql/ver1/products.csv'  DELIMITER ','  CSV HEADER;
\COPY events(product_pk, user_id, event_time, event_type, user_session, price)  FROM 'cleaned_datasets/psql/ver1/events.csv'  DELIMITER ','  CSV HEADER;
\COPY client_first_purchase_date(client_id, first_purchase_date)  FROM 'cleaned_datasets/psql/ver1/client_first_purchase_date.csv'  DELIMITER ','  CSV HEADER;
\COPY campaigns(campaign_pk, id, campaign_type, channel, topic)  FROM 'cleaned_datasets/psql/ver1/campaigns.csv'  DELIMITER ','  CSV HEADER;
\COPY campaign_bulks(campaign_pk,started_at,finished_at,total_count,warmup_mode,hour_limit,ab_test)  FROM 'cleaned_datasets/psql/ver1/campaign_bulks.csv'  DELIMITER ','  CSV HEADER;
\COPY campaign_subjects(campaign_pk,subject_length,subject_with_personalization,subject_with_deadline,subject_with_emoji,subject_with_bonuses,subject_with_discount,subject_with_saleout)  FROM 'cleaned_datasets/psql/ver1/campaign_subjects.csv'  DELIMITER ','  CSV HEADER;
\COPY campaign_triggers(campaign_pk, position)  FROM 'cleaned_datasets/psql/ver1/campaign_triggers.csv'  DELIMITER ','  CSV HEADER;
\COPY messages(message_id,campaign_id,message_type,client_id,channel,platform,email_provider,sent_at,created_at,updated_at)  FROM 'cleaned_datasets/psql/ver1/messages.csv'  DELIMITER ','  CSV HEADER;
\COPY message_behavior(message_id,type,happened_first_time,happened_last_time)  FROM 'cleaned_datasets/psql/ver1/message_behavior.csv'  DELIMITER ','  CSV HEADER;


