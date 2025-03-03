import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Configure logger to monitor processing progress.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATASET_PATH = 'datasets'
CLEANED_PATH = 'cleaned_datasets'

# ------------------------------------------------------------------------------
# PROCESS MESSAGES
# ------------------------------------------------------------------------------
logger.info("Reading messages.csv with appropriate parsing and dtypes")
messages = pd.read_csv(
    Path(DATASET_PATH, 'messages.csv'),
    parse_dates=[
        'clicked_first_time_at', 
        'clicked_last_time_at',
        'opened_first_time_at',
        'opened_last_time_at', 
        'unsubscribed_at',
        'hard_bounced_at',
        'soft_bounced_at',
        'complained_at',
        'purchased_at',
        'blocked_at',
        'created_at',
        'sent_at',
        'updated_at'
    ],
    date_format='%Y-%m-%d %H:%M:%S.%f',
    true_values=['t'], false_values=['f'],
    dtype={
        'message_id': 'string',
        'campaign_id': 'int32',
        'message_type': 'category',
        'channel': 'category',
        'platform': 'category', 
        'email_provider': 'string', 
        'user_device_id': 'int16', 
        'user_id': 'int32'
    }
).drop(columns=['category', 'stream', 'id', 'date'])
logger.info("Messages loaded, shape: %s", messages.shape)

# Create an abstract unique ID for each unique (campaign_id, message_type) pair.
# This ID will serve as the primary key in the abstract messages table.
logger.info("Generating unique abstract message IDs based on (campaign_id, message_type)")
unique_messages = messages[['campaign_id', 'message_type']].drop_duplicates().reset_index(drop=True)
unique_messages['id'] = unique_messages.index + 1  # auto-increment starting from 1
messages = messages.merge(unique_messages, on=['campaign_id', 'message_type'], how='left')
logger.info("Abstract message IDs merged, new shape: %s", messages.shape)

# Extract distinct client and user information for later normalization.
clients = messages[['client_id', 'user_id', 'user_device_id']].drop_duplicates()
users = messages['user_id'].drop_duplicates()

# ------------------------------------------------------------------------------
# PROCESS BEHAVIOR DATA FROM MESSAGES
# ------------------------------------------------------------------------------
logger.info("Extracting behavior data from messages into a long format")
# Identify behavior types by detecting columns starting with 'is_'
behaviors_cols = [col.replace('is_', '') for col in messages.columns if col.startswith('is_')]

# For each behavior type, extract the flag and corresponding time columns.
# Note: Some behaviors have only a single timestamp column (<behavior>_at), which we record as 'happened_first_time'.
message_behavior_list = [
    (
        messages[['message_id', f'is_{b}']].rename(columns={f'is_{b}': 'flag'})
        .assign(
            type=b,
            happened_first_time = messages.get(f'{b}_first_time_at', messages.get(f'{b}_at', pd.NaT)),
            happened_last_time = messages.get(f'{b}_last_time_at', pd.NaT)
        )
    )
    for b in behaviors_cols
]
# Concatenate behavior data for all behavior types
message_behavior = pd.concat(message_behavior_list, ignore_index=True)
# Retain only records where the behavior actually occurred.
message_behavior = message_behavior[message_behavior['flag'] == True].drop(columns=['flag'])
# Sort by message_id and timestamp to preserve temporal order.
message_behavior = message_behavior.sort_values(['message_id', 'happened_first_time'])
# Set a MultiIndex for ease of further merging or analysis.
message_behavior = message_behavior.set_index(['message_id', 'type'])
logger.info("Extracted behavior data shape: %s", message_behavior.shape)
message_behavior.to_csv(Path(CLEANED_PATH, 'message_behavior.csv'))
del(message_behavior)

# ------------------------------------------------------------------------------
# CREATE MESSAGE_SENT TABLE
# ------------------------------------------------------------------------------
logger.info("Creating message_sent table with relevant timestamps and client info")
message_sent = messages[['message_id', 'id', 'client_id', 'email_provider', 'platform',
                         'created_at', 'updated_at', 'sent_at']].set_index('message_id')
message_sent.to_csv(Path(CLEANED_PATH, 'message_sent.csv'))
del(message_sent)

# ------------------------------------------------------------------------------
# CREATE ABSTRACT MESSAGES TABLE
# ------------------------------------------------------------------------------
logger.info("Creating abstract messages table with primary key and campaign linkage")
abstract_messages = messages[['id', 'campaign_id', 'message_type', 'channel']].drop_duplicates().set_index('id')
abstract_messages.to_csv(Path(CLEANED_PATH, 'messages.csv'))
del(messages)

# ------------------------------------------------------------------------------
# PROCESS CAMPAIGNS
# ------------------------------------------------------------------------------
logger.info("Reading campaigns.csv and applying business rules")
campaigns = pd.read_csv(
    Path(DATASET_PATH, 'campaigns.csv'),
    parse_dates=['started_at', 'finished_at'],
    dtype={
        'campaign_type': 'category',
        'channel': 'category',
        'topic': 'string',
        'total_count': 'Int32',
        'ab_test': 'boolean',
        'warmup_mode': 'boolean',
        'hour_limit': 'Int32',
        'subject_length': 'Int16',
        'subject_with_personalization': 'boolean',
        'subject_with_deadline': 'boolean',
        'subject_with_emoji': 'boolean',
        'subject_with_bonuses': 'boolean',
        'subject_with_discount': 'boolean',
        'subject_with_saleout': 'boolean',
        'is_test': 'boolean',
        'position': 'Int16'
    }
).fillna({
    'ab_test': False,
    'warmup_mode': False,
    'is_test': False
})
campaigns.index.names = ['campaign_pk']

# Apply filtering rules to remove invalid campaigns based on type-specific criteria.
logger.info("Filtering campaigns based on test flags and missing critical fields")
campaigns = campaigns[~(
    (campaigns['is_test'] == True)
    | ((campaigns['campaign_type'] == 'bulk') & 
       (campaigns['started_at'].isna() | ((campaigns['warmup_mode'] == True) & campaigns['hour_limit'].isna())))
    | ((campaigns['campaign_type'] == 'trigger') & campaigns['position'].isna())
)].drop(columns=['is_test'])

# ------------------------------------------------------------------------------
# SPLIT CAMPAIGNS INTO SUBTABLES BASED ON SPECIFIC ATTRIBUTES
# ------------------------------------------------------------------------------
# Bulk-specific attributes: only bulk campaigns have topic, finish time, etc.
bulk_cols = ['started_at', 'finished_at', 'total_count', 'warmup_mode', 'hour_limit', 'ab_test']
bulks = campaigns[campaigns['campaign_type'] == 'bulk'][bulk_cols]
logger.info("Extracted bulk campaign data, shape: %s", bulks.shape)
bulks.to_csv(Path(CLEANED_PATH, 'bulks.csv'))
del(bulks)

# Subject-specific attributes: applicable to campaigns with a subject (e.g., email, mobile_push, web_push)
subject_cols = [
    'subject_length',
    'subject_with_personalization',
    'subject_with_deadline',
    'subject_with_emoji',
    'subject_with_bonuses',
    'subject_with_discount',
    'subject_with_saleout'
]
campaign_subject = campaigns[~campaigns['channel'].isin(['sms', 'multichannel'])][subject_cols]
logger.info("Extracted campaign subject data, shape: %s", campaign_subject.shape)
campaign_subject.to_csv(Path(CLEANED_PATH, 'campaign_subject.csv'))
del(campaign_subject)

# Trigger-specific attributes: only trigger campaigns have a position.
trigger_cols = ['position']
triggers = campaigns[campaigns['campaign_type'] == 'trigger'][trigger_cols]
logger.info("Extracted trigger campaign data, shape: %s", triggers.shape)
triggers.to_csv(Path(CLEANED_PATH, 'triggers.csv'))
del(triggers)

# Remove subtype columns from the general campaigns table to isolate common attributes.
campaigns = campaigns.drop(columns=bulk_cols + subject_cols + trigger_cols)
logger.info("Final general campaigns table shape: %s", campaigns.shape)
campaigns.to_csv(Path(CLEANED_PATH, 'campaigns.csv'))
del(campaigns)

# ------------------------------------------------------------------------------
# PROCESS EVENTS
# ------------------------------------------------------------------------------
logger.info("Reading events.csv and performing product/user mappings")
events = pd.read_csv(
    Path(DATASET_PATH, 'events.csv'),
    parse_dates=['event_time'],
    date_format='%Y-%m-%d %H:%M:%S UTC',
    dtype={
        'event_type': 'category',
        'product_id': 'int32',
        'category_id': 'int64',
        'category_code': 'category',
        'brand': 'category',
        'price': 'float32',
        'user_id': 'int32',
        'user_session': 'string'
    }
)
logger.info("Events loaded, shape: %s", events.shape)

# Integrate user IDs from events into our users list.
users = pd.concat([users, events['user_id'].drop_duplicates()])

# Build a unique products table for mapping product IDs to surrogate keys.
unique_products = events[['product_id', 'category_id']].drop_duplicates().reset_index(drop=True)
unique_products['product_pk'] = unique_products.index + 1
events = events.merge(unique_products, on=['product_id', 'category_id'], how='left')

# Associate each product with its brand via a unique product card.
unique_product_cards = events[['product_pk', 'brand']].drop_duplicates().reset_index(drop=True)
unique_product_cards['product_card_pk'] = unique_product_cards.index + 1
events = events.merge(unique_product_cards, on=['product_pk', 'brand'], how='left')

# Create the final products table (normalized) and write to CSV.
products = events[['product_pk', 'product_id', 'category_id', 'category_code']].drop_duplicates().set_index('product_pk')
logger.info("Products table generated, shape: %s", products.shape)
products.to_csv(Path(CLEANED_PATH, 'products.csv'))
del(products)

# Create a product_cards table with brand details.
product_cards = events[['product_card_pk', 'product_pk', 'brand']].drop_duplicates().set_index('product_card_pk')
logger.info("Product cards table generated, shape: %s", product_cards.shape)
product_cards.to_csv(Path(CLEANED_PATH, 'product_cards.csv'))
del(product_cards)

# Remove duplicate events (by product_card, user, event_time) and retain relevant columns.
events = events.drop_duplicates(['product_card_pk', 'user_id', 'event_time'])[
    ['product_card_pk', 'user_id', 'event_time', 'event_type', 'user_session', 'price']
]
logger.info("Final events table shape: %s", events.shape)
events.to_csv(Path(CLEANED_PATH, 'events.csv'))
del(events)

# ------------------------------------------------------------------------------
# PROCESS CLIENT FIRST PURCHASE DATA
# ------------------------------------------------------------------------------
logger.info("Reading client_first_purchase_date.csv and integrating with clients/users")
first_purchase = pd.read_csv(
    Path(DATASET_PATH, 'client_first_purchase_date.csv'),
    parse_dates=['first_purchase_date'],
    date_format='%Y-%m-%d',
    dtype={'user_id': 'int32', 'user_device_id': 'int16'}
)
clients = pd.concat([clients, first_purchase[['client_id', 'user_id', 'user_device_id']].drop_duplicates()])
clients.to_csv(Path(CLEANED_PATH, 'clients.csv'))
del(clients)

users = pd.concat([users, first_purchase['user_id'].drop_duplicates()])
users.to_csv(Path(CLEANED_PATH, 'users.csv'))
del(users)

first_purchase = first_purchase[['client_id', 'first_purchase_date']].drop_duplicates()
logger.info("First purchase data shape: %s", first_purchase.shape)
first_purchase.to_csv(Path(CLEANED_PATH, 'first_purchase.csv'))
del(first_purchase)

# ------------------------------------------------------------------------------
# PROCESS FRIENDS
# ------------------------------------------------------------------------------
logger.info("Processing friends.csv to enforce symmetric representation (sort values in each row)")
friends = pd.read_csv(
    Path(DATASET_PATH, 'friends.csv'),
    dtype={'friend1': 'int32', 'friend2': 'int32'}
)
# Sorting each row ensures that (A,B) and (B,A) are represented identically.
friends = pd.DataFrame(np.sort(friends.values, axis=1), columns=friends.columns)
logger.info("Friends table processed, final shape: %s", friends.shape)
friends.to_csv(Path(CLEANED_PATH, 'friends.csv'))
del(friends)

logger.info("Data preprocessing completed successfully.")
