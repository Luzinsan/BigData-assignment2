import pandas as pd
import numpy as np
from pathlib import Path
import logging
import os

# Configure logger to monitor processing progress.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATASET_PATH = Path('datasets')
PSQL_CLEANED_PATH = Path('output/psql/')
MONGO_CLEANED_PATH = Path('output/mongo/')
load_to_neo4j_import_dir = True


if load_to_neo4j_import_dir \
    and (path_list := os.environ.get('PATH')) \
    and (neo4j_root := [i for i in path_list.split(';') if 'neo4j' in i.lower()]):
    NEO4J_CLEANED_PATH = Path(neo4j_root[0]).parent / 'import'
else: NEO4J_CLEANED_PATH = Path('output/neo4j/')
PSQL_CLEANED_PATH.mkdir(exist_ok=True, parents=True)
MONGO_CLEANED_PATH.mkdir(exist_ok=True, parents=True)
NEO4J_CLEANED_PATH.mkdir(exist_ok=True, parents=True)

# ------------------------------------------------------------------------------
# PROCESS MESSAGES
# ------------------------------------------------------------------------------
logger.info("Reading messages.csv with appropriate parsing and dtypes")
messages = pd.read_csv(
    DATASET_PATH / 'messages.csv',
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
        'stream':'category',
        'email_provider': 'string', 
        'user_device_id': 'int16', 
        'user_id': 'int32'
    }
).drop(columns=['category', 'date', 'id','stream'])
logger.info("Messages loaded, shape: %s", messages.shape)

logger.info("Adjusting UUID formatting...")
messages['message_id'] = messages['message_id'].str.replace(
    r'(\w{8})(\w{4})(\w{4})(\w{3})-(\w)(\w{12})', 
    r'\1-\2-\3-\4\5-\6', regex=True)
logger.info("UUID formatting complete")

# Create an abstract unique ID for each unique (campaign_id, message_type) pair.
# This ID will serve as the primary key in the abstract messages table.
logger.info("[PSQL]: Generating unique abstract message IDs based on (campaign_id, message_type)")
unique_messages = messages[['campaign_id', 'message_type','channel']].drop_duplicates().reset_index(drop=True)
unique_messages['id'] = unique_messages.index + 1  # auto-increment starting from 1
messages = messages.merge(unique_messages, on=['campaign_id', 'message_type','channel'], how='left')
logger.info("[PSQL]: Abstract message IDs merged, new shape: %s", messages.shape)


# Extract distinct client and user for normalization.
clients = messages[['client_id','user_id','user_device_id']].drop_duplicates('client_id')
users = messages['user_id'].drop_duplicates()

# ------------------------------------------------------------------------------
# PROCESS BEHAVIOR DATA FROM MESSAGES
# ------------------------------------------------------------------------------
logger.info("Extracting behavior data from messages into long format")
# Identify behavior types from columns starting with 'is_'
behaviors_cols = [col.replace('is_', '') for col in messages.columns if col.startswith('is_')]
message_behavior_list = [
    (messages[['message_id', f'is_{b}']].rename(columns={f'is_{b}': 'flag'})
        .assign(
            type=b,
            # Use the specific first/last columns if present; otherwise, fallback to <behavior>_at for first_time.
            happened_first_time = messages.get(f'{b}_first_time_at', messages.get(f'{b}_at', pd.NaT)),
            happened_last_time  = messages.get(f'{b}_last_time_at', pd.NaT)
        ))
    for b in behaviors_cols
]
message_behavior = pd.concat(message_behavior_list, ignore_index=True)
message_behavior = message_behavior[message_behavior['flag'] == True].drop(columns=['flag'])
message_behavior = message_behavior.sort_values(['message_id', 'happened_first_time'])
# Set a MultiIndex for clear identification of behavior per message.
message_behavior = message_behavior.set_index(['message_id', 'type'])
logger.info("Behavior data shape: %s", message_behavior.shape)
message_behavior.to_csv(PSQL_CLEANED_PATH / 'message_behavior.csv')
#################################### NEO4J ####################################
def convert_for_neo4J_node(df_orig, name):
    df = df_orig.copy()
    df.rename(columns={df.columns[0]: 
                       f'{df.columns[0]}:ID({name})'}, 
                       inplace=True)
    df.insert(1, ':LABEL', name)
    return df

def convert_for_neo4J_rels(df_orig, name, start_table, end_table, duplicate=True):
    df = df_orig.copy()
    if (start_table==end_table) and duplicate:
        df.insert(1, df.columns[0], df.iloc[:, 0], allow_duplicates=True)
    df.columns.values[0] = f'{df.columns[0]}:START_ID({start_table})'
    df.columns.values[1] = f'{df.columns[1]}:END_ID({end_table})'
    df.insert(2, ':TYPE', name)
    return df

messages.merge(message_behavior.reset_index(), 
               'right', 'message_id')\
    [['client_id','message_id',
      'type','happened_first_time','happened_last_time']]\
    .transform(convert_for_neo4J_rels, name='DO_BEHAVIOR', start_table='client', end_table='message')\
    .to_csv('output/neo4j/message_behavior.csv', index=False)
# ------------------------------------------------------------------------------
# CREATE MESSAGE_SENT TABLE
# ------------------------------------------------------------------------------
logger.info("[PSQL]: Creating message_sent.")
message_sent = messages[['message_id', 'id', 'client_id', 
                         'email_provider', 'platform',
                         'sent_at']].set_index('message_id')
message_sent.to_csv(PSQL_CLEANED_PATH / 'message_sent.csv')
message_sent.reset_index().drop(columns='id')\
    .transform(convert_for_neo4J_rels, 
               name='SENT_TO', start_table='message', end_table='client')\
    .to_csv(NEO4J_CLEANED_PATH / 'message_sent.csv', index=False)
del(message_sent)
# ------------------------------------------------------------------------------
# CREATE ABSTRACT MESSAGES TABLE
# ------------------------------------------------------------------------------
logger.info("[PSQL]: Creating abstract messages table.")
abstract_messages = messages[['id', 'campaign_id', 'message_type',
                              'channel','created_at', 'updated_at']]\
                                .drop_duplicates('id').set_index('id')
abstract_messages.to_csv(PSQL_CLEANED_PATH / 'messages.csv')
messages[['message_id', 'campaign_id', 'message_type','channel']]\
    .transform(convert_for_neo4J_node, name='message')\
    .to_csv(NEO4J_CLEANED_PATH / 'messages.csv', index=False)
del(abstract_messages)
# ------------------------------------------------------------------------------
# MESSAGES TABLE (using for MongoDB)
# ------------------------------------------------------------------------------
logger.info("[MONGODB]: Creating messages table with complete columns per model...")
messages = messages[['message_id', 'campaign_id', 'message_type', 'client_id', 
                             'channel', 'platform', 'email_provider',
                             'sent_at', 'created_at', 'updated_at']].drop_duplicates('message_id').set_index('message_id')
logger.info("[MONGODB]: Messages table shape: %s", messages.shape)
################################# MONGODB ######################################
# Group behavior events by message_id and collect as list of dictionaries.
logger.info("[MONGODB]: Grouping message behaviors for Mongo database...")
behavior_grouped = message_behavior.reset_index().groupby('message_id')\
    [['message_id', 'type','happened_first_time','happened_last_time']].apply(
    lambda grp: grp[['type', 'happened_first_time', 'happened_last_time']].to_dict('records')
).reset_index(name='behaviors')
logger.info("[MONGODB]: Complete grouping message_behavior.")
messages_mongo = messages.reset_index().merge(behavior_grouped, on='message_id', how='left')
# For messages without behavior records, set empty list.
messages_mongo['behaviors'] = messages_mongo['behaviors'].apply(lambda x: x if isinstance(x, list) else [])
logger.info("[MONGODB]: Final messages collection shape: %s", messages_mongo.shape)
# Save final messages collection (with embedded events) to JSON
messages_mongo.to_json(MONGO_CLEANED_PATH / 'messages.json', orient='records', date_format='iso')
logger.info("[MONGODB]: Messages file created successfully.")
del(messages_mongo, message_behavior)
# ------------------------------------------------------------------------------
# PROCESS CAMPAIGNS
# ------------------------------------------------------------------------------
logger.info("Reading campaigns.csv and applying business rules")
campaigns = pd.read_csv(
    DATASET_PATH / 'campaigns.csv',
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
logger.info("Campaigns loaded, shape: %s", campaigns.shape)

logger.info("Filtering campaigns based on business rules")
campaigns = campaigns[~(
    (campaigns['is_test'] == True)
    | ((campaigns['campaign_type'] == 'bulk') & 
       (campaigns['started_at'].isna() | ((campaigns['warmup_mode'] == True) & campaigns['hour_limit'].isna())))
    | ((campaigns['campaign_type'] == 'trigger') & campaigns['position'].isna())
)].drop(columns=['is_test'])
logger.info("Filtered campaigns shape: %s", campaigns.shape)

campaigns.reset_index().merge(messages.reset_index(), how='inner', 
                left_on=['id','campaign_type'], 
                right_on=['campaign_id','message_type'])\
                [['message_id','campaign_pk','created_at','updated_at']]\
                .transform(convert_for_neo4J_rels,
                           name='BELONGS_TO', start_table='message', end_table='campaign')\
                .to_csv(NEO4J_CLEANED_PATH / 'messages_belong_to.csv', index=False)


################### MONGODB ######################
# Build embedded subdocuments for campaign details.
def build_campaign_doc(row):
    doc = {
        "id": row["id"],
        "campaign_type": row["campaign_type"],
        "channel": row["channel"],
        "topic": row.get("topic")
    }
    if row["campaign_type"] == "bulk":
        doc["bulk_details"] = {
            "started_at": row["started_at"],
            "finished_at": row.get("finished_at"),
            "total_count": row.get("total_count", None),
            "warmup_mode": row["warmup_mode"],
            "hour_limit": row.get("hour_limit", None),
            "ab_test": row["ab_test"]
        }
    if row["campaign_type"] == "trigger":
        doc["trigger_details"] = {"position": row.get("position", None)}
    if row["channel"] not in ["sms", "multichannel"]:
        doc["subject_details"] = {
            "subject_length": row.get("subject_length", None),
            "subject_with_personalization": row["subject_with_personalization"],
            "subject_with_deadline": row["subject_with_deadline"],
            "subject_with_emoji": row["subject_with_emoji"],
            "subject_with_bonuses": row["subject_with_bonuses"],
            "subject_with_discount": row["subject_with_discount"],
            "subject_with_saleout": row["subject_with_saleout"]
        }
    return doc
logger.info("[MONGODB]: Preparing campaign documents")
campaigns_docs = campaigns.reset_index().apply(build_campaign_doc, axis=1)
logger.info("[MONGODB]: Prepared %s campaign documents", campaigns_docs.shape[0])
campaigns_docs.to_json(MONGO_CLEANED_PATH / 'campaigns.json', orient='records', date_format='iso')
del(campaigns_docs)

# Split campaigns into subtype tables.
logger.info("[PSQL]: Preparing campaign table.")
bulk_cols = ['started_at', 'finished_at', 'total_count', 'warmup_mode', 'hour_limit', 'ab_test']
bulks = campaigns[campaigns['campaign_type'] == 'bulk'][bulk_cols]
logger.info("Extracted bulk campaign data, shape: %s", bulks.shape)
bulks.to_csv(PSQL_CLEANED_PATH / 'campaign_bulks.csv')
bulks.reset_index().transform(convert_for_neo4J_rels,
                name='has_bulk_details', 
                start_table='campaign', end_table='campaign')\
     .to_csv(NEO4J_CLEANED_PATH / 'campaign_bulks.csv', index=False)
del(bulks)

subject_cols = [
    'subject_length',
    'subject_with_personalization',
    'subject_with_deadline',
    'subject_with_emoji',
    'subject_with_bonuses',
    'subject_with_discount',
    'subject_with_saleout'
]
campaign_subjects = campaigns[~campaigns['channel'].isin(['sms', 'multichannel'])][subject_cols]
logger.info("[PSQL]: Extracted campaign subject data, shape: %s", campaign_subjects.shape)
campaign_subjects.to_csv(PSQL_CLEANED_PATH / 'campaign_subjects.csv')
campaign_subjects.reset_index().transform(convert_for_neo4J_rels,
                  name='has_subject_details', 
                  start_table='campaign', end_table='campaign')\
                 .to_csv(NEO4J_CLEANED_PATH / 'campaign_subjects.csv', index=False)
del(campaign_subjects)

trigger_cols = ['position']
triggers = campaigns[campaigns['campaign_type'] == 'trigger'][trigger_cols]
logger.info("[PSQL]: Extracted trigger campaign data, shape: %s", triggers.shape)
triggers.to_csv(PSQL_CLEANED_PATH / 'campaign_triggers.csv')
triggers.reset_index().transform(convert_for_neo4J_rels,
                name='has_trigger_details', 
                start_table='campaign', end_table='campaign')\
        .to_csv(NEO4J_CLEANED_PATH / 'campaign_triggers.csv', index=False)
del(triggers)

campaigns = campaigns.drop(columns=bulk_cols + subject_cols + trigger_cols)
logger.info("[PSQL]: Final general campaigns table shape: %s", campaigns.shape)
campaigns.to_csv(PSQL_CLEANED_PATH / 'campaigns.csv')
campaigns.reset_index().transform(convert_for_neo4J_node, name='campaign')\
         .to_csv(NEO4J_CLEANED_PATH / 'campaigns.csv', index=False)
del(campaigns)

# ------------------------------------------------------------------------------
# PROCESS EVENTS & PRODUCTS
# ------------------------------------------------------------------------------
logger.info("Reading events.csv and mapping products")
events = pd.read_csv(
    DATASET_PATH / 'events.csv',
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
# Update users list with those from events.
users = pd.concat([users, events['user_id'].drop_duplicates()]).drop_duplicates()

logger.info("[MONGODB/NEO4J]: Building unique products.")
unique_products = events[['product_id', 'brand', 'category_id','category_code']]\
    .drop_duplicates(['product_id', 'brand', 'category_id']).reset_index(drop=True)
unique_products.insert(0, 'product_pk', unique_products.index + 1)
logger.info("[MONGODB/NEO4J]: Unique products table shape: %s", unique_products.shape)
unique_products.to_json(MONGO_CLEANED_PATH / 'products.json', orient='records', date_format='iso', index=False)
unique_products.transform(convert_for_neo4J_node, 
                          name='product')\
               .to_csv(NEO4J_CLEANED_PATH / 'products.csv', index=False)

logger.info("[MONGODB/NEO4J]: Building events with product_pk referrence.")
# Merge surrogate product key into events.
events_mongo = events.merge(unique_products, on=['product_id', 'brand', 'category_id'], how='left')
# Retain only relevant event columns.
events_mongo = events_mongo.drop_duplicates(['event_time','product_pk', 'user_id'])\
    [['product_pk', 'user_id', 'event_time', 'event_type', 'user_session', 'price']]
logger.info("[MONGODB/NEO4J]: Final events table shape: %s", events_mongo.shape)
events_mongo.to_json(MONGO_CLEANED_PATH / 'events.json', orient='records', date_format='iso', index=False)
events_mongo.transform(convert_for_neo4J_rels, 
                       name='events', start_table='product', end_table='user')\
            .to_csv(NEO4J_CLEANED_PATH / 'events.csv', index=False)
del(events_mongo, unique_products)
#################################### PSQL ##########################################
# Function to choose the representative value from category_code within each group.
def choose_representative(series):
    non_null = series.dropna()
    if non_null.empty: return np.nan
    else:
        mode_vals = non_null.mode()
        if not mode_vals.empty:
            return mode_vals.iloc[0]
        else:
            return non_null.iloc[0]
logger.info("[PSQL]: Grouping unique events with filtering")        
# Build a unique products table for mapping product IDs to surrogate keys.    
unique_products = events.groupby(['product_id', 'category_id'], 
                                 as_index=False)['category_code']\
    .agg(choose_representative)
unique_products['product_pk'] = unique_products.index + 1
events = events.drop(columns='category_code').merge(unique_products, 
                                                    on=['product_id', 'category_id'], 
                                                    how='left')
logger.info("[PSQL]: Building product table.") 
# Associate each product with its brand via a unique product card.
unique_product_cards = events[['product_pk', 'brand']].drop_duplicates().reset_index(drop=True)
unique_product_cards['product_card_pk'] = unique_product_cards.index + 1
events = events.merge(unique_product_cards, on=['product_pk', 'brand'], how='left')
# Create the final products table (normalized) and write to CSV.
products = events[['product_pk', 'product_id', 'category_id', 'category_code']].drop_duplicates().set_index('product_pk')
logger.info("[PSQL]: Products table generated, shape: %s", products.shape)
products.to_csv(PSQL_CLEANED_PATH / 'products.csv')
del(products)
# Create a product_cards table with brand details.
product_cards = events[['product_card_pk', 'product_pk', 'brand']].drop_duplicates().set_index('product_card_pk')
logger.info("[PSQL]: Product cards table generated, shape: %s", product_cards.shape)
product_cards.to_csv(PSQL_CLEANED_PATH / 'product_cards.csv')
del(product_cards)
# Remove duplicate events (by product_card, user, event_time) and retain relevant columns.
events = events.drop_duplicates(['product_card_pk', 'user_id', 'event_time'])[
    ['product_card_pk', 'user_id', 'event_time', 'event_type', 'user_session', 'price']
]
logger.info("[PSQL]: Final events table shape: %s", events.shape)
events.to_csv(PSQL_CLEANED_PATH / 'events.csv', index=False)
del(events)
# ------------------------------------------------------------------------------
# PROCESS CLIENT FIRST PURCHASE DATA
# ------------------------------------------------------------------------------
logger.info("Reading client_first_purchase_date.csv and integrating with clients/users")
first_purchase = pd.read_csv(
    DATASET_PATH / 'client_first_purchase_date.csv',
    parse_dates=['first_purchase_date'],
    date_format='%Y-%m-%d',
    dtype={'user_id': 'int32', 'user_device_id': 'int16'}
).drop_duplicates(['client_id'])
logger.info("First purchase data shape: %s", first_purchase.shape)
clients = pd.concat([clients, first_purchase\
                     .drop(columns='first_purchase_date')]).drop_duplicates('client_id')
clients = clients.merge(first_purchase[['client_id','first_purchase_date']], how='left', on='client_id')
clients.to_csv(PSQL_CLEANED_PATH / 'clients.csv', columns=['client_id', 'first_purchase_date'], index=False)
clients.drop(columns=['user_id','user_device_id'])\
       .transform(convert_for_neo4J_node, name='client')\
       .to_csv(NEO4J_CLEANED_PATH / 'clients.csv', index=False)
users = pd.concat([users, first_purchase['user_id'].drop_duplicates()]).drop_duplicates()
# ------------------------------------------------------------------------------
# PROCESS FRIENDS
# ------------------------------------------------------------------------------
logger.info("Processing friends.csv to enforce symmetric representation (sort values in each row)")
friends = pd.read_csv(
    DATASET_PATH / 'friends.csv',
    dtype={'friend1': 'int32', 'friend2': 'int32'}
)
# Sorting each row ensures symmetric pairs are stored consistently.
friends = pd.DataFrame(np.sort(friends.values, axis=1), columns=friends.columns).drop_duplicates()
logger.info("Friends table processed, final shape: %s", friends.shape)
users = pd.concat([users, 
                   friends['friend1'].drop_duplicates()]).drop_duplicates()
users = pd.concat([users, 
                   friends['friend2'].drop_duplicates()]).drop_duplicates()
users.to_csv(PSQL_CLEANED_PATH / 'users.csv', index=False)
friends.to_csv(PSQL_CLEANED_PATH / 'friends.csv', index=False)
friends.to_json(MONGO_CLEANED_PATH / 'friends.json', orient='records', index=False)

friends.transform(convert_for_neo4J_rels, duplicate=False,
                  name='FRIENDSHIP', start_table='user', end_table='user')\
       .to_csv(NEO4J_CLEANED_PATH / 'friends.csv', index=False)
del(friends)

users.rename('user_id').to_frame().transform(convert_for_neo4J_node, name='user')\
     .to_csv(NEO4J_CLEANED_PATH / 'users.csv', index=False)
users = pd.merge(users.rename('user_id'),  clients.drop(columns='first_purchase_date'), 
                 on='user_id', how='left')
users[['user_id','client_id']].transform(convert_for_neo4J_rels, 
                                         name='OWNS', start_table='user', end_table='client')\
                              .to_csv(NEO4J_CLEANED_PATH / 'user_owns.csv', index=False)
del(clients)

users = users.merge(first_purchase[['client_id','first_purchase_date']], how='left', on='client_id')
logger.info("[MONGODB]: Start grouping users with cliends dictionary")
grouped = users.groupby('user_id')\
    [['user_id', 'client_id', 
      'user_device_id', 'first_purchase_date']].apply(
          lambda x: 
          x[['client_id', 'user_device_id', 
             'first_purchase_date']].to_dict('records')).reset_index()
grouped.columns = ['user_id', 'devices']
logger.info("[MONGODB]: Filtering the nan first_purchase_date records in grouped users table")
grouped['devices'] =\
      grouped['devices'].apply(lambda device_list: [
          {k: v 
           for k, v in device.items() if pd.notna(v)}
        for device in device_list])
grouped.to_json(MONGO_CLEANED_PATH / 'users.json', orient='records', date_format='iso')
del(users, grouped)
logger.info("Data preprocessing completed successfully.")