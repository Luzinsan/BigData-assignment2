neo4j-admin database import full ^
  --nodes=user=import/users.csv ^
  --nodes=client=import/clients.csv ^
  --nodes=campaign=import/campaigns.csv ^
  --nodes=message=import/messages.csv ^
  --nodes=product=import/products.csv ^
  --relationships=FRIENDSHIP=import/friends.csv ^
  --relationships=OWNS=import/user_owns.csv ^
  --relationships=HAS_BULK_DETAILS=import/campaign_bulks.csv ^
  --relationships=HAS_SUBJECT_DETAILS=import/campaign_subjects.csv ^
  --relationships=HAS_TRIGGER_DETAILS=import/campaign_triggers.csv ^
  --relationships=SENT_TO=import/message_sent.csv ^
  --relationships=BELONGS_TO=import/messages_belong_to.csv ^
  --relationships=DO_BEHAVIOR=import/message_behavior.csv ^
  --relationships=INTERACTED_WITH=import/events.csv ^
  --verbose --overwrite-destination ^
  neo4j