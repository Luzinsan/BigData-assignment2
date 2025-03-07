mongosh --file ./scripts/load_data_mongodb.js

mongoimport --db ecommerce --collection users --file output/mongo/users.json --jsonArray
mongoimport --db ecommerce --collection friends --file output/mongo/friends.json --jsonArray
mongoimport --db ecommerce --collection campaigns --file output/mongo/campaigns.json --jsonArray
mongoimport --db ecommerce --collection messages --file output/mongo/messages.json --jsonArray
mongoimport --db ecommerce --collection products --file output/mongo/products.json --jsonArray
mongoimport --db ecommerce --collection events --file output/mongo/events.json --jsonArray