# Preprocession
# uv run .\scripts\loading\clean_data.py
################################### PSQL ######################################
psql.exe -U postgres -c 'DROP DATABASE ecommerce;'
psql.exe -U postgres -c 'CREATE DATABASE ecommerce;'
psql.exe -U postgres -d ecommerce -f .\scripts\loading\load_data_psql.sql

################################## MongoDB #####################################
mongosh --file scripts\loading\load_data_mongodb.js
## Windows:
.\scripts\loading\load_data_mongodb.bat
## Linux
# bash .\scripts\loading\load_data_mongodb.bash
################################### Neo4J ######################################
# Windows
neo4j.bat start
## Create constraints & indexes
cat scripts\loading\load_data_neo4j.cypher | cypher-shell
neo4j.bat stop
## Load data with neo4j-admin
.\scripts\loading\load_data_neo4j.bat
# Linux - similar
