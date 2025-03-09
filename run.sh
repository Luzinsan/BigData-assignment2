# Preprocession
# uv run .\scripts\clean_data.py
################################### PSQL ######################################
psql.exe -U postgres -c 'DROP DATABASE ecommerce;'
psql.exe -U postgres -c 'CREATE DATABASE ecommerce;'
psql.exe -U postgres -d ecommerce -f .\scripts\load_data_psql.sql

################################## MongoDB #####################################
## Windows:
.\scripts\load_data_mongodb.bat
## Linux
# bash .\scripts\load_data_mongodb.bash
################################### Neo4J ######################################
# Windows
# neo4j.bat start
## Create constraints & indexes
# cat scripts/load_data_neo4j.cypher | cypher-shell
# neo4j.bat stop
## Load data with neo4j-admin
# .\scripts\load_data_neo4j.bat
# Linux - similar


#================================================================================
################################# PSQL ver2 #####################################
# uv run .\scripts\clean_data_ver_2.py
# psql.exe -U postgres -c 'DROP DATABASE ecommerce_ver2;'
# psql.exe -U postgres -c 'CREATE DATABASE ecommerce_ver2;'
# psql.exe -U postgres -d ecommerce_ver2 -f .\scripts\load_data_psql_ver2.sql