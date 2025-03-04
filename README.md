# BigData-assignment2


- All raw datasets needs to be in datasets directory (it can change in DATASET_PATH variable of clean_data_ver#.py script)

## PostgreSQL
- Data preparing. Run:
```
uv run .\scripts\psql\ver2\clean_data.py
```
- Firstly you need to create database `ecommerce` (if you don't have any one). You can do this with:
```
psql -c 'CREATE DATABASE ecommerce;'
```
- Then, load all preprocessed data with:
```
psql -d ecommerce -f .\scripts\psql\ver2\load_data_psql.sql
```

