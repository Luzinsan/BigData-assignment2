# E-commerce Data Analysis with PSQL, MongoDB, and Neo4j

This repository contains the code and data models for a comparative analysis of e-commerce data using three different database systems: PostgreSQL (PSQL), MongoDB, and Neo4j.  The project implements data models, data cleaning and loading scripts, and analysis queries for each database.

## Project Structure

```
.
├── datasets/           <- Raw data files (CSV) - PLACE YOUR DATASETS HERE
├── output/
│   ├── mongo/          <- Processed data for MongoDB (JSON)
│   ├── neo4j/          <- Processed data for Neo4j (CSV)
│   └── psql/           <- Processed data for PSQL (CSV)
├── scripts/
│   ├── loading/
│   │   ├── clean_data.py          <- Python script for data cleaning and transformation
│   │   ├── load_data_mongodb.js   <- MongoDB schema creation script
│   │   ├── load_data_neo4j.cypher <- Neo4j schema creation script (constraints and indexes)
│   │   ├── load_data_psql.sql     <- PSQL schema creation and data loading script
│   │   ├── load_data_mongodb.bat  <- Windows batch script for MongoDB data import
│   │   ├── load_data_mongodb.bash <- Linux/macOS script for MongoDB data import
│   │   └── load_data_neo4j.bat    <- Windows batch script for Neo4j data import using neo4j-admin
│   └── analysis/
│       └── data_analysis.py       <- Python script for data analysis (PSQL)
        └──q1.sql                  <- Psql query for task 1
└── README.md                      <- This file
```

## Setup and Data Loading

This project uses three different databases: PostgreSQL, MongoDB, and Neo4j.  You will need to have these databases installed and running on your system.

**1. Install Required Software:**

*   **PostgreSQL:** Download and install PostgreSQL (version 17.4 or later recommended).
*   **MongoDB:** Download and install MongoDB (mongosh version 2.3.9 or later, MongoDB Server version compatible with this shell version).
*   **Neo4j:** Download and install Neo4j (version 5.24.0 or later recommended).  The Community Edition is sufficient for this project.
*   **Python:** Install Python (version 3.11.0 or later recommended).
*   **uv:** Install `uv` (recommended for virtual environment management).

**2. Clone the Repository:**

```bash
git clone https://github.com/Luzinsan/BigData-assignment2.git
cd BigData-assignment2
```

**3. Create and Activate a Virtual Environment (using `uv`):**

It is highly recommended to use a virtual environment to manage Python dependencies.  This project uses `uv` for virtual environment management:

```bash
uv venv
source .venv/bin/activate
```
(OR) For Windows:
```windows
.venv\Scripts\activate
```

**4. Install Python Dependencies (from pyproject.toml):**

```bash
uv sync
```


**6. Download and Place Datasets:**

Download the required e-commerce datasets from the following sources:

*   [Direct Messaging](https://www.kaggle.com/datasets/mkechinov/direct-messaging)
*   [Ecommerce Behavior Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
*  [E-commerce Purchase History](https://www.kaggle.com/datasets/mkechinov/ecommerce-purchase-history-from-electronics-store)

Place the downloaded CSV files (`messages.csv`, `campaigns.csv`, `events.csv`, `client_first_purchase_date.csv`, `friends.csv`) into the `datasets/` directory.

**7. Preprocess the Data:**

Run the `clean_data.py` script to clean, transform, and prepare the data for each database:

```bash
uv run python scripts/loading/clean_data.py
```

This script will generate CSV files for PSQL and Neo4j, and JSON files for MongoDB, storing them in the `output/psql`, `output/mongo`, and `output/neo4j` directories, respectively.

**8. Load Data into Databases:**

*   **PSQL:**
    *   Create an empty database, and then use the `load_data_psql.sql` script to create the schema and load the data:
        ```bash
         psql -U postgres -c 'CREATE DATABASE ecommerce;'
         psql -U postgres -d ecommerce -f scripts/loading/load_data_psql.sql
        ```

*   **MongoDB:**
    *   **Create collections and indexes:** Run the `load_data_mongodb.js` script using `mongosh`:
        ```bash
        mongosh --file scripts/loading/load_data_mongodb.js
        ```
    *   **Load data:**  Use `mongoimport` to load the data from the JSON files.  On Windows, you can use the provided batch script:
        ```bash
        .\scripts\loading\load_data_mongodb.bat
        ```
        On Linux/macOS, you would typically use a similar shell script (you might need to adapt the script for your specific shell). An example shell script is included (`load_data_mongodb.bash`), but adjust paths if necessary:
        
        ```bash
        bash ./scripts/loading/load_data_mongodb.bash  #  (Optional - if you have a bash script)
        ```

*   **Neo4j:**
     * **Start Neo4j Server:**  Start the Neo4j server.  The method depends on your setup (Desktop, Community, Enterprise). On Windows, you can often use `neo4j.bat start` from the Neo4j `bin` directory.
    *   **Create Constraints and Indexes:** Run the `load_data_neo4j.cypher` script to create constraints and indexes.  This is best done *before* loading the data for optimal performance.
        ```bash
        cat scripts/loading/load_data_neo4j.cypher | cypher-shell
        ```
    * **Stop Neo4j Server:** Neo4j must be stopped before you can load the database using `neo4j-admin database import`.
    *  **Load Data (neo4j-admin):**  Use the `neo4j-admin database import` command to load the data from the CSV files.  On Windows, you can use the provided batch script:

        ```bash
        .\scripts\loading\load_data_neo4j.bat
        ```
     * **Start Neo4j Server:** Start the Neo4j server after loading the data.

**9. Data Analysis:**

The script `scripts/analysis/data_analysis.py` contains Python code to connect to PSQL database and execute the query # 1 analysis.

To run the analysis for Task 1 (Campaign Effectiveness):

```bash
uv run python scripts/analysis/data_analysis.py
```

The script will connect to each database, execute the corresponding query (currently only `q1.sql` is fully functional), and print the results.
