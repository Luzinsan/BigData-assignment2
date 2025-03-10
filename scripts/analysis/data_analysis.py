import platform, psutil
import logging
import time
import statistics
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as pg_connection
from pymongo import MongoClient
from pymongo.database import Database
from neo4j import GraphDatabase, Driver
from neo4j.exceptions import Neo4jError
import configparser
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("analysis.log"), logging.StreamHandler()]
)

class HybridAnalysis:
    def __init__(self, config_path: str = "scripts/analysis/config.ini"):
        self.config = self._load_config(config_path)
        self.pg_conn: Optional[pg_connection] = None
        self.mongo_client: Optional[MongoClient] = None
        self.neo4j_driver: Optional[Driver] = None
        self.performance_data: Dict[str, List[float]] = {
            'postgres': [],
            'mongo': [],
            'neo4j': []
        }
        
        self.log_system_specs()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def _load_config(config_path: str) -> configparser.ConfigParser:
        config = configparser.ConfigParser()
        if not Path(config_path).exists():
            logging.error(f"Config file {config_path} not found")
            raise FileNotFoundError(f"Missing configuration: {config_path}")
        config.read(config_path)
        return config

    def log_system_specs(self):
        """Log hardware and OS specifications"""
        system_info = {
            'OS': platform.system(),
            'OS Version': platform.version(),
            'CPU': platform.processor(),
            'RAM': f"{round(psutil.virtual_memory().total / (1024.**3))} GB",
            'Machine Type': platform.machine(),
            'Python Version': platform.python_version()
        }
        logging.info("System Specifications:\n" + 
                    tabulate(system_info.items(), headers=["Component", "Details"], tablefmt="pretty"))

    def connect(self):
        try:
            # PostgreSQL
            self.pg_conn = psycopg2.connect(**self.config['postgresql'])
            logging.info("PostgreSQL connection established")
            
            # MongoDB
            self.mongo_client = MongoClient(
                self.config['mongodb']['host'],
                serverSelectionTimeoutMS=5000
            )
            self.mongo_db = self.mongo_client[self.config['mongodb']['dbname']]
            logging.info("MongoDB connection established")
            
            # Neo4j
            self.neo4j_driver = GraphDatabase.driver(
                self.config['neo4j']['uri'],
                auth=(self.config['neo4j']['user'], 
                     self.config['neo4j']['password'])
            )
            logging.info("Neo4j connection established")
            
        except Exception as e:
            logging.error(f"Connection failed: {str(e)}")
            self.close()
            raise

    def close(self):
        if self.pg_conn:
            self.pg_conn.close()
            logging.info("PostgreSQL connection closed")
        if self.mongo_client:
            self.mongo_client.close()
            logging.info("MongoDB connection closed")
        if self.neo4j_driver:
            self.neo4j_driver.close()
            logging.info("Neo4j connection closed")

    def _execute_pg_query(self, query: str, params: Dict = None) -> Tuple[List[tuple], float]:
        start_time = time.time()
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute(sql.SQL(query), params or {})
                return cursor.fetchall(), time.time() - start_time
        except Exception as e:
            logging.error(f"PostgreSQL error: {str(e)}")
            return [], 0.0

    def _execute_mongo_aggregation(self, pipeline: List[Dict]) -> Tuple[List[Dict], float]:
        start_time = time.time()
        try:
            result = list(self.mongo_db.messages.aggregate(pipeline))
            return result, time.time() - start_time
        except Exception as e:
            logging.error(f"MongoDB error: {str(e)}")
            return [], 0.0

    def _execute_neo4j_query(self, query: str, params: Dict = None) -> Tuple[List[Dict], float]:
        start_time = time.time()
        try:
            with self.neo4j_driver.session() as session:
                result = session.run(query, params or {})
                return result.data(), time.time() - start_time
        except Neo4jError as e:
            logging.error(f"Neo4j error: {e.message}")
            return [], 0.0

    def run_query_multiple_times(self, db_type: str, *args, **kwargs) -> List[float]:
        """Run query 5 times and collect execution times"""
        times = []
        for i in range(5):
            if db_type == 'postgres':
                _, exec_time = self._execute_pg_query(*args, **kwargs)
            elif db_type == 'mongo':
                _, exec_time = self._execute_mongo_aggregation(*args, **kwargs)
            elif db_type == 'neo4j':
                _, exec_time = self._execute_neo4j_query(*args, **kwargs)
                
            times.append(exec_time)
            logging.info(f"{db_type.capitalize()} run {i+1}: {exec_time:.4f}s")
            time.sleep(0.5)
            
        return times

    def analyze_campaigns(self) -> Dict[str, Any]:
        results = {}
        
        # PostgreSQL
        pg_query = Path("scripts/analysis/q1.sql").read_text()
        pg_times = self.run_query_multiple_times('postgres', pg_query)
        results['postgres'] = self._execute_pg_query(pg_query)[0]
        self.performance_data['postgres'].extend(pg_times)
        
        # MongoDB
        # mongo_pipeline = eval(Path("scripts/analysis/q1.js").read_text())
        # mongo_times = self.run_query_multiple_times('mongo', mongo_pipeline)
        # results['mongo'] = self._execute_mongo_aggregation(mongo_pipeline)[0]
        # self.performance_data['mongo'].extend(mongo_times)
        
        # # Neo4j
        # cypher_query = Path("scripts/analysis/q1.cypher").read_text()
        # neo_times = self.run_query_multiple_times('neo4j', cypher_query)
        # results['neo4j'] = self._execute_neo4j_query(cypher_query)[0]
        # self.performance_data['neo4j'].extend(neo_times)
        
        return results

    def generate_performance_report(self):
        """Calculate statistics and log performance results"""
        report = []
        for db in self.performance_data:
            if not self.performance_data[db]:
                continue
            avg = statistics.mean(self.performance_data[db])
            stdev = statistics.stdev(self.performance_data[db]) if len(self.performance_data[db]) > 1 else 0
            report.append([
                db.capitalize(),
                f"{avg:.4f}s",
                f"{stdev:.4f}s",
                f"{min(self.performance_data[db]):.4f}s",
                f"{max(self.performance_data[db]):.4f}s"
            ])
            
        logging.info("\nPerformance Report:\n" + 
                    tabulate(report, 
                            headers=["Database", "Avg Time", "Std Dev", "Min", "Max"],
                            tablefmt="pretty"))
        logging.info("Individual run times:")
        for db_type, times in self.performance_data.items():
            logging.info(f"{db_type.capitalize()}: {['%.4fs' % t for t in times]}")

if __name__ == "__main__":
    with HybridAnalysis() as analyzer:
        try:
            logging.info("Starting campaign analysis")
            campaign_results = analyzer.analyze_campaigns()
            analyzer.generate_performance_report()
            
            logging.info("\n" + tabulate(campaign_results['postgres'], 
                                  headers=["campaign_id", "campaign_type", "total_messages", 
                                           "clients_with_interaction", "users_purchased", 
                                           "friends_who_also_purchased", "conversion_rate (%)"], 
                                  tablefmt="pretty"))
            
            logging.info("Analysis completed successfully")
            
        except Exception as e:
            logging.error(f"Analysis failed: {str(e)}", exc_info=True)
            raise