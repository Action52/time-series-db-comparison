import pyspark
import os
import logging
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from tqdm import tqdm

# Initiate Spark
spark = SparkSession.builder.appName("tpcds-loaddata-testing")\
    .master("spark://spark-master:7077")\
    .config("spark.hadoop.hive.insert.into.external.tables", True)\
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", True)\
    .enableHiveSupport()\
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

relations = ["call_center", "catalog_page", "catalog_returns", "catalog_sales",
             "customer_address", "customer_demographics", "customer", "date_dim",
             "dbgen_version", "household_demographics", "income_band", "inventory", "item",
             "promotion", "reason", "ship_mode", "store_returns", "store_sales", "store",
             "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"
            ]

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
data_dir = "/data_vol_shared/"
sql_dir = "/scripts/queries/table/"
# sql_dir = "{}/queries/table/".format(ROOT_DIR)


logging.basicConfig(filename='/scripts/import_data.log', level=logging.DEBUG)

# Create database by reading from create_db file
def create_database():
    with open("/scripts/queries/create_db.sql") as fr:
        queries = fr.readlines()
    
    for query in queries:
        spark.sql(query)


# Import the data from sql
def import_data():
    print("Creating tables, please wait until completion...")
    for relation in tqdm(relations):
        logging.debug(f"Processing {relation}")
        filepath = "{}{}.sql".format(sql_dir, relation)
        
        # Read queries file
        with open(filepath) as fr:
            datafile_dir = data_dir + relation
            queries = fr.read().strip("\n")\
                .replace("${path}", datafile_dir)\
                .replace("${name}", f"{relation}")\
                .split(";")
        
        for query in queries:
            if query != "":
                #logging.debug(query)
                spark.sql("USE tpcds;")
                spark.sql(query)
    print("Done.")

create_database()
import_data()
