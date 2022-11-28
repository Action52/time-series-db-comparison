"""
This scripts parses the queries sql file, transforms them into pyspark format, and executes them.
"""
from pyspark.sql import SparkSession
from argparse import ArgumentParser
from tqdm import tqdm
import time
import os

def create_spark_session():
    # Initiate Spark
    spark = SparkSession.builder.appName("tpcds-loadqueries-testing") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.hive.insert.into.external.tables", True)\
        .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", True)\
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    return spark


def parse_args():
    """
    Parses arguments to run the queries. Expects the file path for the queries to run.
    :return:
    """
    parser = ArgumentParser()
    parser.add_argument("-q", "--queries-file-path", default="/queries_vol_shared/query_0.sql")
    parser.add_argument("-o", "--output-file-path", default="/queries_vol_shared/all.sql")
    choices = [i for i in range(99)]
    parser.add_argument("-r", "--query-to-run", default=-1, choices=choices, type=int,
                        help="Query to run. To run all select -1, which is default")
    args = parser.parse_args()
    return args


def parse_file(file_obj) -> list:
    """
    Parses the file object to split into queries. Returns a list with the query strings.
    :param file_obj:
    :return:
    """
    comment_count = 0
    queries = []
    query_lines = []
    pbar = tqdm(total=99)
    for line in file_obj:
        if comment_count == 0 and "--" in line:  # it is a comment and therefore the beginning or end of a query
            comment_count += 1
            query_lines = []
            continue
        elif comment_count == 1 and "--" not in line:  # we are reading part of the query
            query_lines.append(line)
        elif comment_count == 1 and "--" in line:  # it is the second comment indicating this is the end of the query
            query = "".join(query_lines)
            queries.append(query)
            pbar.update(1)
            comment_count = 0
    pbar.close()
    return queries


def run_queries(query_number=-1,
                queries_new=[],
                path_to_output="/queries_vol_shared/query_results/",
                statistics_output = "/queries_vol_shared/statistics.csv"
                ):
    """
    :param query_number: -1 to run all. If not, provide query number.
    :param queries_new: List of query strings.
    :return:
    """
    spark = create_spark_session()
    spark.sql("USE tpcds")
    skip_queries = []
    statistics = []
    try:
        os.mkdir(path_to_output)
    except:
        print("Directory exists.")
    if query_number == -1:  # Run all
        print("Running all queries.")
        for i, query in tqdm(enumerate(queries_new)):
            try:
                start_time = time.time()
                result = spark.sql(query)
                row_cnt = result.count()
                result.write.mode("overwrite").option("header", True) \
                    .option("delimiter", "|") \
                    .csv(f"{path_to_output}query_{i}")
                end_time = time.time()
                elapsed_time = end_time - start_time
                statistics.append(
                    {"query": f"Query {i}", "elapsed_time": elapsed_time, "rows": row_cnt}
                )
            except Exception as e:
                print(f"Query {i + 1} failed.")
                print(e)
                skip_queries.append(i + 1)
        print(skip_queries)
        return statistics
    else:  # Run only one query
        print(f"Running query {query_number}...")
        query = queries_new[query_number - 1]
        try:
            start_time = time.time()
            result = spark.sql(query)
            row_cnt = result.count()
            result.write.mode("overwrite").option("header", True) \
                .option("delimiter", "|") \
                .csv(f"{path_to_output}query_{query_number}")
            end_time = time.time()
            elapsed_time = end_time - start_time
            statistics.append(
                {"query": f"Query {query_number}", "elapsed_time": elapsed_time, "rows": row_cnt}
            )
        except:
            print(f"Query {query_number} failed.")
        print("Done.")
        return statistics


def main():
    """
    Main function
    :return:
    """
    args = parse_args()
    print(f"Processing queries from .sql file. ({args.queries_file_path})")
    with open(args.queries_file_path, "r") as queries_file_old:
        queries_new = parse_file(queries_file_old)

    results = run_queries(args.query_to_run, queries_new)
    print(results)


if __name__ == "__main__":
    main()
