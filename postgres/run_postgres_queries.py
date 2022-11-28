import pandas as pd
from glob import glob
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
import traceback
import time
import logging
from argparse import ArgumentParser
import psycopg as ps
import pandas.io.sql as pd_sql
import warnings
warnings.filterwarnings("ignore")


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--scale-factor", "-s", choices=[1000, 10000, 100000], default=1000, type=int,
                        help="Scale factor to select, between 1000, 10000, or 100000 securities.")
    parser.add_argument("--ticks-file-path", "-t", required=True,
                        help="Full path to the ticks file to run the benchmark.")
    parser.add_argument("--base-file-path", "-b", required=True,
                        help="Full path to the base file to run the benchmark.")
    parser.add_argument("--queries-file-path", "-q", default="./queries.sql",
                        help="Full path to the file where the postgres queries are located.")
    parser.add_argument("--results-folder-path", "-r", required=True,
                        help="Full path to the folder where you wish to store the benchmark and query results.")
    parser.add_argument("--users-emulate", "-e", default=3, choices=[3, 5, 10], type=int)
    parser.add_argument("--system-cost", "-c", default=100, type=int,
                        help="Total cost in dollars by month for the system.")
    parser.add_argument("--logging-level", "-l", default="INFO")
    parser.add_argument("--user", "-u", default="postgres")
    parser.add_argument("--port", "-p", default=5432, type=int)
    parser.add_argument("--pswd", "-P", default=None)
    parser.add_argument("--host", "-H", default="localhost")
    parser.add_argument("--db-name", "-d", default="postgres")
    parser.add_argument("--display-results", action="store_true", default=False)
    args = parser.parse_args()
    return args


def create_connection(args):
    try:
        if args.pswd is None:
            conn = ps.connect(f"host={args.host} dbname={args.db_name} user={args.user} port={args.port}",
                              autocommit=True)
        else:
            conn = ps.connect(
                f"host={args.host} dbname={args.db_name} user={args.user} port={args.port} password={args.pswd}",
                autocommit=True
            )
    finally:
        if conn:
            return conn


def close_connection(conn):
    conn.close()


def save_multi_user_stats(url, data):
    logging.debug("save multi user stats")
    logging.debug(data)
    try:
        prev = pd.read_csv(f"{url}_multi.csv")
        newdf = pd.concat([prev, data]).round(4)
    except Exception as e:
        newdf = data.copy(deep=True).round(4)
    newdf.to_csv(f"{url}_multi.csv", mode="w")


def save_list_results(url, data):
    logging.debug("save_list_results")
    df = data.copy(deep=True).round(4)
    df.to_csv(url, index=False)


def save_stats(url, data):
    logging.debug("save_stats")
    df = pd.DataFrame(data).round(4)
    df.to_csv(url, index=False)


def create_database(conn: ps.Connection):
    conn.execute("""
        DROP TABLE IF EXISTS ticks;
        DROP TABLE IF EXISTS base;
        CREATE TABLE IF NOT EXISTS base(
          Id    varchar(40) PRIMARY KEY NOT NULL,
          Ex    varchar(5),
          Descr varchar(260),
          SIC   varchar(30),
          Cu    varchar(30)
        );
        CREATE TABLE IF NOT EXISTS ticks(
          TickID      serial PRIMARY KEY,
          Id          varchar(40) NOT NULL ,
          SeqNo       bigint,
          TradeDate   date,
          Ts          time,
          TradePrice  float,
          TradeSize   float,
          AskPrice    float,
          AskSize     float,
          BidPrice    float,
          BidSize     float,
          Type_       char(3),
          CONSTRAINT fk_security_id
            FOREIGN KEY(Id) REFERENCES base(Id)
        );
    """)


def load_database(conn, tick_base_file, tick_price_file):
    conn.execute(f"""
        COPY base(Id, Ex, Descr, SIC, Cu)
        FROM '{tick_base_file}' DELIMITER '|' CSV HEADER;
        """)
    conn.execute(f"""
        COPY ticks(Id, SeqNo, TradeDate, Ts, TradePrice, TradeSize, AskPrice, AskSize, BidPrice, BidSize, Type_)
        FROM '{tick_price_file}'
        DELIMITER '|' CSV HEADER;
        """)


def load_queries(path_to_queries) -> dict:
    with open(path_to_queries, 'r') as file_input:
        queries_joined = "".join(file_input.readlines())
        queries_stripped = queries_joined.strip("\n")
        queries = {f"query_{query_id+1}": query for query_id, query in enumerate(queries_stripped.split(";"))}
        del queries["query_7"]
    return queries


def run_query(conn, run_id, query_number, queries, path_to_save_results, data_size, print_result=False, save_result=True,
              query_id=""):
    logging.debug(f"Running query {query_number} for scale factor {data_size}, saving results at {path_to_save_results}")
    try:
        start = time.time()
        df = pd_sql.read_sql(queries[query_id], conn)
        end = time.time()
        count = df.shape[0]
        df = df.round(4)
        logging.debug(df)
        if save_result:
            df.to_csv(f"{path_to_save_results}/result_{query_id}_{data_size}.csv", index=False)
        stats = {
            "run_id": f"{run_id}_{query_id}",
            "query_id": query_number,
            "start_time": start,
            "end_time": end,
            "elapsed_time": end-start,
            "row_count": count,
            'error': False
        }
        return stats
    except Exception:
        logging.debug(queries[query_id])
        logging.debug(traceback.format_exc())
        return {
            "run_id": f"{run_id}_{query_id}",
            "query_id": query_number,
            "start_time": time.time(),
            "end_time": time.time(),
            "elapsed_time": 0.0,
            "row_count": 0,
            "error": True
        }


def run_queries_iter(conn, run_id, queries, path_to_save_results, path_to_save_stats, data_size, print_result=False,
                     save_result=True, args=None):
    if conn is None:
        conn = create_connection(args)
    stats = []
    i = 0
    for query_id, query in queries.items():
        stat = run_query(conn, run_id, i + 1, queries, path_to_save_results, data_size, print_result,
                         save_result, query_id=query_id)
        stats.append(stat)
        logging.debug(stat)
        i+=1
    #print(stats)
    close_connection(conn)
    return pd.DataFrame.from_dict(stats).round(4)


def run_queries_multi_user(run_id, queries, path_to_save_results, path_to_save_stats, data_size, print_result=False,
                           args=None, query_id=""):
    answers = []
    with ThreadPoolExecutor(args.users_emulate) as executor:
        futures = [
            executor.submit(
                run_queries_iter, None, f"{run_id}_{query_id}", queries, path_to_save_results, path_to_save_stats, data_size,
                print_result, False, args
            )
            for i in range(args.users_emulate)
        ]
        wait(futures)
        for future in futures:
            answer = future.result()
            logging.debug(answer)
            answers.append(answer)
        executor.shutdown()
    answer_df = pd.concat(answers).round(4)
    throughput = answer_df.groupby("run_id").sum()['elapsed_time'].mean()
    save_multi_user_stats(path_to_save_stats, answer_df)
    return throughput


def run(data_sizes, conn, args):
    for i, data_size in enumerate(data_sizes):
        queries_path = args.queries_file_path
        stats_path = f"{args.results_folder_path}/run_stats_size_{data_size}.csv"
        start_create_db = time.time()
        # Create metastore for the given size
        create_database(conn)
        load_database(conn, tick_price_file=args.ticks_file_path, tick_base_file=args.base_file_path)
        end_create_db = time.time()
        queries = load_queries(args.queries_file_path)

        # Load queries for the given size
        queries = load_queries(queries_path)
        # Run once as single user to get response time metric
        start_run = time.time()
        stats_ind = run_queries_iter(conn, f"{data_size}_single_user", queries, args.results_folder_path, stats_path,
                                     data_size, save_result=args.display_results, args=args)
        end_run = time.time()
        response_t_single = end_run - start_run
        save_list_results(stats_path, stats_ind)
        df = pd.read_csv(stats_path).round(4)
        response_t_multi = end_run - start_run

        # Run a second time emulating a multi-user system to get throughput
        start_run = time.time()
        throughput = run_queries_multi_user(f"{data_size}_multi_user", queries, args.results_folder_path, stats_path,
                                            data_size, args=args)
        end_run = time.time()

        # Saving the overall stats to csv file
        overall_stats = [{
            'batch_id': i + 1,
            'create_db_time': end_create_db - start_create_db,
            'run_query_time_single': end_run - start_run,
            'response_time_metric_multi': response_t_multi,
            'thorughput_metric': throughput,
            'cost_metric': response_t_single*throughput/args.system_cost
        }]
        print(overall_stats[0])
        overall_stats_path = f"{args.results_folder_path}/{data_size}_overall_stats.csv"
        save_stats(overall_stats_path, overall_stats)


def main():
    args = parse_args()
    conn = create_connection(args)
    run(data_sizes=[args.scale_factor], conn=conn, args=args)
    close_connection(conn)


if __name__ == "__main__":
    main()



