import pandas as pd
import traceback
import time
import logging

from glob import glob
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from argparse import ArgumentParser
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Influx config
org = 'ulb'
token = '9ohN0dz-g1lH-d1emYInZjBqq0L2R5MJzRSWZ_9ezo5-U2N384Yjy5m7DNOdILieglyCYkpQWoOw5R4_FHzJlg=='
host = 'http://localhost:8086'
bucket = 'advdb'
timeout = 60000 # set to 1 minutes

# Delete var
start = "2022-01-01T00:00:00Z"
stop = "2022-12-12T00:00:00Z"


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--scale-factor", "-s", choices=[1000, 10000, 100000], default=1000, type=int,
                        help="Scale factor to select, between 1000, 10000, or 100000 securities.")
    parser.add_argument("--file-path", "-f", required=True,
                        help="Full path to the ticks file to run the benchmark.")
    parser.add_argument("--queries-folder-path", "-q", default="./queries/",
                        help="Full path to the folder where the kdb queries are located.")
    parser.add_argument("--results-folder-path", "-r", required=True,
                        help="Full path to the folder where you wish to store the benchmark and query results.")
    parser.add_argument("--system-cost", "-c", default=100, type=int,
                        help="Total cost in dollars by month for the system.")  
    parser.add_argument("--users-emulate", "-u", default=3, choices=[3, 5, 10], type=int)
    parser.add_argument("--logging-level", "-l", default="INFO")
    parser.add_argument("--display-results", "-d", action="store_true", default=False)
    args = parser.parse_args()
    return args

def create_connection(host, token, org):
    client = InfluxDBClient(url=host, token=token, org=org, timeout=timeout)
    return client

def close_connection(client):
    client.close()


def create_database(tick_db_path):
    with InfluxDBClient(url=host, token=token, org=org, timeout=timeout) as client:
        client.delete_api().delete(start, stop, '_measurement="findata"', bucket='advdb', org='ulb')
        data = read_file_data(tick_db_path)
        
        with client.write_api(batch_size=10000, write_options=SYNCHRONOUS) as write_client:
            write_client.write("advdb", "ulb", data, write_precision="ns")
            

def read_file_data(tick_db_path):
    data = []
    with open(tick_db_path, 'r') as f:
        data = f.read().split("\n")
    return data
        

def save_list_results(url, data):
    logging.debug("save_list_results")
    df = data.copy(deep=True).round(4)
    df.to_csv(url, index=False)


def save_multi_user_stats(url, data):
    logging.debug("save multi user stats")
    logging.debug(data)
    try:
        prev = pd.read_csv(f"{url}_multi.csv")
        newdf = pd.concat([prev, data]).round(4)
    except Exception as e:
        newdf = data.copy(deep=True).round(4)
    newdf.to_csv(f"{url}_multi.csv", mode="w")


def save_stats(url, data):
    logging.debug("save_stats")
    df = pd.DataFrame(data).round(4)
    df.to_csv(url, index=False)


def load_queries(path_to_queries):
    queries=dict()

    for file in glob(path_to_queries+'*.flux'):
        with open(file, 'r') as file_input:
            data = file_input.read()
            queries[str(file)] = data
            
    return queries


def run_query(client, run_id, query_number, queries, 
                path_to_save_results, data_size, save_result=True,
                query_id=""):
    print(f"Running query {query_id} for scale factor {data_size}, saving results at {path_to_save_results}")
    try:
        start = time.time()
        res = client.query_api().query(queries[query_id])
        end = time.time()
        
        df = pd.read_json(res.to_json())
        count = df.shape[0]
        df = df.round(4)
        
        if save_result:
            q_id = query_id.replace("./", "_").replace("/", "_")
            df.to_csv(f"{path_to_save_results}/result_Q{q_id}_{data_size}.csv", index=False)
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
  
def run_queries_iter(run_id, queries, path_to_save_results, data_size, save_result=True):
    with InfluxDBClient(url=host, token=token, org=org, timeout=timeout) as client: 
        stats = []
        i = 0
        for query_id, _ in queries.items():
            stat = run_query(client, run_id, i + 1, queries, path_to_save_results, data_size, save_result, query_id=query_id)
            stats.append(stat)
            logging.debug(stat)
            i+=1
        return pd.DataFrame.from_dict(stats).round(4)

def run_queries_multi_user(run_id, queries, path_to_save_results, path_to_save_stats, data_size, args=None, query_id=""):
    print("Running multi user on {}".format(query_id))
    answers = []
    with ThreadPoolExecutor(args.users_emulate) as executor:
        futures = [
            executor.submit(
                run_queries_iter, f"{run_id}_{query_id}", queries, path_to_save_results, data_size,
                True
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

def run(data_sizes, args):
    for i, data_size in enumerate(data_sizes):
        queries_path = args.queries_folder_path
        stats_path = f"{args.results_folder_path}/run_stats_size_{data_size}.csv"
        
        start_create_db = time.time()
        # Create metastore for the given size
        end_create_db = time.time()

        # Load queries for the given size
        queries = load_queries(queries_path)
        # Run once as single user to get response time metric
        start_run = time.time()
        stats_ind = run_queries_iter(f"{data_size}_single_user", queries, args.results_folder_path, data_size, True)
        end_run = time.time()
        response_t_single = end_run - start_run
        save_list_results(stats_path, stats_ind)
        df = pd.read_csv(stats_path).round(4)
        response_t_multi = end_run - start_run

        # Run a second time emulating a multi-user system to get throughput
        start_run = time.time()
        throughput = run_queries_multi_user(f"{data_size}_multi_user", queries, args.results_folder_path, stats_path, data_size,
                               args=args)
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

        overall_stats_path = f"{args.results_folder_path}/{data_size}_overall_stats.csv"
        save_stats(overall_stats_path, overall_stats)


def main():
    args = parse_args()
    logging.basicConfig(filename=f'{args.results_folder_path}/logging.log', encoding='utf-8', level=args.logging_level)
    print(f"Running findata benchmark with security size {args.scale_factor}")
    run(data_sizes=[args.scale_factor], args=args)
    
if __name__ == "__main__":
    main()
