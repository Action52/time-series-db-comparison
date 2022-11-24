import pandas as pd
from glob import glob
from qpython import qconnection
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
import traceback
import time
import logging
from argparse import ArgumentParser


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--scale-factor", "-s", choices=[1000, 10000, 100000], default=1000, type=int,
                        help="Scale factor to select, between 1000, 10000, or 100000 securities.")
    parser.add_argument("--ticks-file-path", "-t", required=True,
                        help="Full path to the ticks file to run the benchmark.")
    parser.add_argument("--base-file-path", "-b", required=True,
                        help="Full path to the base file to run the benchmark.")
    parser.add_argument("--queries-folder-path", "-q", default="./queries/",
                        help="Full path to the folder where the kdb queries are located.")
    parser.add_argument("--results-folder-path", "-r", required=True,
                        help="Full path to the folder where you wish to store the benchmark and query results.")
    parser.add_argument("--users-emulate", "-u", default=3, choices=[3, 5, 10], type=int)
    parser.add_argument("--system-cost", "-c", default=100, type=int,
                        help="Total cost in dollars by month for the system.")
    parser.add_argument("--logging-level", "-l", default="INFO")
    parser.add_argument("--port", "-p", default=5050, type=int)
    parser.add_argument("--display-results", "-d", action="store_true", default=False)
    args = parser.parse_args()
    return args


def create_connection(pandas=False, port=5050):
    q = qconnection.QConnection(host='localhost', port=port, pandas=pandas)
    q.open()
    return q


def close_connection(q):
    q.close()


def create_database(q, tick_csv_path, tickenum_folder_path, base_csv_path, baseenum_folder_path):
    print('IPC version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))
    # Load price tick file
    q.sendSync(f'tick:("SIDTFIFIFIS"; enlist"|")0:`:{tick_csv_path}')
    # Create enumeration for table (this is required to create a splayed table and then a partitioned table)
    q.sendSync(f'tickenum: .Q.en[`:{tickenum_folder_path}] tick')
    # Save table
    q.sendSync('rsave `tickenum')

    # Load base tick file
    q.sendSync(f'base:("SSSSS"; enlist"|")0:`:{base_csv_path}')
    # Create enumeration for table (this is required to create a splayed table and then a partitioned table)
    q.sendSync(f'baseenum: .Q.en[`:{baseenum_folder_path}] base')
    # Save table
    q.sendSync('rsave `baseenum')

    q.sendSync('baseenum2:get `baseenum ')
    q.sendSync('priceenum2:get `tickenum')
    q.sendSync('secs: select[101] Id from baseenum2')
    q.sendSync('securities: (exec Id from secs)')


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


def load_queries(path_to_queries) -> dict:
    queries=dict()
    for file in glob(path_to_queries+'*.q'):
        with open(file, 'r') as file_input:
            data = file_input.read().replace('\n', ';')
            queries[str(file)] = data
    return queries


def run_query(q, run_id, query_number, queries, path_to_save_results, data_size, print_result=False, save_result=True,
              query_id=""):
    logging.debug(f"Running query {query_number} for scale factor {data_size}, saving results at {path_to_save_results}")
    try:
        start = time.time()
        #temp = np.array(q(queries[query_number-1], qtype=1, adjust_dtype=False))
        # logging.debug(queries[query_number-1], temp)
        #print(f"{run_id}_{query_number} -> {queries[query_number-1]}")
        #print(q.sendSync(queries[query_number-1]))
        result = q.sendSync(queries[query_id])
        end = time.time()
        df = pd.DataFrame(result)
        try:
            str_df = df.select_dtypes([object])
            str_df = str_df.stack().str.decode('utf-8').unstack()
            for col in str_df:
                df[col] = str_df[col]
        except Exception as e:
            logging.debug(e)
        # df = pd.DataFrame(data=q(queries[query_number-1], qtype=1, adjust_dtype=False))
        count = df.shape[0]
        df = df.round(4)
        logging.debug(df)
        if save_result:
            df.to_csv(f"{path_to_save_results}/result_Q{query_id}_{data_size}.csv", index=False)
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


def run_queries_multi_user(q, run_id, queries, path_to_save_results, path_to_save_stats, data_size, print_result=False,
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


def run_queries_iter(q, run_id, queries, path_to_save_results, path_to_save_stats, data_size, print_result=False,
                     save_result=True, args=None):
    if q is None:
        q = create_connection(port=args.port)
    stats = []
    i = 0
    for query_id, query in queries.items():
        stat = run_query(q, run_id, i + 1, queries, path_to_save_results, data_size, print_result,
                         save_result, query_id=query_id)
        stats.append(stat)
        logging.debug(stat)
        i+=1
    #print(stats)
    close_connection(q)
    return pd.DataFrame.from_dict(stats).round(4)


def run(data_sizes, q, args):
    for i, data_size in enumerate(data_sizes):
        queries_path = args.queries_folder_path
        stats_path = f"{args.results_folder_path}/run_stats_size_{data_size}.csv"
        start_create_db = time.time()

        # Create metastore for the given size
        create_database(q,
                        tick_csv_path=args.ticks_file_path, tickenum_folder_path=args.results_folder_path,
                        base_csv_path=args.base_file_path,  baseenum_folder_path=args.results_folder_path)
        end_create_db = time.time()

        # Load queries for the given size
        queries = load_queries(queries_path)
        # Run once as single user to get response time metric
        start_run = time.time()
        stats_ind = run_queries_iter(q, f"{data_size}_single_user", queries, args.results_folder_path, stats_path,
                                     data_size, save_result=False)
        end_run = time.time()
        response_t_single = end_run - start_run
        save_list_results(stats_path, stats_ind)
        df = pd.read_csv(stats_path).round(4)
        response_t_multi = end_run - start_run

        # Run a second time emulating a multi-user system to get throughput
        start_run = time.time()
        throughput = run_queries_multi_user(q, f"{data_size}_multi_user", queries, args.results_folder_path, stats_path, data_size,
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
        print(overall_stats[0])
        overall_stats_path = f"{args.results_folder_path}/{data_size}_overall_stats.csv"
        save_stats(overall_stats_path, overall_stats)


def main():
    args = parse_args()
    logging.basicConfig(filename=f'{args.results_folder_path}/logging.log', encoding='utf-8', level=args.logging_level)
    q = create_connection(port=args.port)
    print(f"Running findata benchmark with security size {args.scale_factor}")
    create_database(q,
                    tick_csv_path=args.ticks_file_path, tickenum_folder_path=args.results_folder_path,
                    base_csv_path=args.base_file_path, baseenum_folder_path=args.results_folder_path)
    run(data_sizes=[args.scale_factor], q=q, args=args)
    close_connection(q=q)


if __name__ == "__main__":
    main()
