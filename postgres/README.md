# Postgres FinTime Benchmarking
* * *
## Requirements

- Postgresql. If you have homebrew, you can easily install psql 14 with
```shell
brew install postgresql@14
```
- conda or miniconda
- pandas
- psycopg
- If you want to generate your own data, please follow the instructions at 
https://cs.nyu.edu/~shasha/fintime.d/gen.html for tickgen data.

## How to run the benchmark

- Clone the repository to your local machine.
- Install dependencies on a clean python environment
```shell
conda create --name psql-benchmark --no-default-packages python=3.9
conda activate psql-benchmark
pip install pandas
pip install psycopg
```

- On the main terminal window, let's run the benchmark.
```shell
python run_postgres_queries.py
  --scale-factor <SCALE_FACTOR> \
  --ticks-file-path <TICKS_FILE_PATH> \
  --base-file-path <BASE_FILE_PATH> \
  --queries-file-path <QUERIES_FILE_PATH> \
  --results-folder-path <RESULTS_FOLDER_PATH> \
  --users-emulate <NUMBER_OF_USERS_TO_EMULATE> \
  --system-cost <SYSTEM_COST> \
  --logging-level <LOGGING_LEVEL> \
  --user <PSQL_USER> \
  --port <PSQL_PORT> \
  --pswd <PSQL_PSWD> \
  --host <PSQL_HOST> \
  --db-name <PSQL_DB_NAME> \
  --display-results

```
This script will execute the fintime benchmark and append the results to the selected folder.