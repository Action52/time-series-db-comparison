# kdb FinTime Benchmarking
* * *
## Requirements

- KDB+. Download from https://kx.com/developers/download-licenses/ . Please be aware that in order to run kdb, 
you need to register and get a license and locate it properly on your machine. 
Follow https://code.kx.com/q/learn/install/ in order to install.
- conda or miniconda
- qpython
- pandas
- If you want to generate your own data, please follow the instructions at 
https://cs.nyu.edu/~shasha/fintime.d/gen.html for tickgen data.

## How to run the benchmark

- Clone the repository to your local machine.
- Install dependencies on a clean python environment
```shell
conda create --name kdb-benchmark --no-default-packages python=3.9
conda activate kdb-benchmark
pip install pandas
pip install qpython
```

- First, on a separate terminal window, run q with access to a port:
```shell
q -p 5000
h:hopen `:localhost:5000
```

- Now that we have q listening on port 5000, on the main terminal window, let's run the benchmark.
```shell
python run_kdb_queries.py
  --scale-factor <SCALE_FACTOR> \
  --ticks-file-path <TICKS_FILE_PATH> \
  --base-file-path <BASE_FILE_PATH> \
  --queries-folder-path <QUERIES_FOLDER_PATH> \
  --results-folder-path <RESULTS_FOLDER_PATH> \
  --users-emulate <NUMBER_OF_USERS_TO_EMULATE> \
  --system-cost <SYSTEM_COST> \
  --logging-level <LOGGING_LEVEL_IF_NEEDED> \
  --port <PORT_FOR_Q> \
  --display-results
```
This script will execute the fintime benchmark and append the results to the selected folder.