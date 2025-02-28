# sparkit


The `sparkit` repository includes a Python package named `sparkit` designed for 
building and managing Spark jobs efficiently. The package provides essential
utilities for developing, configuring, and running distributed data processing 
workflows with Apache Spark.

## `main.py`

The `main.py` script serves as the main entry point for submitting Spark 
applications. Users can invoke this script using `spark-submit` to execute 
the jobs defined inside the `jobs` directory.

### Example Usage:
```bash
spark-submit main.py --job <job_name> --job-args key1=value key2=value2
```

- `--job`: Specifies the name of the job to execute.
- `--jobs-args`: (Optional) Job arguments as `key=value`

## `jobs/`

The `jobs` directory contains the job definitions. Users can extend this 
directory to include their own custom jobs, which can then be executed
through `main.py`.

### Adding a Custom Job:
1. Create a new Python file in `jobs/` (e.g., `my_custom_job.py`).
2. Define your job logic in the file. It must expose a `run` function.


### Example Structure:
```plaintext
src/
  jobs/
    my_custom_job.py
```

### Sample Job Implementation (`my_custom_job.py`):
```python
from pyspark.sql import SparkSession

def run(print_msg: str):
    spark = SparkSession.builder.appName("My Custom Job").getOrCreate()
    print(f"{print_msg}")
```

Run it with

```bash
spark-submit main.py --job my_custom_job --job-args print_msg="Hello, World!"
```