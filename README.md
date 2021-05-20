# PySpark Adevinta Challenge

This document describes how to execute the code and the tests

The development of this execise has been develop using docker. The image used is: jupyter/pyspark-notebook

## Structure of the repo

The basic project structure is as follows:

```bash
├── jobs
│   ├── data
│   │   └── input.zip
│   └── main.py
├── README.md
└── test
    ├── data
    │   ├── output_test
    │   │   ├── part-00000-dd18760a-37e5-4e57-a0e3-530f2a1e999e-c000.snappy.parquet
    │   │   ├── part-00130-dd18760a-37e5-4e57-a0e3-530f2a1e999e-c000.snappy.parquet
    │   │   ├── part-00299-dd18760a-37e5-4e57-a0e3-530f2a1e999e-c000.snappy.parquet
    │   │   ├── part-00439-dd18760a-37e5-4e57-a0e3-530f2a1e999e-c000.snappy.parquet
    │   │   ├── part-00530-dd18760a-37e5-4e57-a0e3-530f2a1e999e-c000.snappy.parquet
    │   │   ├── part-00566-dd18760a-37e5-4e57-a0e3-530f2a1e999e-c000.snappy.parquet
    │   │   ├── part-00612-dd18760a-37e5-4e57-a0e3-530f2a1e999e-c000.snappy.parquet
    │   │   ├── part-00650-dd18760a-37e5-4e57-a0e3-530f2a1e999e-c000.snappy.parquet
    │   │   ├── part-00723-dd18760a-37e5-4e57-a0e3-530f2a1e999e-c000.snappy.parquet
    │   │   └── _SUCCESS
    │   └── test_data.csv
    └── test.py
```

## Execute the code

1. Start docker container from image: 
```bash
sudo docker run -it -p 8888:8888 jupyter/pyspark-notebook
```
2. Upload to the home path the following files: input.zip and main.py
3. Execute the following command: 
```bash
spark-submit main.py
```

This execution will create a folder with name "output" with all the parquet files

## Execute the tests

1. Start docker container from image: 
```bash
sudo docker run -it -p 8888:8888 jupyter/pyspark-notebook
```
2. Upload to the home path the following files: main.py, test.py, test_data.csv y output_test/
3. Execute the following command: 
```bash
spark-submit test.py
```

This execution validate output_test/ with the data processed from the "test_data.csv".

"test_data.csv" is a subset of 100 rows of data from the original input.zip.


## Things to improve

1. Create utils folder to organise the functions of the main.py
2. Create unit test for all the functions