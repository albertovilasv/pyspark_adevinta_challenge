# PySpark Adevinta Challenge

This document describes how to execute the code and the tests

The development of this execise has been develop using docker. The image used is: jupyter/pyspark-notebook

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

Pending

## Things to improve

1. Create utils folder to organise the functions of the main.py
2. Create unit test for all the functions
3. 