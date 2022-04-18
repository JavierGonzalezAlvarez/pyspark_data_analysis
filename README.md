# etl pipeline pyspark

## install pypsark
https://spark.apache.org/docs/latest/api/python/getting_started/install.html
$ pip install pyspark
$ pip install findspark 

# settigs environment varibales
$ pip show pyspark

$ export SPARK_HOME=/Users/prabha/apps/spark-2.4.0-bin-hadoop2.7
$ export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH

# Spark SQL
$ pip install pyspark[sql]

## run pyspark
$ python3 new_column.py
or (no REPL)
$ spark-submit read_book.py

# Datetime partterns for formatting and parsing in Spark 
https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
