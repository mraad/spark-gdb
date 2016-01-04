#!/usr/bin/env bash
#  --packages com.vividsolutions:jts:1.14\
spark-submit\
 --master "local[*]"\
 --driver-memory 1G\
 --executor-memory 8G\
 --jars ../../../target/spark-gdb-0.1.jar\
 udtapp.py
