#!/usr/bin/env bash
spark-submit\
 --master "local[*]"\
 --driver-memory 1G\
 --executor-memory 8G\
 --jars ../../../target/spark-gdb-0.2.jar\
 udtapp.py
