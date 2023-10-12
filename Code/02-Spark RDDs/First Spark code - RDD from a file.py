# Databricks notebook source
from pyspark import SparkConf, SparkContext
import os

# Get the current script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Navigate to the parent folder
parent_dir = os.path.dirname(script_dir)

# Navigate to the file
file_path = os.path.join(parent_dir, "Sample.txt")

# COMMAND ----------

conf = SparkConf().setAppName("Read sample file")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

sample = sc.textFile(file_path)

# COMMAND ----------

print(sample.collect())
