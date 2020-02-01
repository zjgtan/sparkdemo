# coding: utf8
from pyspark.sql import SparkSession
from src.utils.rdd_to_text_file import rddToTextFile

filePath = "/user/chenjiawei/test/test_data"
outPath = "/user/chenjiawei/test/word_count"

spark = SparkSession\
    .builder\
    .appName("PythonWordCount")\
    .getOrCreate()

lines = spark.read.text(filePath).rdd.map(lambda x: x[0])
counts = lines.flatMap(lambda x: x.split(' '))\
              .map(lambda x: (x, 1))\
              .reduceByKey(lambda a, b: a + b)\
              .map(rddToTextFile)\
              .saveAsTextFile(outPath)
