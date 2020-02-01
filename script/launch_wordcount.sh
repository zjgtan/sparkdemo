zip -r src.zip src

spark-submit \
    --master yarn \
    --num-executors 10 \
    --executor-cores 5 \
    --executor-memory 10g \
    --queue root.xxx \
    --py-file src.zip \
    src/wordcount.py
