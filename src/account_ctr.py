# coding: utf8

from pyspark.sql import HiveContext, SparkSession
outPath = "/user/chenjiawei/test/account_ctr"

sparkSession = SparkSession\
        .builder\
        .enableHiveSupport()\
        .appName("account_ctr")\
        .getOrCreate()

accountCtrDF = sparkSession.sql(
        """
        select 
            user_id, sum(click) / sum(show) as ctr
        from xxx.
        where log_date=20200131
        group by
            user_id
        """)

accountBasicDF = sparkSession.sql("select * from ods.ad_acc_account")

accountCtrDF = accountCtrDF\
        .join(accountBasicDF, 
                accountCtrDF["user_id"] == accountBasicDF["account_id"], how = "left_outer")\
        .select(accountCtrDF["user_id"], accountBasicDF["username"], accountCtrDF["ctr"])\
        .repartition(2, "user_id")

accountCtrDF.write\
        .csv(path=outPath, sep="\t", compression="gzip")
