In [1]: df = sqlContext.read.parquet("/home/diffstand/Downloads/x1.parquet")
   x = df.select("userAnalytics.ip2locCountryCode","ads.clicked")
   ...: from pyspark.sql.functions import explode,concat,col,lit,udf
   ...: x1=x.select(x.ip2locCountryCode,explode("clicked").alias("eclicked"))
   ...: x1.show()
   ...: from pyspark.sql.functions import when, col
   ...: x2 = x1.withColumn("z",when(col("eclicked").isNull(),1).otherwise(0))
   ...: x3 = x2.withColumn("z2",when(col("eclicked").isNull(),0).otherwise(1))
   ...: x3.show()
   ...: from pyspark.sql.functions import expr
   ...: from pyspark.sql.functions import desc
   ...: x4 =x3.groupby("ip2locCountryCode").agg(expr("sum(z)"),expr("sum(z2)")).
   ...: sort(desc("sum(z2)"))
   ...: x5 =x4.withColumn("ctr",x4['sum(z2)']/(x4['sum(z)']+x4['sum(z2)']))
   ...: x5.show()
   ...: 
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
17/08/20 14:08:00 WARN Utils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf.
17/08/20 14:08:02 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
+-----------------+--------+
|ip2locCountryCode|eclicked|
+-----------------+--------+
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
|               US|    null|
+-----------------+--------+
only showing top 20 rows

17/08/20 14:08:03 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
+-----------------+--------+---+---+
|ip2locCountryCode|eclicked|  z| z2|
+-----------------+--------+---+---+
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
|               US|    null|  1|  0|
+-----------------+--------+---+---+
only showing top 20 rows

17/08/20 14:08:04 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
17/08/20 14:08:04 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
17/08/20 14:08:04 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
17/08/20 14:08:04 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
[Stage 3:>                                                          (0 + 4) / 4]17/08/20 14:08:06 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
17/08/20 14:08:06 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
17/08/20 14:08:06 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
17/08/20 14:08:06 WARN ParquetRecordReader: Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
+-----------------+------+-------+--------------------+                         
|ip2locCountryCode|sum(z)|sum(z2)|                 ctr|
+-----------------+------+-------+--------------------+
|               US|325242|    203|6.237613114351119E-4|
|               GB| 31835|     23| 7.21953669407998E-4|
|               CA| 41836|     22|5.255865067609537E-4|
|               IN|  5465|     12|0.002190980463757...|
|               AU| 21326|      9|4.218420435903445E-4|
|               ZA|  6887|      6|8.704482808646452E-4|
|               IE|  6619|      4|6.039559112184811E-4|
|             null|  3596|      3|8.335648791330926E-4|
|               IT|  1951|      2|0.001024065540194...|
|               MK|   120|      2| 0.01639344262295082|
|               GR|  1531|      2|0.001304631441617743|
|               NZ|  4108|      2|4.866180048661800...|
|               TR|  1241|      2|0.001609010458567...|
|               HR|  2413|      2|8.281573498964803E-4|
|               NL|  6180|      1|1.617861187510111...|
|               HK|   670|      1|0.001490312965722...|
|               ES|  2127|      1|4.699248120300751...|
|               IL|   172|      1|0.005780346820809248|
|               RS|   423|      1|0.002358490566037...|
|               ID|   595|      1|0.001677852348993...|
+-----------------+------+-------+--------------------+
only showing top 20 rows
