在idea中直接运行，或者打成jar放到spark所在机器某个目录，在该目录中执行

/app/spark2.2/bin/spark-submit --class come.hadoop.spark.WordCount spark-1.0.jar inputPath outputPath

/app/spark2.2 ：为我的spark的安装目录
come.hadoop.spark.WordCount ：主类名称
spark-1.0.jar ：jar包名称
inputPath ：输入文件路径
outputPath ：输出结果路径