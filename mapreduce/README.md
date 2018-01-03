打成jar包（mapreduce-1.0.jar），上传jar包到某个目录，并上传需要统计的文本文件到hdfs对应的 /tmp/stest/spc/wordcount/input 目录；进到jar包所在目录

执行命令如下：

hadoop jar mapreduce-1.0.jar come.hadoop.mapreduce.WordcountDriver  /tmp/stest/spc/wordcount/input /tmp/stest/spc/wordcount/output
