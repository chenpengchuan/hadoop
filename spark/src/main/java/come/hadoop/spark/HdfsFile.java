package come.hadoop.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;

public class HdfsFile {
    private static String fs_default_name = "hdfs://node1:8020";
    public static String in_put_Path = fs_default_name + "/tmp/spc/input/";
    public static String out_put_Path = fs_default_name + "/tmp/spc/wordcount/output";

    // 文件系统连接到 hdfs的配置信息
    private static Configuration getConf() {
        Configuration conf = new Configuration();
        // 这句话很关键，这些信息就是hadoop配置文件中的信息
        // conf.set("mapred.job.tracker", "192.168.1.71:9001");
        conf.set("fs.default.name", fs_default_name);
        return conf;
    }

    public static void saveAsHdsfFile(JavaPairRDD<String, Integer> rdd, String path) throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(new Path(path));
        rdd.saveAsTextFile(path);
        fs.close();
    }
}
