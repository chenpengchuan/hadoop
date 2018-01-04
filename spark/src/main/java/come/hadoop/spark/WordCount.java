package come.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            args=new String[2];
            args[0]=HdfsFile.in_put_Path;
            args[1]=HdfsFile.out_put_Path;
            System.out.println("input path:"+args[0]);
            System.out.println("output path:"+args[1]);
        }
        SparkConf conf = new SparkConf()
                .setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建初始RDD
        JavaRDD<String> lines = sc.textFile(args[0]);
        //每一行都会触发执行一次
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
//               return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        try {
            HdfsFile.saveAsHdsfFile(counts,args[1]);
        } catch (IOException e) {
            e.printStackTrace();
        }
        sc.stop();
    }
}
