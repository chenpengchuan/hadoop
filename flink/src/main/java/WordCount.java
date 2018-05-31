import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class WordCount {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements("hello test","hello world hello flink");
        DataSet<Tuple2<String,Integer>> wordcounts = text.flatMap(new LineSplitter())
                .groupBy(0).sum(1);
        wordcounts.print();
        env.execute("word count example");
    }

    public static class LineSplitter implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            for(String world:line.split(" ")){
                out.collect(new Tuple2<String,Integer>(world,1));
            }
        }
    }
}
