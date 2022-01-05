package come.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

/**
 * create by pengchuan.chen on 2021/9/8
 */
public class SparkJdbcTest {
    public static JavaSparkContext sc;

    public static SparkSession spark;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkJdbcTest").setMaster("local[*]");
        spark = SparkSession.builder().config(conf).getOrCreate();
        sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JdbcDialects.registerDialect(new HiveDialect());

        DataFrameReader jdbcReader = spark.read().format("jdbc")
                .option("driver", "org.apache.hive.jdbc.HiveDriver")
                .option("url", "jdbc:hive2://192.168.1.81:10000/default")
                .option("user", "hive").option("password", "hive")
                .option("fetchsize","10")
                .option("dbtable", "hive_student");
        Dataset<Row> df = jdbcReader.load();
//        System.out.println(df.count());
        df.show();
    }
}

class HiveDialect extends JdbcDialect {

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:hive2");
    }

    @Override
    public String quoteIdentifier(String colName) {
        return "`" + colName + "`";
    }

    @Override
    public String getTableExistsQuery(String table) {
        return "SELECT 1 FROM " + table + " LIMIT 1";
    }
}
