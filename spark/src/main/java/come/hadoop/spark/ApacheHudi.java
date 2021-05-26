package come.hadoop.spark;

import org.apache.hudi.QuickstartUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hbase.mapreduce.Import.TABLE_NAME;
import static org.apache.hudi.QuickstartUtils.convertToStringList;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY;
import static org.apache.spark.sql.SaveMode.Overwrite;

/**
 * create by pengchuan.chen on 2021/4/8
 */
public class ApacheHudi {

    private static final String tableName = "hudi_trips_cow";
    private static final String basePath = "file:///tmp/hudi_trips_cow";

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder()
                .appName("ApacheHudi")
                .master("local")
                .getOrCreate();

        QuickstartUtils.DataGenerator dataGen = new QuickstartUtils.DataGenerator();
        List<String> inserts = convertToStringList(dataGen.generateInserts(10));
        Seq<String> insertsSeq = JavaConverters.asScalaIteratorConverter(inserts.iterator()).asScala().toSeq();
        Dataset df = spark.read().json(spark.sparkContext().parallelize(insertsSeq, 2, null));
        df.write().format("hudi").
                options(QuickstartUtils.getQuickstartWriteConfigs()).
                option(org.apache.hudi.DataSourceWriteOptions.OPERATION_OPT_KEY(),"").
//                option(PRECOMBINE_FIELD_OPT_KEY, "ts").
        option(org.apache.hudi.DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "ts").
                option(RECORDKEY_FIELD_OPT_KEY, "uuid").
                option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
                option(TABLE_NAME, tableName).
                mode(Overwrite).
                save(basePath);
    }
}
