import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试flink sql从kafka读取数据写入mysql
 * create by pengchuan.chen on 2021/12/17
 */
public class FlinkSqlTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableContext = StreamTableEnvironment.create(env, settings);

        String sqls = "CREATE TABLE table1 (\n" +
                "  name VARCHAR,\n" +
                "  age INTEGER\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'test',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'connector.properties.zookeeper.connect' = '192.168.1.200:2181',\n" +
                "  'connector.properties.bootstrap.servers' = '192.168.1.200:9092',\n" +
                "  'connector.properties.group.id' = 'group1',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ");\n" +
                "CREATE TABLE table2 (\n" +
                "  name VARCHAR,\n" +
                "  age INTEGER\n" +
                ") WITH (\n" +
                "  'connector.type' = 'jdbc',\n" +
                "  'connector.url' = 'jdbc:mysql://192.168.1.75:3306/spc_merce',\n" +
                "  'connector.table' = 'flink_sql_sink',\n" +
                "  'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "  'connector.username' = 'merce',\n" +
                "  'connector.password' = 'merce',\n" +
                "  'connector.write.flush.max-rows' = '1',\n" +
                "  'connector.write.flush.interval' = '0s', \n" +
                "  'connector.write.max-retries' = '3'\n" +
                "); \n\n" +
                "INSERT INTO\n" +
                "  table2\n" +
                " select\n" +
                "  name ,\n" +
                "  age\n" +
                "FROM\n" +
                "  table1";

        for (String sq : sqls.split(";")) {
            tableContext.executeSql(sq.trim());
        }
        env.execute();

    }
}
