package com.hadoop.geospark;

import com.vividsolutions.jts.geom.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.geosparksql.UDT.GeometryUDT;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.Adapter;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.*;

/**
 * create by pengchuan.chen on 2021/5/19
 */
public class SpcQueryTest {
    public static JavaSparkContext sc;

    public static SparkSession spark;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("spcGeoSparkTest").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
//        conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        spark = SparkSession.builder().config(conf).getOrCreate();
        sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        testSpcSqlDistanceJoinQuery();
//        testDistanceJoinQueryFlat();
//        testSpcDistanceJoinQuery();
//        spcKnnQueryTest();
//        spcTestSpatialRangeQuery();
    }


    private static void testSpcSqlDistanceJoinQuery() throws Exception {
        GeoSparkSQLRegistrator.registerAll(spark);
        String tsvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\county_small.tsv";
        PolygonRDD polygonRDD = new PolygonRDD(sc, tsvPath, 0, -1, FileDataSplitter.WKT, true, StorageLevel.MEMORY_ONLY());
        String csvPointInputLocation = "E:\\github/GeoSparkTemplateProject/geospark/java/src/test/resources/arealm-small.csv";
        PointRDD objectRDD = new PointRDD(sc, csvPointInputLocation, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        CircleRDD queryWindowRDD = new CircleRDD(objectRDD, 0.1);

        polygonRDD.spatialPartitioning(GridType.QUADTREE);
        queryWindowRDD.spatialPartitioning(polygonRDD.getPartitioner());
        polygonRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<Geometry, HashSet<Polygon>> javaPairRDD = JoinQuery.DistanceJoinQuery(polygonRDD, queryWindowRDD, false, true);
        JavaRDD<Row> javaRDD = javaPairRDD.mapPartitions(pair -> {
            List<Row> rows = new ArrayList<>();
            while (pair.hasNext()) {
                Tuple2<Geometry, HashSet<Polygon>> p = pair.next();
                Geometry geometry = p._1();
                List<Object> list = new LinkedList<>();
                list.add(geometry);

                Set<Polygon> geometrySet = p._2();
                list.add(geometrySet.toArray(new Geometry[0]));

                Row row = Row$.MODULE$.fromSeq(JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq());
                rows.add(row);
            }

            return rows.iterator();
        });

        StructField[] structFields = new StructField[2];
        structFields[0] = StructField.apply("col_" + 0, new GeometryUDT(), true, Metadata.empty());
        structFields[1] = StructField.apply("col_" + 1, new ArrayType(new GeometryUDT(), true), true, Metadata.empty());
        StructType schema = new StructType(structFields);

        Dataset<Row> df = spark.createDataFrame(javaRDD, schema);

        df.show(10, false);
        System.out.println(df.count());
    }


    public static void testDistanceJoinQueryFlat() throws Exception {
        GeoSparkSQLRegistrator.registerAll(spark);
        String tsvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\county_small.tsv";
        PolygonRDD polygonRDD = new PolygonRDD(sc, tsvPath, 0, -1, FileDataSplitter.WKT, true, StorageLevel.MEMORY_ONLY());
        String csvPointInputLocation = "E:\\github/GeoSparkTemplateProject/geospark/java/src/test/resources/arealm-small.csv";
        PointRDD objectRDD = new PointRDD(sc, csvPointInputLocation, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        CircleRDD queryWindowRDD = new CircleRDD(objectRDD, 0.1);

        polygonRDD.spatialPartitioning(GridType.QUADTREE);
        queryWindowRDD.spatialPartitioning(polygonRDD.getPartitioner());
        polygonRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<Geometry, Polygon> flatPairRdd = JoinQuery.DistanceJoinQueryFlat(polygonRDD, queryWindowRDD, false, true);
        JavaRDD<Row> rdd = flatPairRdd.mapPartitions(pair -> {
            List<Row> rows = new ArrayList<>();
            while (pair.hasNext()) {
                Tuple2<Geometry, Polygon> p = pair.next();
                Geometry geometry = p._1();
                List<Object> list = new LinkedList<>();
                list.add(geometry);

                Polygon v = p._2();
                list.add(v);

                Row row = Row$.MODULE$.fromSeq(JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq());
                rows.add(row);
            }
            return rows.iterator();
        });

        StructField[] fields = new StructField[2];
        fields[0] = StructField.apply("col_" + 0, new GeometryUDT(), true, Metadata.empty());
        fields[1] = StructField.apply("col_" + 1, new GeometryUDT(), true, Metadata.empty());
        StructType tmpSchema = new StructType(fields);

        Dataset<Row> tmpDf = spark.createDataFrame(rdd, tmpSchema);

        tmpDf.show(10, false);
        System.out.println(tmpDf.count());
    }

    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    public static void testSpcDistanceJoinQuery() throws Exception {
        GeoSparkSQLRegistrator.registerAll(spark);
        String csvPointInputLocation = "E:\\github/GeoSparkTemplateProject/geospark/java/src/test/resources/arealm-small.csv";
        PointRDD objectRDD = new PointRDD(sc, csvPointInputLocation, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        CircleRDD queryWindowRDD = new CircleRDD(objectRDD, 0.1);

        objectRDD.spatialPartitioning(GridType.QUADTREE);
        queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner());

        objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        queryWindowRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<Geometry, HashSet<Point>> javaPairRDD = JoinQuery.DistanceJoinQuery(objectRDD, queryWindowRDD, false, true);

        JavaRDD<Row> javaRDD = javaPairRDD.mapPartitions(pair -> {
            List<Row> rows = new ArrayList<>();
            while (pair.hasNext()) {
                Tuple2<Geometry, HashSet<Point>> p = pair.next();
                Geometry geometry = p._1();
                List<Object> list = new LinkedList<>();
                list.add(geometry);

                Set<Point> pointSet = p._2();
                list.add(pointSet.toArray(new Point[0]));

                Row row = Row$.MODULE$.fromSeq(JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq());
                rows.add(row);
            }

            return rows.iterator();
        });

        StructField[] structFields = new StructField[2];
        structFields[0] = StructField.apply("col_" + 0, new GeometryUDT(), true, Metadata.empty());
        structFields[1] = StructField.apply("col_" + 1, new ArrayType(new GeometryUDT(), true), true, Metadata.empty());
        StructType schema = new StructType(structFields);

        Dataset<Row> df = spark.createDataFrame(javaRDD, schema);

        df.show(10, false);
        System.out.println(df.count());
    }

    private static void spcKnnQueryTest() {
        String pointPath = "E:\\github/GeoSparkTemplateProject/geospark/java/src/test/resources/arealm-small.csv";
        GeometryFactory geometryFactory = new GeometryFactory();
        Point kNNQueryPoint = geometryFactory.createPoint(new Coordinate(-84.01, 34.01));
        SpatialRDD pointRDD = new PointRDD(sc, pointPath, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        String tsvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\county_small.tsv";
        PolygonRDD polygonRDD = new PolygonRDD(sc, tsvPath, 0, -1, FileDataSplitter.WKT, true);
        List<Polygon> resultList = KNNQuery.SpatialKnnQuery(polygonRDD, kNNQueryPoint, 10, false);

        JavaRDD javaRDD = sc.parallelize(resultList);
        SpatialRDD spatialRDD = new SpatialRDD();
        spatialRDD.setRawSpatialRDD(javaRDD);

        List<String> fileNames = new ArrayList<String>();
        for (int i = 0; i < 17; i++) {
            fileNames.add("col_" + i);
        }
        Dataset df = Adapter.toDf(spatialRDD, JavaConverters.asScalaIteratorConverter(fileNames.iterator()).asScala().toList(), spark);
        df.show(10, false);

        System.out.println(resultList.size());
    }

    /**
     * Test spatial range query(测试转换输出自定义).
     *
     * @throws Exception the exception
     */
    public static void spcTestSpatialRangeQuery() throws Exception {
        String pointRDDInputLocation = "E:\\github/GeoSparkTemplateProject/geospark/java/src/test/resources/arealm-small.csv";
        Envelope rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01);
        PointRDD pointRDD = new PointRDD(sc, pointRDDInputLocation, 0, FileDataSplitter.CSV, true);
        JavaRDD<Point> rdd = RangeQuery.SpatialRangeQuery(pointRDD, rangeQueryWindow, false, false);
        JavaRDD<List<Object>> doubleRdd = rdd.map(p -> {
            List<Object> colms = new LinkedList<>();
            colms.add(p.getX());
            colms.add(p.getY());
            return colms;
        });

        JavaRDD<Row> rowRdd = doubleRdd.map(p -> {
            Row row = Row$.MODULE$.fromSeq(JavaConverters.asScalaIteratorConverter(p.iterator()).asScala().toSeq());
            return row;
        });

        Dataset<Row> df = spark.createDataFrame(rowRdd, createXYSchema());
        df.show(10, false);

        System.out.println(df.count());

    }

    private static StructType createXYSchema() {
        StructField xF = DataTypes.createStructField("x", DataTypes.DoubleType, true);
        StructField yF = DataTypes.createStructField("y", DataTypes.DoubleType, true);
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(xF);
        fieldList.add(yF);

        StructType schema = DataTypes.createStructType(fieldList);

        return schema;
    }
}
