package com.hadoop.geospark;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.commons.lang.NullArgumentException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper;
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparksql.utils.Adapter;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.ImageGenerator;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;
import org.datasyslab.geosparkviz.extension.visualizationEffect.HeatMap;
import org.datasyslab.geosparkviz.utils.ImageType;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.*;

/**
 * create by pengchuan.chen on 2021/4/26
 */
public class SpcExample {

    public static JavaSparkContext sc;

    public static SparkSession spark;

    public static String pointRDDInputLocation;

    static Envelope rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01);

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("spcGeoSparkTest").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
//        conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        spark = SparkSession.builder().config(conf).getOrCreate();
        sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        pointRDDInputLocation = "E:\\github/GeoSparkTemplateProject/geospark/java/src/test/resources/arealm-small.csv";

        PointRDD pointRDD = ShapefileReader.readToPointRDD(sc, "E:\\workdir\\result_out\\shapefile\\3232a8fa-576d-4e5e-8b75-11deeebcf9ce");
        System.out.println(pointRDD.rawSpatialRDD.count());
//        testSaveAsGeoJson();
//        spcTestSpatialRangeQuery(pointRDDInputLocation);
//        testSpatialJoinQuery();
//        testSpatialDfJoinQuery();
//        testShapefiles();
//        testSpatialJoinQuery1();
    }

    public static void testSaveAsGeoJson() throws IOException {
        String tsvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\county_small.tsv";
        PolygonRDD polygonRDD = new PolygonRDD(sc, tsvPath, 0, -1, FileDataSplitter.WKT, true);

        List<String> fileNames = new ArrayList<String>();
        for (int i = 0; i < 17; i++) {
            fileNames.add("col_" + i);
        }
        Dataset df = Adapter.toDf(polygonRDD, JavaConverters.asScalaIteratorConverter(fileNames.iterator()).asScala().toList(), spark);
        df.show(10, false);

        String outputPath = "E:\\workdir\\result_out\\tmp";
        JavaPairRDD<String, String> javaPairRDD = JavaPairRDD(polygonRDD.rawSpatialRDD);
        javaPairRDD.saveAsHadoopFile(outputPath, String.class, String.class, MyTextOutputFormat.class);
        System.out.println(df.count());
    }

    public static <T extends Geometry> JavaPairRDD<String, String> JavaPairRDD(JavaRDD<T> rawSpatialRDD) {
        if (rawSpatialRDD == null) {
            throw new NullArgumentException("save as WKT cannot operate on null RDD");
        }

        JavaRDD<Tuple2<String, String>> stringRdd = rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Iterator<T> iterator)
                    throws Exception {
                WKTWriter writer = new WKTWriter(3);
                ArrayList<Tuple2<String, String>> wkts = new ArrayList<>();

                while (iterator.hasNext()) {
                    Geometry spatialObject = iterator.next();
                    String wkt = writer.write(spatialObject);

                    Tuple2<String, String> tuple2;
                    if (spatialObject.getUserData() != null) {
                        String value = wkt + "\t" + spatialObject.getUserData();
                        tuple2 = new Tuple2<>("wkt", value);
                    } else {
                        tuple2 = new Tuple2<>("wkt", wkt);
                    }
                    wkts.add(tuple2);
                }
                return wkts.iterator();
            }
        });
        return JavaPairRDD.fromJavaRDD(stringRdd);
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

    public static void testSpatialJoinQuery() throws Exception {
        GeoSparkSQLRegistrator.registerAll(spark);
        String csvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\arealm-small.csv";
        String tsvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\county_small.tsv";

        PointRDD pointRDD = new PointRDD(sc, csvPath, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        PolygonRDD polygonRDD = new PolygonRDD(sc, tsvPath, 0, -1, FileDataSplitter.WKT, true);

        Dataset<Row> pointDf = Adapter.toDf(pointRDD, spark);
        Dataset geoDf = pointDf.selectExpr("ST_GeomFromWKT(geometry)");
        SpatialRDD spatialRdd = Adapter.toSpatialRdd(geoDf);

        spatialRdd.analyze(StorageLevel.MEMORY_ONLY());
        spatialRdd.spatialPartitioning(GridType.QUADTREE);
        polygonRDD.spatialPartitioning(spatialRdd.getPartitioner());

        JavaPairRDD<Polygon, HashSet<Point>> resultRdd = JoinQuery.SpatialJoinQuery(spatialRdd, polygonRDD, false, true);
        long resultSize = resultRdd.count();
        System.out.println("resultSize : " + resultSize);
        assert resultSize > 0;
    }

    public static void testSpatialDfJoinQuery() throws Exception {
        GeoSparkSQLRegistrator.registerAll(spark);
        String csvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\arealm-small.csv";
        String tsvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\county_small.tsv";
        Dataset<Row> csvTmp = spark.read().format("csv").option("delimiter", ",").option("header", "false").schema(createXYSchema()).load(csvPath);
        csvTmp.createOrReplaceTempView("pointtable");
        Dataset pdf = spark.sql("select ST_Point(cast(pointtable.x as Decimal(24,20)), cast(pointtable.y as Decimal(24,20))) as arealandmark from pointtable");
        SpatialRDD prdd = Adapter.toSpatialRdd(pdf);

        Dataset<Row> tsvTmp = spark.read().format("csv").option("delimiter", "\t").option("header", "false").load(tsvPath);
        List<String> polydfFieldNames = new ArrayList<>();
        for (int i = 1; i < 18; i++) {
            polydfFieldNames.add("col_" + i);
        }
        StringBuilder expr = new StringBuilder("select");
        for (int i = 0; i < 18; i++) {
            if (i == 0) {
                expr.append(" ST_GeomFromWKT(polygontable._c0) as " + "geometry");
            } else {
                expr.append(", _c" + i + " as " + polydfFieldNames.get(i - 1));
            }
        }
        expr.append(" from polygontable");

        tsvTmp.createOrReplaceTempView("polygontable");
        Dataset polydf = spark.sql(expr.toString());

        SpatialRDD polyRdd = Adapter.toSpatialRdd(polydf, 0, JavaConverters.asScalaIteratorConverter(polydfFieldNames.iterator()).asScala().toList());

        prdd.analyze(StorageLevel.MEMORY_ONLY());
        prdd.spatialPartitioning(GridType.QUADTREE);
        polyRdd.spatialPartitioning(prdd.getPartitioner());

        JavaPairRDD<Polygon, HashSet<Point>> result1Rdd = JoinQuery.SpatialJoinQuery(prdd, polyRdd, false, true);
        long result1Size = result1Rdd.count();
        System.out.println("result1 Size : " + result1Size);

        PointRDD pointRDD = new PointRDD(sc, csvPath, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        PolygonRDD polygonRDD = new PolygonRDD(sc, tsvPath, 0, -1, FileDataSplitter.WKT, true);

        Dataset<Row> pointDf = Adapter.toDf(pointRDD, spark);
        Dataset geoDf = pointDf.selectExpr("ST_GeomFromWKT(geometry)");
        SpatialRDD spatialRdd = Adapter.toSpatialRdd(geoDf);

        spatialRdd.analyze(StorageLevel.MEMORY_ONLY());
        spatialRdd.spatialPartitioning(GridType.QUADTREE);
        polygonRDD.spatialPartitioning(spatialRdd.getPartitioner());

        JavaPairRDD<Polygon, HashSet<Point>> resultRdd = JoinQuery.SpatialJoinQuery(spatialRdd, polygonRDD, false, true);
        long resultSize = resultRdd.count();
        System.out.println("resultSize : " + resultSize);
        assert resultSize > 0;
    }

    public static void testShapefiles() throws Exception {
        String outputPath = "E:\\workdir\\test\\output0";
        String shapeFilePath = "E:\\workdir\\test\\shapefiles\\point";
        String urlPrefix = "E:\\workdir\\test\\modis\\";
        String hdfPath = urlPrefix + "modis.csv";
        String tsvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\county_small.tsv";

        String[] HDFDataVariableList = {"LST", "QC", "Error_LST", "Emis_31", "Emis_32"};
        PointRDD pointRDD = ShapefileReader.readToPointRDD(sc, shapeFilePath);

        EarthdataHDFPointMapper earthdataHDFPoint = new EarthdataHDFPointMapper(5, 2, "MOD_Swath_LST",
                HDFDataVariableList, "LST", true, urlPrefix);
        PointRDD pointRDD1 = new PointRDD(sc, hdfPath, 5, earthdataHDFPoint, StorageLevel.MEMORY_ONLY());
//        pointRDD1.saveAsGeoJSON(outputPath);
        Adapter.toDf(pointRDD1, spark).write().mode("overwrite").format("csv").save(outputPath);
        System.out.println(pointRDD);

        try {
            Envelope USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000);
            PolygonRDD polygonRDD = new PolygonRDD(sc, tsvPath, 0, -1, FileDataSplitter.WKT, true);
            HeatMap visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2);
            visualizationOperator.Visualize(sc, pointRDD);
            ImageGenerator imageGenerator = new ImageGenerator();
            imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.SVG);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testSpatialJoinQuery1() throws Exception {
        GeoSparkSQLRegistrator.registerAll(spark);
        String csvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\arealm-small.csv";
        String tsvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\county_small.tsv";

        PointRDD pointRDD = new PointRDD(sc, csvPath, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        PolygonRDD tmp_polygonRDD = new PolygonRDD(sc, tsvPath, 0, -1, FileDataSplitter.WKT, true);
        Dataset<Row> tmp = Adapter.toDf(tmp_polygonRDD, spark);
        tmp.selectExpr("geometry").write().format("csv").mode("overwrite").save("E:\\workdir\\tmp_out");

        Dataset tmpPloyDf = spark.read().csv("E:\\workdir\\tmp_out");
        Dataset ployDf = tmpPloyDf.selectExpr("ST_GeomFromWKT(_c0)");
        SpatialRDD polygonRDD = Adapter.toSpatialRdd(ployDf);

        Dataset<Row> pointDf = Adapter.toDf(pointRDD, spark);
        Dataset geoDf = pointDf.selectExpr("ST_GeomFromWKT(geometry) as c_0");
        SpatialRDD spatialRdd = Adapter.toSpatialRdd(geoDf);

        spatialRdd.analyze(StorageLevel.MEMORY_ONLY());
        spatialRdd.spatialPartitioning(GridType.QUADTREE);
        polygonRDD.spatialPartitioning(spatialRdd.getPartitioner());

        JavaPairRDD<Polygon, HashSet<Point>> resultRdd = JoinQuery.SpatialJoinQuery(spatialRdd, polygonRDD, false, true);
        resultRdd.saveAsTextFile("E:\\workdir\\result_out/out1");
        long resultSize = resultRdd.count();
        System.out.println("resultSize : " + resultSize);
        assert resultSize > 0;

        spatialRdd.analyze(StorageLevel.MEMORY_ONLY());
        spatialRdd.spatialPartitioning(GridType.QUADTREE);
        tmp_polygonRDD.spatialPartitioning(spatialRdd.getPartitioner());
        JavaPairRDD<Polygon, HashSet<Point>> resultRdd1 = JoinQuery.SpatialJoinQuery(spatialRdd, tmp_polygonRDD, false, true);
        resultRdd1.saveAsTextFile("E:\\workdir\\result_out/out2");
        long resultSize1 = resultRdd1.count();
        System.out.println("resultSize1:" + resultSize1);

        JavaRDD<Point> javaRDD = resultRdd.map(pair -> {
            Polygon polygon = pair._1();
            Set<Point> points = pair._2();
            return points;
        }).flatMap(x -> x.iterator());

    }
}
