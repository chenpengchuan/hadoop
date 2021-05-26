package com.hadoop.geospark;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.shp.ShapeType;
import org.geotools.data.shapefile.shp.ShapefileWriter;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * create by pengchuan.chen on 2021/5/24
 */
public class ShapeFileTest implements Serializable {
    public static JavaSparkContext sc;

    public static SparkSession spark;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("shapeFileWriteTest").setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
//        conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());
        spark = SparkSession.builder().config(conf).getOrCreate();
        sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        String shapeFileSourcePath = "E:\\github\\hadoop\\geospark\\src\\test\\resources\\shapefiles\\point\\map.dbf";
        String shapeFileSinkPath = "E:\\workdir\\result_out\\shapefile\\test.shp";
        transShape(shapeFileSinkPath);
        writeShapeFileForEach(shapeFileSinkPath);
//        testWriter();
    }


    /**
     * 读取point csv文件输出到Shapefile
     *
     * @param destfilepath
     * @throws SchemaException
     */
    public static void transShape(String destfilepath) throws SchemaException {
        String csvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\arealm-small.csv";
        PointRDD pointRDD = new PointRDD(sc, csvPath, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        //定义属性
        final SimpleFeatureType TYPE = DataUtilities.createType("map",
                "location:Point"  // <- the geometry attribute: Point type
//                        "POIID:String," + // <- a String attribute
//                        "MESHID:String," + // a number attribute
//                        "OWNER:String"
        );
        try {
            /****************************************************************************************/
            List<Point> pointList = pointRDD.getRawSpatialRDD().collect();
            System.out.println(pointList.size());
            /****************************************************************************************/
            //创建目标shape文件对象
            ShapefileDataStore dest = new ShapefileDataStore(new File(destfilepath).toURI().toURL());

            dest.createSchema(SimpleFeatureTypeBuilder.retype(TYPE, DefaultGeographicCRS.WGS84));
            //设置writer
//            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dest.getFeatureWriter(dest.getTypeNames()[0], Transaction.AUTO_COMMIT);
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dest.getFeatureWriter(Transaction.AUTO_COMMIT);

            Iterator<Point> iterator = pointList.iterator();
            while (iterator.hasNext()) {
                Point p = iterator.next();
                List<Object> values = new ArrayList<>();
                values.add(p);
                SimpleFeature fNew = writer.next();
                fNew.setAttributes(values);
                writer.write();
            }

            writer.close();
            dest.dispose();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        PointRDD prdd = org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader.readToPointRDD(sc, "E:\\workdir\\result_out\\shapefile\\");
        System.out.println(prdd.rawSpatialRDD.count());
    }

    /**
     * 通过rdd分区写出数据到ShapeFile
     *
     * @param destfilepath
     * @throws SchemaException
     */
    public static void writeShapeFileForEach(String destfilepath) throws SchemaException {
        String csvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\arealm-small.csv";
        PointRDD pointRDD = new PointRDD(sc, csvPath, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        /****************************************************************************************/
        List<Point> pointList = pointRDD.getRawSpatialRDD().collect();
        System.out.println(pointList.size());
        /****************************************************************************************/

//        pointRDD.rawSpatialRDD.repartition(3).foreachPartition(pointIterator -> {
//            //todo 定义属性
//            final SimpleFeatureType TYPE1 = DataUtilities.createType("map",
//                    "location:Point"  // <- the geometry attribute: Point type
////                        "POIID:String," + // <- a String attribute
////                        "MESHID:String," + // a number attribute
////                        "OWNER:String"
//            );
//            String sinkPath = "E:\\workdir\\result_out\\shapefile";
//            String path = sinkPath + "\\" + UUID.randomUUID().toString();
//            File dir = new File(path);
//            if (!dir.exists()) {
//                dir.mkdirs();
//            }
//            ShapefileDataStore dest1 = new ShapefileDataStore(new File(path + "\\part-000.dbf").toURI().toURL());
//
//            dest1.createSchema(SimpleFeatureTypeBuilder.retype(TYPE1, DefaultGeographicCRS.WGS84));
//            //设置writer
////            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dest.getFeatureWriter(dest.getTypeNames()[0], Transaction.AUTO_COMMIT);
//            FeatureWriter<SimpleFeatureType, SimpleFeature> writer1 = dest1.getFeatureWriter(Transaction.AUTO_COMMIT);
//            while (pointIterator.hasNext()) {
//                Point p = pointIterator.next();
//                List<Object> values = new ArrayList<>();
//                values.add(p);
//                SimpleFeature fNew = writer1.next();
//                fNew.setAttributes(values);
//                writer1.write();
//            }
//            writer1.close();
//            dest1.dispose();
//        });

        pointRDD.rawSpatialRDD.repartition(3).foreachPartition(new ShapeFileWriteFunction("point", "E:\\workdir\\result_out\\shapefile"));
    }


    /**
     * 使用ShapefileWriter写数据到ShapeFile
     */
    public static void testWriter() throws IOException {
        String csvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\arealm-small.csv";
        String tsvPath = "E:\\github\\GeoSparkTemplateProject\\geospark\\java\\src\\test\\resources\\county_small.tsv";

        PointRDD pointRDD = new PointRDD(sc, csvPath, 0, FileDataSplitter.CSV, true, StorageLevel.MEMORY_ONLY());
        String shapeFileSinkPath = "E:\\workdir\\result_out\\shapefile";
        File shpFile = new File(shapeFileSinkPath + "\\test.shp");
        File dir = new File(shapeFileSinkPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        if (!shpFile.exists()) {
            shpFile.createNewFile();
        }
        File shxFile = new File(shapeFileSinkPath + "\\test.shx");
        if (!shxFile.exists()) {
            shxFile.createNewFile();
        }
        FileOutputStream shpOutputStream = new FileOutputStream(shpFile);
        FileOutputStream shxOutputStream = new FileOutputStream(shxFile);
        ShapefileWriter shapefileWriter = new ShapefileWriter(shpOutputStream.getChannel(), shxOutputStream.getChannel());

        GeometryFactory geometryFactory = new GeometryFactory();

        Geometry[] geometries = pointRDD.rawSpatialRDD.collect().toArray(new Geometry[0]);
        GeometryCollection geometryCollection = new GeometryCollection(geometries, geometryFactory);
        shapefileWriter.write(geometryCollection, ShapeType.POINT);
        shapefileWriter.close();

        PointRDD prdd = org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader.readToPointRDD(sc, shapeFileSinkPath);
        System.out.println(prdd.rawSpatialRDD.count());
    }

}
