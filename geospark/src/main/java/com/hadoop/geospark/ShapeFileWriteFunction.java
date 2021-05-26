package com.hadoop.geospark;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.function.VoidFunction;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * create by pengchuan.chen on 2021/5/26
 */
public class ShapeFileWriteFunction<T> implements VoidFunction<T> {

    private String geometryType;

    private String path;

    public ShapeFileWriteFunction(String geometryType, String path) {
        this.geometryType = geometryType;
        this.path = path;
    }


    @Override
    public void call(Object obj) throws Exception {
        Iterator<Geometry> geometryIterator = (Iterator<Geometry>) obj;
        ShapefileDataStore destDataStore = null;
        FeatureWriter<SimpleFeatureType, SimpleFeature> writer = null;
        //TODO create partition dir
        String partitionDirPath = this.path + "\\" + UUID.randomUUID().toString();
        File dir = new File(partitionDirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        //TODO create output file name
        String outputPath = dir.getPath() + "\\part-000.dbf";
        try {
            SimpleFeatureType featureType;
            if ("point".equals(this.geometryType)) {
                featureType = DataUtilities.createType("point",
                        "location:Point"  // <- the geometry attribute: Point type
                        //                        "POIID:String," + // <- a String attribute
                        //                        "MESHID:String," + // a number attribute
                        //                        "OWNER:String"
                );
            } else if ("polygon".equals(this.geometryType)) {
                featureType = DataUtilities.createType("polygon",
                        "location:Polygon"  // <- the geometry attribute: Point type
                );
            } else {
                featureType = DataUtilities.createType("line",
                        "location:Line"  // <- the geometry attribute: Point type
                );
            }
            destDataStore = new ShapefileDataStore(new File(outputPath).toURI().toURL());
            destDataStore.createSchema(SimpleFeatureTypeBuilder.retype(featureType, DefaultGeographicCRS.WGS84));
            writer = destDataStore.getFeatureWriter(Transaction.AUTO_COMMIT);
            while (geometryIterator.hasNext()) {
                Geometry g = geometryIterator.next();
                List<Object> values = new ArrayList<>();
                values.add(g);
                SimpleFeature fNew = writer.next();
                fNew.setAttributes(values);
                writer.write();
            }
        } catch (Exception e) {
            dir.deleteOnExit();
            throw new RuntimeException(e);
        } finally {
            if (writer != null) {
                writer.close();
            }
            if (destDataStore != null) {
                destDataStore.dispose();
            }
        }
    }
}
