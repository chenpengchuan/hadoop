package com.hadoop.geospark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import java.io.IOException;
import java.util.UUID;

/**
 * create by pengchuan.chen on 2021/5/21
 */
public class MyTextOutputFormat extends MultipleTextOutputFormat<String, Object> {

    @Override
    public String generateFileNameForKeyValue(String key, Object value, String name) {
        return name + "." + key;
    }

    @Override
    public String generateLeafFileName(String name) {
        return name + "-" + UUID.randomUUID().toString();
    }

    @Override
    public String generateActualKey(String key, Object value) {
        return null;
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        Path outDir = getOutputPath(job);
        if (outDir == null && job.getNumReduceTasks() != 0) {
            throw new InvalidJobConfException("Output directory not set in JobConf.");
        } else {
            if (outDir != null) {
                FileSystem fs = outDir.getFileSystem(job);
                outDir = fs.makeQualified(outDir);
                setOutputPath(job, outDir);
                TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[]{outDir}, job);
//                if (fs.exists(outDir)) {
//                    throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
//                }
            }

        }
    }
}
