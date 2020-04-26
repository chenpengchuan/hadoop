package com.hadoop.rocketmq.producer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * create by pengchuan.chen on 2020/4/17
 */
public class FileUtils {

  private static final String FILE_PATH = "E:\\tmp\\woven-resource-app.log";

  public static byte[] getStreamBytes() throws IOException {

    File file = new File(FILE_PATH);
    FileInputStream is = new FileInputStream(file);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int len = 0;
    while ((len = is.read(buffer)) != -1) {
      baos.write(buffer, 0, len);
    }
    byte[] b = baos.toByteArray();
    is.close();
    baos.close();
    return b;
  }

}
