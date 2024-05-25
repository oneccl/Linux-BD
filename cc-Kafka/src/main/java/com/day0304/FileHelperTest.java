package com.day0304;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/3
 * Time: 18:08
 * Description:
 */

public class FileHelperTest {

    public static void main(String[] args) {
        FileHelper helper = new FileHelper();

        // producer
        String src = "D:\\JavaProjects\\Linux-BD\\cc-Kafka\\src\\main\\java\\com\\day0304\\fileHelper.properties";
        String inPath = "C:\\Users\\cc\\Desktop\\temp\\weblogs.rar";
        helper.put(src, inPath);

        // consumer
        String outPath = "C:\\Users\\cc\\Desktop";
        //helper.consume(src,outPath);

    }

}
