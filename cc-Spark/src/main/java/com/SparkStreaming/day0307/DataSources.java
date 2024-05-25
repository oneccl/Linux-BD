package com.SparkStreaming.day0307;

import java.io.*;
import java.util.Objects;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/6
 * Time: 17:19
 * Description:
 */
public class DataSources {

    // 模拟数据源

    public static void main(String[] args) {
        String inPath = args[0];
        File inFile = new File(inPath);
        String outPath = args[1];
        // 输出目录是否存在
        File outFile = new File(outPath);
        if (!outFile.exists()){
            System.out.println(outFile.mkdir()?"正在创建目录:"+outPath+"...\n"+"正在输出数据到:"+outPath+"...":"正在输出数据到:"+outPath+"...");
        }
        // 读写目录下所有文件
        for (File file : Objects.requireNonNull(inFile.listFiles())) {
            try {
                // 输入缓冲流
                BufferedReader reader = new BufferedReader(new FileReader(file));
                // 输出缓冲流
                BufferedWriter writer = new BufferedWriter(new FileWriter(outPath+File.separator+file.getName()));
                // 读取数据
                String data = reader.readLine();
                while (data!=null && !data.equals("")){
                    Thread.sleep(Integer.parseInt(args[2]));
                    // 写出数据
                    writer.write(data);
                    writer.flush();
                    data=reader.readLine();
                }
                writer.close();
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
