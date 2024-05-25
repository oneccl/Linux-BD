package com.SparkStreaming.day0308;

import java.io.*;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/8
 * Time: 10:20
 * Description:
 */
public class DouYinDataSources {

    // Java模拟

    public static void main(String[] args) {
        String inPath = args[0];
        File inFile = new File(inPath);
        String outPath = args[1];
        // 输出目录是否存在
        File outFile = new File(outPath);
        if (!outFile.exists()) {
            System.out.println(outFile.mkdir() ? "正在创建目录:" + outPath + "...\n" + "正在输出数据到:" + outPath + "..." : "正在输出数据到:" + outPath + "...");
        } else {
            System.out.println("正在输出数据到:" + outPath + "...");
        }
        // 读写目录下所有文件
        for (int i = 0; i < Integer.parseInt(args[2]); i++) {  // 同一文件读取多少次/份
            try {
                Thread.sleep(Integer.parseInt(args[3]));  // 多久生成一份
                for (File file : Objects.requireNonNull(inFile.listFiles())) {
                    // 读取所有行
                    // 输入缓冲流
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    List<String> lines = reader.lines().collect(Collectors.toList());
                    lines.remove(0);  // 去表头
                    // 输出缓冲流
                    BufferedWriter writer = new BufferedWriter(new FileWriter(outPath + File.separator + file.getName() + i));
                    // 读取数据
                    int count = 0;
                    for (String line : lines) {
                        writer.write(line);
                        writer.flush();
                        count++;
                        if (count%10 == 0){    // 每读取10行时间间隔
                            Thread.sleep(Integer.parseInt(args[3]));
                        }
                    }
                    writer.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
