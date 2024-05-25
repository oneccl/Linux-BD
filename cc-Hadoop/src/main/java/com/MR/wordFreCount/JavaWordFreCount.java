package com.MR.wordFreCount;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/14
 * Time: 21:02
 * Description:
 */
public class JavaWordFreCount {

    // Java实现词频统计

    @Test
    public void wordCount() throws Exception {
        // 缓冲流读取文件
        String inputPath = "C:\\Users\\cc\\Desktop\\temp\\HarryPotter.txt";
        BufferedReader reader = new BufferedReader(new FileReader(inputPath));
        // 读取一行
        String line = reader.readLine();
        Map<String, Integer> map = new HashMap<>();
        // 处理每一行
        while (!line.isEmpty()){
            mapReduce(line, map);
            line = reader.readLine();
        }
        // 处理结果，输出次数最多的前20个单词
        sortOutput(map,20);
    }

    // 单行处理
    public void mapReduce(String line, Map<String, Integer> map) {
        // 正则表达式：Pattern类
        // \\W+ 去除除字母数字下划线外的所有符号
        // \\s+ 使用空白字符(空格、\n、\t等)分割
        String[] words = line.replaceAll("\\W+"," ").split("\\s+");
        for (String word : words) {
            String w = word.trim().toLowerCase();
            if (!w.isEmpty()){
//                if (!map.containsKey(w)){
//                    map.put(w,1);
//                }else {
//                    map.put(w, map.get(w)+1);
//                }
                Integer f = map.getOrDefault(w, 0) + 1;
                map.put(w,f);
            }
        }
    }

    // Map转换为List并排序输出
    public void sortOutput(Map<String, Integer> map, Integer n) {
        Set<Map.Entry<String, Integer>> entries = map.entrySet();
        List<Map.Entry<String, Integer>> list = new ArrayList<>(entries);
        list.sort((o1, o2) -> Integer.compare(o2.getValue(),o1.getValue()));
        for (int i = 0; i < list.size() && i < n; i++) {
            System.out.println(list.get(i));
        }
    }

}
