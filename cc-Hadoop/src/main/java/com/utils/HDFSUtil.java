package com.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/1/11
 * Time: 10:22
 * Description:
 */
public class HDFSUtil {

    // 获取路径下所有文件
    public static List<String> getFiles(FileSystem fs, List<String> files, String path) throws IOException {
        // 1)获取根目录下的所有文件和目录
        FileStatus[] statuses = fs.listStatus(new Path(path));
        for (FileStatus status : statuses) {
            // 文件
            if (status.isFile()){
                files.add(status.getPath().toString());
            }else {
                String s = status.getPath().toString();
                // 2)截取hdfs://bd91:8020/后面的路径
                getFiles(fs,files,s.substring(s.indexOf("/", 7)));
            }
        }
        return files;
    }

    // 获取hdfs dfs -ls path下所有目录/文件信息
    public static String getLsAlign(FileSystem fs, String path, Integer size, Boolean left) throws IOException {
        FileStatus[] statuses = fs.listStatus(new Path(path));
        StringBuilder builder = new StringBuilder();
        for (FileStatus status : statuses) {
            // 文件类型
            String type = status.isFile() ? "-" : "d";
            // 3)获取目录/文件的权限
            String permission = status.getPermission().toString();
            // 4)获取副本数
            int replication = status.getReplication();
            // 5)获取当前用户
            String owner = status.getOwner();
            // 6)获取当前用户所在组
            String group = status.getGroup();
            // 7)获取文件大小
            long len = status.getLen();
            // 8)获取最后修改时间
            long modTime = status.getModificationTime();
            String formatTime = new SimpleDateFormat("yyyy-MM-dd hh:mm").format(new Date(modTime));
            // 截取hdfs://bd91:8020/后面的路径
            String s = status.getPath().toString();
            String pa = s.substring(s.indexOf("/",7));
            // 拼接结果
            builder.append(type).append(permission).append("\t")
                    .append(replication).append("\t")
                    .append(owner).append("\t")
                    .append(group).append("\t")
                    .append(len).append("\t")
                    .append(formatTime).append("\t")
                    .append(pa).append("\n");
        }
        return align(builder.toString(),"\t",size,left,7);
    }

    // 获取hdfs dfs -ls path下所有文件信息(仅getLen()列右对齐)
    public static String getLsFile(FileSystem fs, String path) throws IOException {
        FileStatus[] statuses = fs.listStatus(new Path(path));
        StringBuilder builder = new StringBuilder();
        Long colLenMax = getColLenMax(fs, path, 0L);
        for (FileStatus status : statuses) {
            if (status.isFile()){
                String type = status.isFile() ? "-" : "d";
                String permission = status.getPermission().toString();
                int replication = status.getReplication();
                String owner = status.getOwner();
                String group = status.getGroup();
                long len = status.getLen();
                long modTime = status.getModificationTime();
                String formatTime = new SimpleDateFormat("yyyy-MM-dd hh:mm").format(new Date(modTime));
                String s = status.getPath().toString();
                String pa = s.substring(s.indexOf("/",7));
                builder.append(type).append(permission).append("\t")
                        .append(replication).append("\t")
                        .append(owner).append("\t")
                        .append(group).append("\t")
                        .append(getLenRightPad(colLenMax,len)).append("\t")
                        .append(formatTime).append("\t")
                        .append(pa).append("\n");
            }
        }
        return builder.toString();
    }

    // 获取文件getLen()列最大长度
    public static Long getColLenMax(FileSystem fs, String path, long maxLen) throws IOException {
        FileStatus[] statuses = fs.listStatus(new Path(path));
        for (FileStatus status : statuses) {
            if (String.valueOf(status.getLen()).length() > maxLen){
                maxLen = String.valueOf(status.getLen()).length();
            }
        }
        return maxLen;
    }

    // getLen()列右对齐补空格
    public static String getLenRightPad(Long maxLen, Long curLen){
        StringBuilder builder = new StringBuilder();
        int length = String.valueOf(curLen).length();
        if (length < maxLen){
            for (int i = 0; i < maxLen - length; i++) {
                builder.append(" ");
            }
        }
        return builder.append(curLen).toString();
    }

    // 字符串输出全左对齐或右对齐
    // s：要处理的字符串；split：以什么分割；size：子字符串总长度(不足用空格补全)；left：左对齐/右对齐；col：列数
    public static String align(String s, String split, Integer size, Boolean left, Integer col){
        if (size < 5){
            size = 5;
        }
        StringBuilder res = new StringBuilder();
        String[] strings = s.split("\n");
        Iterator<String> it = Arrays.stream(strings).iterator();

        // 获取列字符串的最大长度
        List<Integer> colStrMaxLens = getColStrMaxLens(s, split, col);

        while (it.hasNext()) {
            String[] strs = it.next().split(split);
            // 拼接
            for (int i = 0; i < strs.length; i++) {
                if (i == strs.length-1){
                    // 左对齐
                    if (left){
                        if (strs[i].length() > size){
                            res.append(StringUtils.rightPad(strs[i],strs[i].length()+size)).append("\n");
                        }else {
                            res.append(StringUtils.rightPad(strs[i],colStrMaxLens.get(i)+size)).append("\n");
                        }
                    // 右对齐
                    }else {
                        if (strs[i].length() > size){
                            res.append(StringUtils.leftPad(strs[i],strs[i].length()+size)).append("\n");
                        }else {
                            res.append(StringUtils.leftPad(strs[i],colStrMaxLens.get(i)+size)).append("\n");
                        }
                    }
                }else {
                    if (left){
                        if (strs[i].length() > size){
                            res.append(StringUtils.rightPad(strs[i],strs[i].length()+size));
                        }else {
                            res.append(StringUtils.rightPad(strs[i],colStrMaxLens.get(i)+size));
                        }
                    }else {
                        if (strs[i].length() > size){
                            res.append(StringUtils.leftPad(strs[i],strs[i].length()+size));
                        }else {
                            res.append(StringUtils.leftPad(strs[i],colStrMaxLens.get(i)+size));
                        }
                    }
                }
            }
        }
        return res.toString();
    }

    // 获取所有列字符串的最大长度
    public static List<Integer> getColStrMaxLens(String s, String split, Integer colCount){
        String[] strings = s.split("\n");
        Iterator<String> it;
        List<Integer> cloStrLenMaxs = new ArrayList<>();
        for (int i = 0; i < colCount; i++) {
            it = Arrays.stream(strings).iterator();
            List<Integer> integers = new ArrayList<>();
            while (it.hasNext()){
                String[] strs = it.next().split(split);
                int len = strs[i].length();
                integers.add(len);
            }
            Integer maxLen = Collections.max(integers);
            cloStrLenMaxs.add(maxLen);
        }
        return cloStrLenMaxs;
    }

}
