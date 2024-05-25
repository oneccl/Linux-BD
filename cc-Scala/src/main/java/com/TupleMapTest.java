package com;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/11
 * Time: 17:52
 * Description:
 */
public class TupleMapTest {

    public static void main(String[] args) {

        // 创建TupleMap
        TupleMap<String,Integer> tuple = new TupleMap<>();
        System.out.println(tuple);      // ()

        TupleMap<String, Integer> tuple1 = new TupleMap<>("Tom", 18);
        System.out.println(tuple1);     // (Tom,18)

        // 获取K和V
        System.out.println(tuple1.$1);  // Tom
        System.out.println(tuple1.$2);  // 18

        // 添加K和V
        tuple.set("Jack",18);
        // 根据K获取V
        System.out.println(tuple.value("Jack"));  // 18

        // 获取对象
        System.out.println(tuple1.get());  // (Tom,18)

        // K和V调换位置
        System.out.println(tuple.swap());  // (18,Jack)

    }

}
