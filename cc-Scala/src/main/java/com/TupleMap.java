package com;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/11
 * Time: 17:48
 * Description:
 */
public class TupleMap<K,V> {

    private static final String InitEle = "()";  // 初始化值
    private final Map<K,V> tupleMap = new ConcurrentHashMap<>();
    public K $1;
    public V $2;

    public TupleMap() {
    }

    public TupleMap(K k,V v) {
        this.$1 = k;
        this.$2 = v;
        tupleMap.put(k, v);
    }

    private K get$1() {
        return this.$1;
    }

    private void set$1(K k) {
        this.$1 = k;
    }

    private V get$2() {
        return this.$2;
    }

    private void set$2(V v) {
        this.$2 = v;
    }

    public TupleMap<K,V> get(){
        return new TupleMap<>(this.$1,this.$2);
    }

    public void set(K k,V v){
        this.$1=k;
        this.$2=v;
        tupleMap.put(k, v);
    }

    // 根据K获取V
    public V value(K k){
        try {
            return tupleMap.isEmpty()?null:tupleMap.get(k);
        } catch (Exception e) {
            throw new RuntimeException("NoSuch"+k.getClass().getSimpleName()+": "+k);
        }
    }

    // K和V调换位置
    public TupleMap<V,K> swap(){
        return new TupleMap<>(this.$2,this.$1);
    }

    // K或V不能为空
    public String toString() {
        return this.$1 == null || this.$2 == null ? InitEle : "(" + this.$1 + "," + this.$2 + ")";
    }

}
