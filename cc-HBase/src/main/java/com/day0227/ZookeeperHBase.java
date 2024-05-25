package com.day0227;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/27
 * Time: 17:08
 * Description:
 */
public class ZookeeperHBase {

    // Zookeeper: Hadoop生态中的集群协调服务
    /*
    1、主要应用场景：
    1）高可用集群的主备切换
    2）统一的资源管理配置
    3）分布式锁
    2、核心原理
    1）数据一致性
    通过记录ZXID(逻辑时钟)保证数据的一致性
    ZXID(逻辑时钟)在逻辑上像钟表一样始终单调递增
    每次进行zk数据的增删改时，ZXID都会变化
    2）zk数据模型
    zk数据模型是一个类似文件系统的树形结构
    从/开始，每个节点称为一个zNode
    每个zNode有名称，且包含数据（默认单个zNode最大存储1M数据）
    zNode的4种类型：
    生命周期：永久节点：若不主动删除，永久保存
            临时节点：创建节点的客户端断开连接后，节点自动消亡（create -e 文件名）
    序号：有序：创建zNode时，zk自动在其名称后追加单调递增序号（create -s 文件名）
         无序
    3）zk选举机制
    首次启动：选择zk服务id最大的为leader，其它节点作为follower
    leader宕机后，主从切换，选举剩余zk服务id最大的为leader
    */

    // HBase: Hadoop生态中的一个列式存储数据库
    // HBase集群角色：
    /*
    1、HMaster（Hbase会默认在执行启动命令的机器上启动HMaster服务）
    1）监控HRegionServer
    2）处理HRegionServer故障转移
    3）处理元数据的变更
    4）处理region的分配或移除
    5）在空闲时间进行数据的负载均衡
    6）通过Zookeeper发布自己的位置给客户端
    2、HRegionServer
    1）负责管理HBase的实际数据
    2）处理分配给它的Region分片
    3）刷新缓存到HDFS
    4）维护HLog Write-Ahead Log WAL预写入日志
    5）执行压缩
    */
    // HBase特点
    /*
    1）非关系型数据库（NoSQL），没有数据类型，不限数据种类
    2）面向列的存储，Null不会被保存，支持列独立检索
    3）可扩展性、可伸缩性、容量巨大：通过HDFS存储数据
    4）高可靠性：提供Wal机制，且HDFS保存副本Replication
    5）高可用性：HMaster HA和HRegionServer异常任务迁移
    6）主从架构，分布式
    7）单个Table的读写操作线程不安全，不能保证数据的一致性
    8）多版本数据：根据rowKey和col定位到的Value支持随意数量的版本号值，可存储变动历史记录的数据
    9）高性能：基于LSM(日志结构合并树)数据结构和rowKey有序排列，Region文件切分、主键索引和缓存机制
    使得HBase在海量数据下具备随机读取性能，该性能针对rowKey的查询能够达到毫秒级别
    */

    // HBase同一表中插入相同RowKey的数据，原来的数据将被覆盖(若Version=1)

}
