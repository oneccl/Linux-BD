package day0407;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/7
 * Time: 11:31
 * Description:
 */
public class DemoElasticsearch {

    // ES使用（因安全问题，ES不允许root用户运行）
    /*
    # 创建组
    # groupadd es
    # 创建用户并添加到组中
    # useradd es -g es
    # 修改es安装包所有者为es
    # chown -R es:es /opt/module/elasticsearch-7.5.2
    # 切换用户
    # su es
    # 进入bin目录
    # cd /opt/module/elasticsearch-7.5.2/bin
    # 执行elasticsearch（每个节点都启动）
    # elasticsearch
    # 后台启动
    # elasticsearch -d
    # 查看es集群状态
    # curl -XGET 'http://bd91:9200/_cluster/health?pretty'
    */
    // Kibana: Elastic公司提供的可操作ES的Web-UI组件
    /*
    # 修改kibana安装包所有者为es
    # chown -R es:es /opt/module/kibana-7.5.2
    # 切换用户
    # su es
    # 进入bin目录
    # cd /opt/module/kibana-7.5.2/bin
    # 启动
    # kibana
    # 后台启动
    # kibana &
    # 访问Web-UI
    # bd91:5601
    */
    // Web-UI操作
    /*
    1、点击Dev Tools
    2、查看集群健康状态
    GET /_cluster/health
    3、查看索引列表
    GET /_cat/indices
    4、创建索引
    PUT student
    {
      "mappings": {
        "properties": {
          "id":{
            "type": "integer"
          },
          "name":{
            "type": "text"
          },
          "age":{
            "type": "integer"
          }
        }
      }
    }
    5、插入数据
    POST student/_doc/1
    {
      "id":1,
      "name":"Tom",
      "age":18
    }
    */

}
