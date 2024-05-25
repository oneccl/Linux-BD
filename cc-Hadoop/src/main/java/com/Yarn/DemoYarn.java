package com.Yarn;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/20
 * Time: 10:48
 * Description:
 */
public class DemoYarn {

    // Yarn（Yet Another Resource Negotiator）:通用的资源管理系统，Hadoop三大组件之一
    // 作用：统一的资源管理和任务调度：用来管理集群中的资源及任务的生命周期
    // 架构：M-S主从模式
    // 1）ResourceManager(RM): 资源管理器：处理Client请求，监控NodeManager节点
    // 2）NodeManager(NM): 单个节点上的资源管理：接收RM指令，向RM汇报状态
    // 3）AppManager(AM): 计算切片，计算资源，执行任务
    // 4）Container: 资源容器，任务运行环境的抽象，封装了CPU、内存、环境变量、启动命令等

}
