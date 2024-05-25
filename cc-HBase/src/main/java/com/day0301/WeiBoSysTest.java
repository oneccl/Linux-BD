package com.day0301;

import com.day0228.HBaseCrudUtil;

import java.util.List;
import java.util.Scanner;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/2
 * Time: 11:59
 * Description:
 */
public class WeiBoSysTest {

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("当前用户微博: ");   // 创建用户
        String userRk = sc.nextLine();
        while (true) {
            System.out.println("请选择操作: 1.发送微博 2.我的微博 3.添加关注 4.我的关注与粉丝 5.查看关注的微博 6.取消关注 0.退出");
            int select = sc.nextInt();
            switch (select) {
                case 1: {
                    // 1、发布微博
                    send(sc,userRk);
                    break;
                }
                case 2: {
                    // 2、我的微博
                    System.out.println(HBaseCrudUtil.getByRowKey(WeiBoSys.Content, userRk));
                    break;
                }
                case 3: {
                    // 3、添加关注
                    addAtt(sc,userRk);
                    break;
                }
                case 4: {
                    // 4、我的关注与粉丝
                    System.out.println(HBaseCrudUtil.getByRowKey(WeiBoSys.Relation, userRk));
                    break;
                }
                case 5: {
                    // 5、查看关注的微博
                    select(sc,userRk);
                    break;
                }
                case 6: {
                    // 6、取消关注
                    removeAtt(sc,userRk);
                    break;
                }
                default: {
                    // 退出
                    return;
                }
            }
        }

    }

    public static void send(Scanner sc,String curRk){
        System.out.print("编辑微博内容: ");
        String content = sc.next();
        if (WeiBoSys.send(WeiBoSys.TextType, curRk, content)) {
            System.out.println("发送成功!");
        }
    }

    public static void addAtt(Scanner sc,String curRk){
        // a、推荐关注博主
        Set<String> users = WeiBoSys.getUsers(curRk, "1");
        System.out.println("推荐关注: " + users);
        System.out.print("请输入要关注的博主: ");
        String toAtt = sc.next();
        // b、点击/选择关注（1000关注1001）
        if (WeiBoSys.relate(curRk, toAtt)) {
            System.out.println("关注成功!");
        }
    }

    public static void removeAtt(Scanner sc,String curRk){
        // a.获取所有关注
        List<String> list = HBaseCrudUtil.getByQua(WeiBoSys.Relation, curRk, WeiBoSys.Attention, "attId");
        System.out.println("当前关注: " + list);
        // b.选择要取关的博主
        System.out.print("请选择要取关的博主: ");
        String remAtt = sc.next();
        if (WeiBoSys.imRelate(curRk, remAtt)) {
            System.out.println("取关成功!");
        }
    }

    public static void select(Scanner sc,String curRk){
        // a.获取所有关注
        List<String> list = HBaseCrudUtil.getByQua(WeiBoSys.Relation, curRk, WeiBoSys.Attention, "attId");
        System.out.println("当前关注: " + list);
        // b.选择查看
        System.out.print("请选择要查看的博主: ");
        String toLook = sc.next();
        System.out.println(HBaseCrudUtil.getByQua(WeiBoSys.Content, toLook, WeiBoSys.ContentInfo, WeiBoSys.TextType));
    }

}
