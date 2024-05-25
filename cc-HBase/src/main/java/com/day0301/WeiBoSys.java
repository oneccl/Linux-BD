package com.day0301;

import com.day0228.HBaseCrudUtil;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/1
 * Time: 12:09
 * Description:
 */
public class WeiBoSys {

    public static final String Content = "weibo:content";    // 内容表
    public static final String ContentInfo = "contentInfo";

    public static final String Relation = "weibo:relation";  // 关系表
    public static final String Fans = "fans";
    public static final String Attention= "attention";

    public static final String Messages = "weibo:messages";  // 粉丝收件箱(保存关注rk)
    public static final String MsgInfo = "msgInfo";

    public static final String TextType = "text";            // 内容类型
    public static final String ImageType = "image";
    public static final String LinkType = "link";

    public static void main(String[] args) {
        //HBaseCrudUtil.listNamespace();
        //HBaseCrudUtil.list();
        // 创建namespace
        //System.out.println(HBaseCrudUtil.createNamespace("weibo"));
        // 创建微博内容表
        //System.out.println(HBaseCrudUtil.createTable(Content, ContentInfo));
        // 创建关系表
        //System.out.println(HBaseCrudUtil.createTable(Relation, Fans, Attention));
        // 创建用户收件箱表
        //System.out.println(HBaseCrudUtil.createTable(Messages, MsgInfo));
        // 清空表数据
//        System.out.println(HBaseCrudUtil.truncate(Content));
//        System.out.println(HBaseCrudUtil.truncate(Relation));
//        System.out.println(HBaseCrudUtil.truncate(Messages));

        System.out.println(HBaseCrudUtil.get(Content));
        System.out.println(HBaseCrudUtil.get(Relation));
        System.out.println(HBaseCrudUtil.get(Messages));

        // 1、发布微博
//        System.out.println(send(TextType, "1001", "content5"));
//        System.out.println(HBaseCrudUtil.get(Content));

        // 2、我的微博
//        System.out.println(HBaseCrudUtil.getByRowKey(Content, "1000"));
//        System.out.println(HBaseCrudUtil.getByRowKey(Content, "1001"));

        // 3、添加关注
        // a、推荐关注博主
//        Set<String> users = getUsers("1000", "1");
//        System.out.println("推荐关注: " + users);
        // b、点击/选择关注（1000关注1001）
        //System.out.println(relate("1000", "1001"));
        // c、显示关注(1000的关注)
//        List<String> ls1 = HBaseCrudUtil.getByQua(Relation, "1000", Attention, "attId");
//        System.out.println("当前关注: " + ls1);

        // 4、关注与粉丝(1001的粉丝1000)
        //System.out.println(HBaseCrudUtil.getByRowKey(Relation, "1001"));

        // 5、查询关注的博主微博(1000查看1001所有微博)
        //System.out.println(HBaseCrudUtil.getByQua(Content, "1001", ContentInfo, TextType));

        // 6、取关(1000取关1001)
//        System.out.println(imRelate("1000", "1001"));
//        System.out.println(HBaseCrudUtil.get(Relation));
//        System.out.println(HBaseCrudUtil.get(Messages));

    }


    // 发布微博(text)
    public static Boolean send(String type,String rk,String text){
        // 1、发送一条微博
        Boolean sendMsg = HBaseCrudUtil.put(Content, rk, ContentInfo, type,text);
        // 2、获取发微博者的粉丝
        List<String> fans = HBaseCrudUtil.getByQua(Relation, rk, Fans, "attId");
        // 3、将该博主rk保存到粉丝收件箱
        boolean putMsg = true;
        if (fans!=null)
        for (String fan : fans) {
            putMsg = HBaseCrudUtil.put(Messages, fan, MsgInfo, "attRk",rk);
            if (!putMsg){
                break;
            }
        }
        return sendMsg && putMsg;
    }

    // 推荐关注 curRk：当前用户 rk：推荐策略
    public static Set<String> getUsers(String curRk,String rk){
        String rks = HBaseCrudUtil.getByRowKey(Content, rk);
        Set<String> set = new HashSet<>();
        if (rks!=null)
        for (String r : rks.split("\n")) {
            set.add(r.split("\t")[0]);
        }
        set.remove(curRk);  // 排除当前用户
        return set;
    }

    // 关注 curRk：当前用户 attRk：当前用户的关注
    public static Boolean relate(String curRk, String attRk){
        // 1、当前用户添加关注
        Map<String, Object> curMap = new HashMap<>();
        curMap.put("attId",attRk);
        Boolean att = HBaseCrudUtil.put(Relation, curRk, Attention, curMap);
        // 2、关系表用户粉丝显示
        Map<String, Object> attMap = new HashMap<>();
        attMap.put("attId",curRk);
        Boolean putFan = HBaseCrudUtil.put(Relation, attRk, Fans, attMap);
        // 3、将关注博主的rk保存到粉丝/当前用户的收件箱
        Boolean putMsg = HBaseCrudUtil.put(Messages, curRk, MsgInfo, "attRk",attRk);
        return att && putFan && putMsg;
    }

    // 取消关注 curRk：当前用户 attRk：要取消的关注
    public static Boolean imRelate(String curRk,String attRk){
        // 1、删除关系表数据
        // a.删除当前用户关注的博主
        Boolean att = HBaseCrudUtil.delByFamRk(Relation, Attention, curRk);
        // b.将当前用户从粉丝中移除
        Boolean fan = HBaseCrudUtil.delByFamRk(Relation, Fans, attRk);
        // 2、删除收件箱中关注博主的rowKey
        Boolean msg = HBaseCrudUtil.delByRowKey(Messages, attRk);
        return att && fan && msg;
    }

}
