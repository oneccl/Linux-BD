package com.shell;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/29
 * Time: 18:33
 * Description:
 */
public class ShellScript {

    // shell脚本常用语法
    // shell脚本以：#!/bin/bash 开头

    /*
    1、变量（自定义变量）
    1）局部变量：变量名 = 值
    2）全局变量：export 变量名 = 值
    3）获取变量值：echo $变量名
    4）字符串(单引号)：echo '$HOME' => $HOME
    5）先解析变量，再输出(双引号)：echo "$HOME" => /root
    6）将命令的输出结果赋值给变量(反引号)：
    HOSTNAME=`hostname` 获取当前主机名，赋值给变量HOSTNAME
    7）转义字符：echo若想要转义字符发生作用，必须使用echo-e，且转义符要使用双引号""
    echo-e "\n"
    8）反斜杠：通常用于行尾作为续行

    2、特殊变量
    1）$n n为正整数，表示第n个参数；其中$0表示脚本名；当n≥10时，需要使用{}: 如${10}
    2）$# 获取参数个数，常用于循环
    3）$@ 获取命令行所有参数，每个参数区分对待
    4）$* 获取命令行所有参数，所有参数看成一个整体

    3、运算符、表达式
    语法：变量 = $[表达式]  或  变量 = $((表达式))

    4、条件判断
    基本语法：[ boolean表达式 ] 注意:表达式前后都有空格
    符号：-eq => ==     -ne => !=
         -lt => <      -le => <=
         -gt => >      -ge => >=
    例：[ 3 -le 10 ]

    5、分支/选择结构
    5.1、单分支
    方式1：
    if [ boolean表达式 ];then
       程序
    fi
    方式2：
    if [ boolean表达式 ]
    then
       程序
    fi

    5.2、多分支
    if [ boolean表达式 ]
    then
       程序
    elif [ boolean表达式 ]
    then
       程序
    else
       程序
    fi

    5.3、case匹配
    case $变量名 in
    "值1")
       若变量值为值1，则执行此程序
    ;;
    "值2")
       若变量值为值2，则执行此程序
    ;;
    ......
    *)
       若变量值都不是上面的值，则执行此程序
    ;;
    esac
    解释：1）;; => 相当于break  2）*) => 相当于default

    6、循环结构
    6.1、for循环
    for((变量初始值;循环控制条件;变量++或--))
    do
       程序
    done

    例：求0~n间所有整数和
    #!/bin/bash
    for((i=0;i<=$1;i++))
    do
       sum=$[ $sum+$i ]
    done
    echo $sum

    6.2、for推导式
    for elem(变量) in arr(数组(多个变量使用空格隔开并;结束))
    do
       程序
    done

    6.3、while循环
    while [ boolean表达式 ]
    do
       程序
    done

    例：求0~n间所有整数和
    #!/bin/bash
    i = 0
    #!/bin/bash
    i=0
    while [ $i -le $1 ]
    do
        sum=$[ $sum+$i ]
        i=$[ $i+1 ]
    done
    echo $sum

    7、函数（自定义函数: []表示可省略；return只能返回0-255的数字）
    [function] 函数名[()]
    {
       函数体;
       [return 0~255;]
    }

    例：计算两数之和
    #!/bin/bash
    function add()
    {
       z=$[ $1+$2 ]
       echo $z
    }
    read -p "请输入x = " x
    read -p "请输入y = " y
    sum=$(add $x $y)
    echo "x+y = "$sum
    */

}
