package com.linux;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/26
 * Time: 21:16
 * Description:
 */
public class LinuxCmd {

    // Linux常用指令
    // 通用格式：命令名称 -选项 参数

    /*
    1、进入文件夹：cd
    当前路径：cd .
    返回上级目录：cd ..
    切换路径：cd -
    根目录：cd /
    进入指定文件夹：cd 绝对/相对路径
    进入当前用户的根/家目录：cd ~

    2、浏览文件夹内容：ls
    查看目录下列表详情：ls -l 或 ll
    查看所有文件（包括隐藏文件）：ls -a
    查看所有文件详情如大小（以人类的方式）：ll -h

    3、查看当前目录路径：pwd

    4、创建目录：mkdir
    新建目录：mkdir 文件夹名称
    递归创建多级目录：mkdir -p/-r d1/d2/...

    5、创建文件：touch
    touch 文件名称

    6、删除文件或目录：rm
    递归删除：rm -r 目录或文件
    强制删除：rm -rf 目录或文件
    删除目录：rmdir 目录

    7、移动文件、修改文件名：mv
    移动文件：mv 路径1 路径2
    修改文件名：mv 文件1 文件2

    8、复制文件或目录：cp
    复制：cp 文件路径 指定路径
    递归复制：cp -r 目录路径 指定路径

    9、查看文件：
    显示内容(顺序)：cat 文件名称
    显示内容(倒序)：tac 文件名称
    查看文件开头n行内容：head -n 文件名
    查看文件最后n行内容：tail -n 文件名
    分页显示(一次n行)：more -n 文件名
    分页显示(一次n行)：less -n 文件名

    10、统计文件、scp文件发送
    统计文本行数(-l)、字数(-w)、字符数(-m)：wc
    文件发送：scp -r 文件路径 主机名:路径

    11、查找文件是否存在
    find 路径 -name 文件名

    12、编辑文件：vi/vim 文件名
    创建并编辑文件：vi/vim 文件名
    命令模式：yy复制行内容；dd删除行内容；p粘贴
    编辑/输入模式：
    末行模式：:q退出；:q!强制退出；:wq保存退出；:wq!强制保存退出；:set nu显示行号

    13、用户
    切换用户（Switch User）：su 用户名（若未指定用户名，会默认切换到超级用户）
    切换到超级用户执行（Super User Do）：sudo 命令（作为超级用户执行命令）
    添加新用户：useradd 用户名
    删除用户：userdel 用户名
    删除用户及其登陆信息：userdel -rf 用户名
    修改当前用户密码：passwd 密码
    修改用户名：usermod -l 新用户名 原用户名
    修改用户所属分组：usermod -g 新组名称 用户名

    14、用户组
    新增组名：groupadd 组名
    递归修改所属用户和组：chown -R 所属用户:所属组 文件或目录
    删除组(-f强制删除)：groupdel [-f] 组名

    15、权限操作
    权限：读r、写w、执行x
    权限模式：所属用户u、所属组g、其它o
    开头类型：文件夹d、文件-、link链接l
    递归修改权限：chmod -R 权限模式+权限（例：chmod -R a+x 文件或目录）a代表全部

    16、过滤筛选、反选：grep
    过滤：结果 | grep 相关字段 （其中|为管道）
    反选：结果 | grep -v 字段 （取除了该字段的其它）
    查找文件关键字所在行：grep "关键字(如ERROR)" 文件路径

    17、选择指定列输出：awk '{print $1,$2,...}'
    awk处理文本：awk '{print $0,...}' 文件路径(默认分割符为空格)
    awk处理文本：awk  -F ':' '{print $0,...}' 文件路径(默认分割符为:)
    其它文本相关：
    文本默认排序(以第一列字符的ASCII排序)、-k指定第n列字符排序：sort 文件路径 [-k n]
    按照数值大小排序：sort -n
    按相反的顺序排序：sort -r
    按照数值大小反序：sort -nr
    文本去重并排序显示次数：sort | uniq -c

    18、修改替换：sed
    sed -i 's/原属性名=原属性值/原属性名=新属性值/' 文件路径

    19、将左侧内容作为右侧指令的参数：结果 | xargs 指令

    20、任务管理器
    查看cpu和内存占用情况：top（按1切换，按q退出）
    查看磁盘容量、使用情况：df -h
    查看文件大小(不是真正的字节数，而是占用的磁盘大小)：du -h
    打印当前内存使用情况：free

    21、进程管理
    显示进程：ps -ef/-aux
    杀死进程：kill -9 进程ID
    查找进程：ps -ef | grep 相关进程名
    杀死指定进程：ps -ef | grep 进程名 | grep -v grep | awk '{print $2}' | xargs kill -9
    杀死指定进程：jps | grep 服务名 | awk '{print$1}' | xargs kill -9

    22、系统服务：systemctl
    启动服务：systemctl start 服务名
    查看服务状态：systemctl status 服务名（如firewalld）
    关闭服务：systemctl stop 服务名（如firewalld）
    重启服务：systemctl restart 服务名 / systemctl service 服务名 restart
    设置开机自启：systemctl enable 服务名
    禁止开启自启：systemctl disable 服务名

    23、网络
    查看当前正在运行的网络设备：ifconfig 或 ip addr
    测试网络的联通性：ping 主机名/IP
    当前主机开启一个网络端口：nc -l -k port
    通过端口监听查看端口是否占用(服务是否启动)：netstat -anp | grep port
    查看端口所在的服务(pid)：fuser -n tcp port
    重启网卡：systemctl service network restart

    24、SSH免密登录
    1）删除old锁和钥匙：rm -rf /root/.ssh
    2）配钥匙（每台机器都配）(2次回车)：ssh-keygen -t rsa
    3）发钥匙（每台机器都发）：ssh-copy-id 主机名
    4）免密连接：ssh 主机名/IP

    25、主机名和echo写入
    修改主机名：hostnamectl set-hostname 主机名/FQDN全域名
    配置主机FQDN：echo 'FQDN全域名' >> /etc/sysconfig/network
    配置本地DNS映射：echo -e 'ip FQDN hostname' >> /etc/hosts

    26、关机：poweroff 重启：reboot

    27、解压缩
    压缩（后缀为gz）：gzip 文件/目录
    解压到指定目录下：tar zxvf 压缩文件名 -C 指定目录

    28、下载
    wget 下载链接/路径

    29、yum（CentOS中的一个软件库）使用
    安装软件：yum install 软件名
    卸载软件：yum remove 软件名
    例1：安装文件树形结构显示插件：yum install -y tree
    例2：安装apache httpd服务：yum install -y httpd
    默认http配置文件目录: /etc/httpd/conf/httpd.conf
    默认http网页或文件存放在: /var/www/html

    30、其它
    帮助：help 指令
    清屏：clear 或 ctrl+l
    强行退出：ctrl+c
    查看历史：history

    31、补充
    查找位置文件：locate my.cnf
    查看服务列表：chkconfig --list
    */

    // 32、curl
    /*
    CURL（CommandLine Uniform Resource Locator）是一个利用URL语法，在命令行终端下使用的网络请求工具，支持HTTP、HTTPS、FTP等协议
    Linux、MAC系统一般默认已安装好CURL，可直接在终端使用；Windows系统下载地址: https://curl.haxx.se/windows/，下载解压后的可执行文件在bin下
    语法：curl [option] <url>
    // 1）Get请求：
    curl url                                       返回响应内容
    curl -v url                                    返回通信过程、头信息、响应内容等
    curl url -o file(file_name.suffix)             指定文件下载
    curl url/file(file_path) -O                    下载文件
    curl -A "Mozilla/5.0" -e url file_path -O      指定User-Agent和Referer请求头下载文件
    curl -H "Authorization: value" file_path -O    指定Authorization请求头下载文件
    // 2）Post请求：
    // 2.1）POST提交JSON格式数据
    curl -H "Content-Type: application/json" \
         -d '{"k1": "v1", "k2": "v2"}' \
         url
    // 2.2）POST提交表单数据
    curl -F "name1=value1" \
         -F "name2=value2" \
         url
    */

}
