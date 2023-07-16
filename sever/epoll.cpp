//服务器端程序
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h> /* superset of previous */
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<ctype.h>
#include"wrap.h" //自己实现的已经封装好的函数，含有错误处理
#include <sys/epoll.h>
#include<fcntl.h>
#include"dodata.h"
#include <mysql/mysql.h>
#include <string>
#include<string.h>

char ip[25]="";
void connect_database(MYSQL *mysql) {
    //定义句柄

    //初始化句柄
    if (NULL == mysql_init(mysql)) {
        printf("mysql init error!\n");
        exit(1);
        //return -1;
    }

    if (NULL == mysql_real_connect(mysql, ip, "root", "Qin1&Cheng2", "test", 0, NULL, 0)) {
        printf("%s\n", mysql_error(mysql));
        exit(1);
        //return -1;
    }
    printf("欢迎登录银行服务器端！\n");
    //设置字符集
    // mysql_set_character_set(mysql,"GBK");
}

int main(int argc,char *argv[]) {
    
    if (argc != 2) {
        printf("input error! format:./epoll 0/1,0 is master,1 is slave\n");
        return 0;
    }
    if(argv[1]=="0")
        strcpy(ip,"192.168.1.222");
    else
        strcpy(ip,"60.205.226.62");
    //创建socket
    int lfd = Socket(AF_INET, SOCK_STREAM, 0);
    //设置端口复用，记得在bind之前就设置好端口复用
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(int));
    //绑定
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1026);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    Bind(lfd, (struct sockaddr *) &addr, sizeof(addr));
    //监听
    Listen(lfd, 128);
    //创建一颗epoll树,传入一个非零数即可
    int epfd = epoll_create(1);
    struct epoll_event ev;
    ev.events = EPOLLIN;//可读事件
    ev.data.fd = lfd;
    //将监听文件描述符上树
    epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &ev);
    int nready;
    int i;
    struct epoll_event rev[1024];
    int sockfd;
    int cfd;
    char buf[1024];
    int n;
    MYSQL mysql;
    connect_database(&mysql);
    while (1) {
        //阻塞等待事件发生
        nready = epoll_wait(epfd, rev, 1024, -1);
        if (nready < 0) {
            if (errno == EINTR) {//被信号中断
                continue;
            }
            break;
        }
        for (i = 0; i < nready; i++) {
            sockfd = rev[i].data.fd;
            //客户端连接到来
            if (sockfd == lfd) {
                cfd = Accept(lfd, NULL, NULL);

                //上树，委托内核监控
                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = cfd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &ev);

                //将cfd设置为非阻塞
                int flag = fcntl(cfd, F_GETFL);
                flag |= O_NONBLOCK;
                fcntl(cfd, F_SETFL, flag);
            } else {//客户端发送数据到来
                memset(buf, 0x00, sizeof(buf));
                //n=Read(sockfd,buf,sizeof(buf));
                while (1) {//循环读数据
                    n = Read(sockfd, buf, sizeof(buf));//读数据
                    printf("n==[%d]\n", n);
                    if (n == -1) {//读完数据了
                        printf("read over,n==[%d]\n", n);
                        break;
                    } else if (n == 0 || (n < 0 && n != -1)) {//对方关闭连接，或者异常
                        close(sockfd);
                        epoll_ctl(epfd, EPOLL_CTL_DEL, sockfd, NULL);
                        perror("client closed\n");
                        break;
                    } else {//读到数据了
                        //解析数据，处理逻辑
                        //数据类型：
                        //普通客户
                        //（1）查找
                        //1-1 查看自己的余额           <-----    客户端：查看余额
                        //1-2 查看自己的交易记录   	<-----    客户端：查看交易记录
                        //1-3 查看自己的个人信息		<-----    客户端：查看信息
                        //（2）删除
                        //（3）改
                        //存钱/取钱：修改余额     <-----    客户端：存钱：存入金额；取钱：取出金额。
                        //修改个人信息？？
                        // (4)添加
                        //增加一次流水记录		<-----    客户端：存钱：存入金额；取钱：取出金额。
                        //
                        //parse_data();
                        std::string str(buf);
                        DoData doData(str, &mysql);
                        auto res = doData.analysis();
                        std::cout << "111:" << res << std::endl;
                        Write(sockfd, (res + '\n').c_str(), res.size());
                        // std::cout << "222" << buf << std::endl;
                        // printf("client: n==[%d],buf=[%s]\n",n,buf);
                        // Write(sockfd, buf, n);
                    }
                }
            }
        }
    }
    close(epfd);
    close(lfd);
    return 0;
}