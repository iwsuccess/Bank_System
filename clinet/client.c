#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<sys/socket.h>
#include<netinet/in.h>

#define  SERVER_PORT 1026
#define  SERVER_IP "127.0.0.1"

int main(int argc, char *argv[]) { //第一个参数是终端输入的字符串个数，第二个存放字符串
    int sockfd;
    //创建信箱
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    //准备信封
    struct sockaddr_in serv_addr;
    //内存清理
    memset(&serv_addr, '\0', sizeof(struct sockaddr_in));
    //协议家族
    serv_addr.sin_family = AF_INET;
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);
    serv_addr.sin_port = htons(SERVER_PORT);
    connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr));
    int n;
    char buf[1024];
    printf("欢迎登录银行客户端！\n");
    printf("请操作：执行完毕后ctrc+c退出！\n");
    while (1) {
        memset(buf, 0x00, sizeof(buf));
        n = Read(STDIN_FILENO, buf, sizeof(buf));
        Write(sockfd, buf, n);
        memset(buf, 0x00, sizeof(buf));
        n = Read(sockfd, buf, sizeof(buf));
        printf("n=[%d],buf=[%s]\n", n, buf);
    }
    close(sockfd);
    return 0;
}