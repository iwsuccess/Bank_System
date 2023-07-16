#include<stdio.h>
#include<stdlib.h>
#include <mysql/mysql.h>
#include<string.h>
#include<time.h>
#include<sys/time.h>
#include<math.h>
#include <unistd.h>
#include <errno.h>
#include<sys/types.h>
#include<sys/wait.h>
#include<fcntl.h>
#include<pthread.h>
#include <assert.h>
#include <libxml/xmlmemory.h>
#include <libxml/parser.h>

#define DEFAULT_XML_FILE "mysql.xml"
#define NAME_STR_LEN         32
#define TEL_STR_LEN         128
#define ADDR_STR_LEN         128
#define IP_LEN 25
#define filename1 "data/user_info_2.txt"
//#define filename2 "new_data/账户5.txt"   //账户1.txt
#define filename2 "data/user_account.txt"   //账户1.txt
#define filename4 "data/user_info_1.txt"
char filename_final[256] = "new_data/批量入金1.txt";

char name[NAME_STR_LEN];     //用户
char password[TEL_STR_LEN];       //密码
char database[ADDR_STR_LEN];  //数据库
char ip[IP_LEN];// ip

//账户信息表
struct account_file_info {
    char account[30];
    char status[30];
    char opendate[30];
    char lastexchange[30];
    float balance;
    char uid[30];
    float salary;
};

//账户流水信息表
struct account_file_liquid_info {
    char account[30];
    int time;
    //char time[30];
    char liquidnum[30];
    char date[30];
    float exchange;
    float balance;

};

//userinfo1表
struct user {
    char uid[30];
    char name[30];
    char address[30];
    char post[30];
    char phone[30];
};

//用户信息2表
struct user2 {
    char uid[30];
    char idcard[30];
    char birthday[30];
    char sex[30];
    char marry[30];
    char country[30];
    char job[30];
};

struct user u;//userinfor1
struct user2 u2;
struct account_file_info a;
struct account_file_liquid_info al;

int count_account_file = 0;
int count_account_liquid_file = 0;
int count_user_info2_file = 0;
int count_user_info1_file = 0;


//解析每一个phone，提取出name、tel、address
static int parse_phone(xmlDocPtr doc, xmlNodePtr cur) {
    assert(doc || cur);
    xmlChar *key;

    cur = cur->xmlChildrenNode;
    while (cur != NULL) {
        //获取name
        if ((!xmlStrcmp(cur->name, (const xmlChar *) "user"))) {
            key = xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            strcpy(name, key);
            //printf("name: %s\t", key);
            xmlFree(key);
        }
        //获取tel
        if ((!xmlStrcmp(cur->name, (const xmlChar *) "password"))) {
            key = xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            strcpy(password, key);
            //printf("password: %s\t", key);
            xmlFree(key);
        }
        //获取address
        if ((!xmlStrcmp(cur->name, (const xmlChar *) "database"))) {
            key = xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            strcpy(database, key);
            //printf("address: %s\n", key);
            xmlFree(key);
        }
        //获取ip
        if ((!xmlStrcmp(cur->name, (const xmlChar *) "ip"))) {
            key = xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            strcpy(ip, key);
            //printf("address: %s\n", key);
            xmlFree(key);
        }
        cur = cur->next;
    }
    return 0;
}

static int parse_phone_book(const char *file_name) {
    assert(file_name);

    xmlDocPtr doc;   //xml整个文档的树形结构
    xmlNodePtr cur;  //xml节点
    xmlChar *id;     //phone id

    //获取树形结构
    doc = xmlParseFile(file_name);
    if (doc == NULL) {
        fprintf(stderr, "Failed to parse xml file:%s\n", file_name);
        goto FAILED;
    }

    //获取根节点
    cur = xmlDocGetRootElement(doc);
    if (cur == NULL) {
        fprintf(stderr, "Root is empty.\n");
        goto FAILED;
    }

    if ((xmlStrcmp(cur->name, (const xmlChar *) "mysql_info"))) {
        fprintf(stderr, "The root is not mysql_info.\n");
        goto FAILED;
    }

    //遍历处理根节点的每一个子节点
    cur = cur->xmlChildrenNode;
    while (cur != NULL) {
        if ((!xmlStrcmp(cur->name, (const xmlChar *) "info"))) {
            id = xmlGetProp(cur, "id");
            //printf("id:%s\t",id);
            parse_phone(doc, cur);
        }
        cur = cur->next;
    }
    xmlFreeDoc(doc);
    return 0;
    FAILED:
    if (doc) {
        xmlFreeDoc(doc);
    }
    return -1;
}

int get_mysql_info() {
    char *xml_file = DEFAULT_XML_FILE;
    if (parse_phone_book(xml_file) != 0) {
        fprintf(stderr, "Failed to parse mysql info.\n");
        return -1;
    }
    // printf("user:%s\n",name);
    // printf("password:%s\n",password);
    // printf("database:%s\n",database);
    //  printf("ip:%s\n",ip);
    return 0;

}

int flag;
void sighander(int signo) {
    //printf("signo=[%d]\n",signo);
    flag = 1;
    printf("data insert success!\n");

}

char *get_stime(void) {
    static char timestr[200] = {0};
    struct tm *pTempTm;
    struct timeval time;
    gettimeofday(&time, NULL);
    pTempTm = localtime(&time.tv_sec);
    if (NULL != pTempTm) {
        snprintf(timestr, 199, "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
                 pTempTm->tm_year + 1900,
                 pTempTm->tm_mon + 1,
                 pTempTm->tm_mday,
                 pTempTm->tm_hour,
                 pTempTm->tm_min,
                 pTempTm->tm_sec,
                 time.tv_usec / 1000);
    }
    return timestr;
}

//数据库连接
void connect_database(MYSQL *mysql) {
    //定义句柄
    //初始化句柄
    if (NULL == mysql_init(mysql)) {
        printf("mysql init error!\n");
        exit(1);
        //return -1;
    }
    if (NULL == mysql_real_connect(mysql, ip, name, password, database, 0, NULL, 0)) {
        printf("%s\n", mysql_error(mysql));
        exit(1);
        //return -1;
    }
    printf("连接数据库成功！\n");
    //设置字符集
    mysql_set_character_set(mysql, "utf8");
}
void connect_database_GBK(MYSQL *mysql) {
    //定义句柄

    //初始化句柄
    if (NULL == mysql_init(mysql)) {
        printf("mysql init error!\n");
        exit(1);
        //return -1;
    }

    if (NULL == mysql_real_connect(mysql, ip, name, password, database, 0, NULL, 0)) {
        printf("%s\n", mysql_error(mysql));
        exit(1);
        //return -1;
    }
    printf("连接数据库成功！\n");
    //设置字符集
    mysql_set_character_set(mysql, "GBK");
}

//数据插入
int database_op(MYSQL mysql, int type) {
    char sql[1024];
    char sql_liquid[1024];
    if (type == 0)
        sprintf(sql, "insert ignore into userinfo2 values('%s','%s','%s','%s','%s','%s','%s')", u2.uid, u2.idcard,
                u2.birthday, u2.sex, u2.marry, u2.country, u2.job);
    else if (type == 1)
        sprintf(sql, "insert ignore into newaccountfile values('%s','%s','%s','%s','%f','%s','%f')", a.account,
                a.status, a.opendate, a.lastexchange, a.balance, a.uid, a.salary);
    else if (type == 2)
        	sprintf(sql, "insert ignore into newaccountliquidfile values('%s','%d','%s','%s','%f')",al.account,al.time,al.liquidnum,al.date,al.exchange);
    else if (type == 3)
        sprintf(sql, "insert ignore into userinfo1 values('%s','%s','%s','%s','%s')", u.uid, u.name, u.address, u.post,
                u.phone);
    if (0 != mysql_query(&mysql, sql)) {
        printf("%s\n", mysql_error(&mysql));
        return -1;
    }
    if (type == 2) {
        sprintf(sql_liquid, "UPDATE newaccountfile SET balance = balance + %f WHERE account = '%s';", al.exchange,
                al.account);
        if (0 != mysql_query(&mysql, sql_liquid)) {
            printf("%s\n", mysql_error(&mysql));
            return -1;
        }
    }
    return 0;
    //printf("数据插入成功！\n");
}

//解析一行数据，按空格分割，返回值放入res
void parse_line(char *input, char *res[]) {
    char *p = strtok(input, " ");
    int i = 0;
    if (p) {
        res[i++] = p;
    }
    while (p = strtok(NULL, " ")) {//使用第一个参数为NULL来提取子串
        res[i++] = p;
    }
}
//解析日期数据，处理12/31/1899----->18991231
void parse_date(char *old_date, char *new_date) {
    char *p = strtok(old_date, "/");
    char *res[3];
    int i = 0;
    if (p) {
        res[i++] = p;
        //printf("%s\n",p);
    }
    while (p = strtok(NULL, "/")) {//使用第一个参数为NULL来提取子串
        res[i++] = p;
        //printf("%s\n",p);
    }
    strcpy(new_date, res[2]);
    strcat(new_date, res[0]);
    strcat(new_date, res[1]);
    //printf("%s\n",new_date);
}
//账户流水表插入数据库

void write_account_liquid_file(FILE *out) {
    FILE *fp = fopen(filename_final, "r");
    if (fp == NULL) {
        fprintf(out, "cannot open the file!\n");
        exit(1);
    }
    fprintf(out, "*************begin write_account_liquid_file**************\n");
    char buf[1024];
    char my_date[16];
    char des[30];
    char des_balance[30];
    int counts = 0;
    int k;
    //定义数据库连接句柄
    MYSQL mysql;
    connect_database(&mysql);
    //mysql_autocommit(&mysql, 0);//关闭自动提交

    while (!feof(fp)) {
        if ((counts != 1 && counts % 50000 == 1) || counts == 0) {

            char sql[1024];
            fprintf(out, "开启一个事务\n");
            sprintf(sql, "START TRANSACTION;");
            if (0 != mysql_query(&mysql, sql)) {
                printf("%s\n", mysql_error(&mysql));
                return;
            }
        }
        count_account_liquid_file++;
        memset(buf, 0x00, sizeof(buf));
        memset(my_date, 0x00, sizeof(my_date));
        char *res[6];
        fgets(buf, 1024, fp);
        if (count_account_liquid_file < 4) {
            counts++;
            continue;
        }
        parse_line(buf, res);
        for (k = 0; k < 6; k++) {
            if (strcmp(res[k], "\r\n") == 0 || strcmp(res[k], "\r") == 0) {
                break;
            }
        }
        if (k < 5)
            continue;
        //al.account,al.time,al.liquidnum,al.date,al.exchange,al.balance
        strcpy(al.account, res[0]);
        //printf("[%s]\n",al.account);
        al.time = atoi(res[1]);
        //strcpy(al.time,res[1]);
        //printf("time=[%s]\n",al.time);
        strcpy(al.liquidnum, res[2]);
        //printf("[%s]\n",al.liquidnum);
        //parse_date(res[3],my_date);
        strcpy(al.date, res[3]);
        //printf("[%s]\n",al.date);
        // char des[30];
        // memset(des, 0x00, sizeof(des));
        // sprintf(des,res[4][0]);
        // if (res[4][0] == '+') {
        //     ++res[4];
        //     if (res[4][1] == '.') {
        //         sprintf(des, "0%s", res[4]);
        //     } else {
        //         sprintf(des, "%s", res[4]);
        //     }
        //     al.exchange = atof(des);
        // } else {
        //     ++res[4];
        //     if (res[4][1] == '.') {
        //         sprintf(des, "0%s", res[4]);
        //     } else {
        //         sprintf(des, "%s", res[4]);
        //     }
        //     al.exchange = -atof(des);
        // }
         al.exchange = atof(res[4]);
        // memset(des_balance,0x00,sizeof(des_balance));
        // if(res[5][0]=='+'){
        // 	++res[5];
        // 	if(res[5][1]=='.')
        // 	{
        // 		sprintf(des_balance,"0%s",res[5]);
        // 	}
        // 	else{
        // 		sprintf(des_balance,"%s",res[5]);
        // 	}
        // 	al.balance=atof(des_balance);
        // }
        // else{
        // 	++res[5];
        // 	if(res[5][1]=='.')
        // 	{
        // 		sprintf(des_balance,"0%s",res[5]);
        // 	}
        // 	else{
        // 		sprintf(des_balance,"%s",res[5]);
        // 	}
        // 	al.balance=-atof(des_balance);
        // }
        database_op(mysql, 2);
        if (counts != 0 && counts % 50000 == 0) {
            if (0 != mysql_query(&mysql, "COMMIT;")) {
                fprintf(out, "%s\n", mysql_error(&mysql));
                return;
            }
            fprintf(out, "第%d插入成功！\n", counts);
            //sleep(10);
        }
        counts++;
    }
    if (0 != mysql_query(&mysql, "COMMIT;")) {
        fprintf(out, "%s\n", mysql_error(&mysql));
        return;
    }
    fprintf(out, "第%d插入成功！\n", counts);
    //释放数据库连接
    mysql_close(&mysql);
    fclose(fp);
}

//账户信息表插入数据库
void write_account_file(FILE *out) {
    FILE *fp1 = fopen(filename2, "r");
    if (fp1 == NULL) {
        fprintf(out, "cannot open the file!\n");
        exit(1);
    }
    fprintf(out, "*************write_account_file**************\n");
    //  FILE *fp;
    // fp=fopen(filename2,"r");
    // if(fp==NULL) 
    //  {   
    //      printf("cannot open the file!\n");  
    //      exit(1);  
    //  }
    char buf[1024];
    char open_data[16];
    char last_data[16];
    int k;
    int counts = 0;
    //定义数据库连接句柄
    MYSQL mysql;
    connect_database(&mysql);
    while (!feof(fp1)) {
        if (counts % 100000 == 1 || counts == 0) {
            char sql[1024];
            fprintf(out, "开启一个事务\n");
            sprintf(sql, "START TRANSACTION;");
            if (0 != mysql_query(&mysql, sql)) {
                fprintf(out, "%s\n", mysql_error(&mysql));
                return;
            }
        }
        count_account_file++;
        memset(buf, 0x00, sizeof(buf));
        // for(int j=0;j<7;j++)
        // {
        // 	res[j]="#";
        // }
        char *res[7];
        fgets(buf, 1024, fp1);
        if (count_account_file < 4)
            continue;
        parse_line(buf, res);
        // for(int t=0;t<7;t++)
        // 	printf("len_res=[%d],res=%s\n",(int)strlen(res[t]),res[t]);
        for (k = 0; k < 7; k++) {
            if (strcmp(res[k], "\r\n") == 0 || strcmp(res[k], "") == 0) {
                break;
            }
        }
        if (k < 7)
            continue;
        //a.account,a.status,a.opendate,a.lastexchange,a.balance,a.uid,a.salary
        strcpy(a.account, res[0]);
        strcpy(a.status, res[1]);
        parse_date(res[2], open_data);
        strcpy(a.opendate, open_data);
        parse_date(res[3], last_data);
        strcpy(a.lastexchange, last_data);
        a.balance = atof(res[4]);
        strcpy(a.uid, res[5]);
        a.salary = atof(res[6]);
        database_op(mysql, 1);
        if ((counts != 0 && counts % 100000 == 0)) {
            if (0 != mysql_query(&mysql, "COMMIT;")) {
                fprintf(out, "%s\n", mysql_error(&mysql));
                return;
            }
            fprintf(out, "第%d插入成功！\n", counts);
            //sleep(10);
        }
        counts++;
    }
    if (0 != mysql_query(&mysql, "COMMIT;")) {
        fprintf(out, "%s\n", mysql_error(&mysql));
        return;
    }
    fprintf(out, "第%d插入成功！\n", counts);
    //释放数据库连接
    mysql_close(&mysql);
    fclose(fp1);
}

//用户信息1写入数据库
void write_user_info1_file(FILE *out) {
    FILE *fp4 = fopen(filename4, "r");
    if (fp4 == NULL) {
        fprintf(out, "cannot open the file!\n");
        exit(1);
    }
    fprintf(out, "*************user_info1_file**************\n");
    char buf[1024];
    int i = 0;
    //char new_birth[16];
    int k;
    int counts = 0;
    //定义数据库连接句柄
    MYSQL mysql;
    connect_database_GBK(&mysql);
    while (!feof(fp4)) {
        if (counts % 100000 == 1 || counts == 0) {
            char sql[1024];
            fprintf(out, "开启一个事务\n");
            sprintf(sql, "START TRANSACTION;");
            if (0 != mysql_query(&mysql, sql)) {
                fprintf(out, "%s\n", mysql_error(&mysql));
                return;
            }
        }
        i++;
        memset(buf, 0x00, sizeof(buf));
        // for(int j=0;j<7;j++)
        // {
        // 	res[j]="#";
        // }
        char *res[5];
        fgets(buf, 1024, fp4);
        if (i < 4)
            continue;
        //printf("len_buf=[%d], buf=[%s]\n",(int)strlen(buf),buf);
        parse_line(buf, res);
        // for(int t=0;t<7;t++)
        // 	printf("len_res=[%d],res=%s\n",(int)strlen(res[t]),res[t]);
        for (k = 0; k < 5; k++) {
            if (strcmp(res[k], "\r\n") == 0 || strcmp(res[k], "") == 0) {
                break;
            }
        }
        //printf("%d\n",k);
        if (k < 5)
            continue;
        strcpy(u.uid, res[0]);
        strcpy(u.name, res[1]);
        strcpy(u.address, res[2]);
        strcpy(u.post, res[3]);
        strcpy(u.phone, res[4]);
        database_op(mysql, 3);

        if (counts != 0 && counts % 100000 == 0) {
            if (0 != mysql_query(&mysql, "COMMIT;")) {
                fprintf(out, "%s\n", mysql_error(&mysql));
                return;
            }
            fprintf(out, "第%d插入成功！\n", counts);
            //sleep(10);
        }
        counts++;
    }
    if (0 != mysql_query(&mysql, "COMMIT;")) {
        fprintf(out, "%s\n", mysql_error(&mysql));
        return;
    }
    fprintf(out, "第%d插入成功！\n", counts);
    //释放数据库连接
    mysql_close(&mysql);
    fclose(fp4);
    return;
}

//用户信息2写入数据库
void write_user_info2_file(FILE *out) {
    FILE *fp3;
    fp3 = fopen(filename1, "r");
    if (fp3 == NULL) {
        fprintf(out, "cannot open the file!\n");
        exit(1);
    }
    fprintf(out, "*************write_user_info2_file**************\n");
    char buf[1024];
    char new_birth[16];
    int k;
    int counts = 0;
    //定义数据库连接句柄
    MYSQL mysql;
    connect_database(&mysql);
    while (!feof(fp3)) {
        if (counts % 100000 == 1 || counts == 0) {
            char sql[1024];
            fprintf(out, "开启一个事务\n");
            sprintf(sql, "START TRANSACTION;");
            if (0 != mysql_query(&mysql, sql)) {
                fprintf(out, "%s\n", mysql_error(&mysql));
                return;
            }
        }
        count_user_info2_file++;
        memset(buf, 0x00, sizeof(buf));
        char *res[7];
        fgets(buf, 1024, fp3);
        if (count_user_info2_file < 4)
            continue;
        parse_line(buf, res);
        for (k = 0; k < 7; k++) {
            if (strcmp(res[k], "\r\n") == 0 || strcmp(res[k], "") == 0) {
                break;
            }
        }
        if (k < 7)
            continue;

        strcpy(u2.uid, res[0]);
        strcpy(u2.idcard, res[1]);
        parse_date(res[2], new_birth);
        strcpy(u2.birthday, new_birth);
        strcpy(u2.sex, res[3]);
        strcpy(u2.marry, res[4]);
        strcpy(u2.country, res[5]);
        strcpy(u2.job, res[6]);

        //每写入10万条数据，重新关闭数据库，再连接数据库
        // if(i%100000==0)
        // {
        // 	mysql_close(&mysql);
        // 	//sleep(5);
        // 	connect_database(&mysql);
        // }
        database_op(mysql, 0);
        if (counts != 0 && counts % 100000 == 0) {
            if (0 != mysql_query(&mysql, "COMMIT;")) {
                fprintf(out, "%s\n", mysql_error(&mysql));
                return;
            }
            fprintf(out, "第%d插入成功！\n", counts);
            //sleep(10);
        }
        counts++;
    }

    if (0 != mysql_query(&mysql, "COMMIT;")) {
        fprintf(out, "%s\n", mysql_error(&mysql));
        return;
    }
    fprintf(out, "第%d插入成功！\n", counts);
    //释放数据库连接
    mysql_close(&mysql);
    fclose(fp3);
}


int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("input error! format:./test_arg /xxx/xxx/xxx.txt\n");
        return 0;
    }
    strcpy(filename_final, argv[1]);
    printf("file_path:%s\n", filename_final);
    int ret = get_mysql_info();
    if (ret == -1) {
        printf("xml parse failed!\n");
        return -1;
    }
    flag = 0;
    pid_t pid = fork();
    if (-1 == pid) {
        printf("fork process failed. errno: %u, error: %s\n", errno, strerror(errno));
        exit(-1);
    }
    if (pid > 0) { // parent 
        printf("parent precess\n");
        //注册信号处理函数
        signal(SIGALRM, sighander);
        struct itimerval tm;
        //周期性时间赋值
        tm.it_interval.tv_sec = 3;//秒
        tm.it_interval.tv_usec = 0;//微秒
        //第一次触发的时间
        tm.it_value.tv_sec =60;
        tm.it_value.tv_usec = 0;
        setitimer(ITIMER_REAL, &tm, NULL);
        printf("data is inserting!\n");
        while (!flag);

    } else { // child 
        printf("child process, pid: %ld, ppid: %ld\n", getpid(), getppid());
        FILE *out;
        out = fopen("log.txt", "aw+");
        if (out != NULL)
            fprintf(out, "process is runing...\n");
        char *p;
        p = get_stime();
        fprintf(out, "start time:%s\n", p);
        write_account_liquid_file(out);
        p = get_stime();
        fprintf(out, "end time:%s\n", p);
        fprintf(out, "process is run over...\n");
        fprintf(out, "*************write_account_liquid_file finished!**************\n");
        fclose(out);
    }
    return 0;
}


