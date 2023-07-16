#ifndef __DODATA_H_
#define __DODATA_H_

#include <string>
#include <mysql/mysql.h>
#include <vector>
#include <iostream>

#define FAILURE "no"
#define SUCCESS "yes"

class DoData {
public:
    DoData() {}

    DoData(std::string s, MYSQL *m) {
        data = s;
        mysql = m;
    }

    DoData(char *s, MYSQL *m) {
        data = std::string(s);
        mysql = m;
    }

    void setData(std::string _data) { data = _data; }

    void setMysql(MYSQL *_mysql) { mysql = _mysql; }

    std::string analysis();

    std::string updateBalance(const std::string &account, const std::string &tranDate, const std::string &tranAmt);

    std::string findBalance(const std::string &account);

    std::string findUid(const std::string &account);

    std::string updateUserInfo(const std::string &account, const std::string &tranDate, const std::string &phone);

private:
    void strDivide(const std::string &str, std::vector<std::string> &vec);

    char *getStime();

    std::string data;
    MYSQL *mysql;
    //MYSQL_ROW* record;
};

#endif