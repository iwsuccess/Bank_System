#include "dodata.h"
#include <ctime>
#include <chrono>
#include<sys/time.h>

using namespace std;

//�����յ��ַ������зָ��
void DoData::strDivide(const string &str, vector<string> &vec) {
    string tem;
    for (auto ch: str) {
        if (ch != ';')
            tem = tem + ch;
        else {
            vec.push_back(tem);
            tem = "";
        }
    }
}

string DoData::analysis() {
    //�ͻ��ţ����룬��ʵ�������绰������֤�ţ���ͨ���ߣ������أ�Ŀ�ĵأ����ڣ�Ʊid, ����ʱ�䣬 ����ʱ�䣬Ʊ�ۣ� Ʊ����
    string uid, name, account, tranDate, tranAmt, address, post, phone, liquidnum, amount;
    vector<string> vec;
    string res;
    strDivide(data, vec);
    if (vec.size() < 1)
        return "404";
    switch (stoi(vec[0].substr(vec[0].size() - 1))) {
        case 1: {
            /*
             *
             */
            if (vec.size() < 4)
                return "GDRC_001;000001;no";
            account = vec[1];
            tranDate = vec[2];
            tranAmt = vec[3];
            res = updateBalance(account, tranDate, tranAmt);
            break;
        }
        case 2: {
            if (vec.size() < 4)
                return "GDRC_002;000001;no";
            account = vec[1];
            tranDate = vec[2];
            phone = vec[3];
            res = updateUserInfo(account, tranDate, phone);
            break;
        }
        case 3: {
            if (vec.size() < 2)
                return "GDRC_003;000001;no";
            account = vec[1];
            res = findBalance(account);
            break;
        }
        default:
            break;
    }
    return res;
}

string DoData::updateBalance(const string &account, const string &tranDate, const string &tranAmt) {
    FILE *out;
    out = fopen("log_op.txt", "aw+");
    string sql;
    string res = SUCCESS;
    MYSQL_RES *result;
    auto start = chrono::steady_clock::now();
    char *p = getStime();
    fprintf(out, "start time:%s\n", p);
    sql = "UPDATE accountfile SET balance = balance + " + tranAmt + ", lastexchange = '" + tranDate +
          "' WHERE account = '" + account + "';";
    mysql_query(mysql, sql.c_str());
    auto end = chrono::steady_clock::now();
    p = getStime();
    fprintf(out, "%ld update_balance time in milliseconds:%dms\n", chrono::steady_clock::now(),
            chrono::duration_cast<chrono::milliseconds>(end - start).count());
    fprintf(out, "end time:%s\n", p);
    cout << "update_balance time in milliseconds: "
         << chrono::duration_cast<chrono::milliseconds>(end - start).count()
         << " ms" << endl;
    result = mysql_store_result(mysql);
    if (!mysql_affected_rows(mysql))
        res = FAILURE;
    time_t now = time(0);
    tm *ltm = localtime(&now);
    string time = to_string(now);
    time = time.substr(time.size() - 7);
    string date = to_string(1900 + ltm->tm_year) + (to_string(1 + ltm->tm_mon).size() < 2 ? "0" : "") +
                  to_string(1 + ltm->tm_mon)
                  + (to_string(ltm->tm_mday).size() < 2 ? "0" : "") + to_string(ltm->tm_mday);
    auto balance = findBalance(account);
    auto start_1 = chrono::steady_clock::now();
    sql = "INSERT INTO accountliquidfile (account, time, liquidnum, date, exchange, balance) VALUES('" + account +
          "', '" + time + "', '"
          + time + "', '" + date + "', '" + tranAmt + "', '" + balance + "');";

    mysql_query(mysql, sql.c_str());
    auto end_1 = chrono::steady_clock::now();
    // fprintf(out,"%ld insert_liquidfile time in milliseconds:%dms\n",chrono::steady_clock::now(),chrono::duration_cast<chrono::milliseconds>(end_1 - start_1).count());
    // cout << "insert_liquidfile time in milliseconds: "
    //     << chrono::duration_cast<chrono::milliseconds>(end - start).count()
    //     << " ms" << endl;
    string uid = findUid(account);
    string ret =
            "GDRC_001;" + account + ";" + uid + ";" + balance + ";" + (res == SUCCESS ? "000000" : "000001") + ";" +
            res;
    fclose(out);
    return ret;
}

string DoData::findBalance(const string &account) {
    FILE *out;
    out = fopen("log_op.txt", "aw+");
    string sql;
    string res = SUCCESS;
    MYSQL_RES *result;
    auto start = chrono::steady_clock::now();
    // fprintf(out,"start time:%s\n",p);
    sql = "SELECT balance FROM accountfile WHERE account = '" + account + "';";
    mysql_query(mysql, sql.c_str());
    auto end = chrono::steady_clock::now();
    // fprintf(out,"end time:%s\n",p);
    // fprintf(out,"%ld find_Balance time in milliseconds:%dms\n",chrono::steady_clock::now(),chrono::duration_cast<chrono::milliseconds>(end - start).count());
    // cout << "find_Balance time in milliseconds: "
    //     << chrono::duration_cast<chrono::milliseconds>(end - start).count()
    //     << " ms" << endl;
    result = mysql_store_result(mysql);
    if (!mysql_num_rows(result)) {
        res = FAILURE;
    } else {
        MYSQL_ROW row = mysql_fetch_row(result);
        res = row[0];
    }
    fclose(out);
    return res;
}

string DoData::findUid(const string &account) {
    string sql;
    string res = SUCCESS;
    MYSQL_RES *result;
    sql = "SELECT uid FROM accountfile WHERE account = '" + account + "';";
    mysql_query(mysql, sql.c_str());
    result = mysql_store_result(mysql);
    if (!mysql_num_rows(result)) {
        res = FAILURE;
    } else {
        MYSQL_ROW row = mysql_fetch_row(result);
        res = row[0];
    }
    return res;
}

string DoData::updateUserInfo(const std::string &account, const std::string &tranDate, const std::string &phone) {
    FILE *out;
    out = fopen("log_op.txt", "aw+");
    string sql;
    string res = SUCCESS;
    vector<string> vec;
    MYSQL_RES *result;
    string uid = findUid(account);
    char *p = getStime();
    fprintf(out, "start time:%s\n", p);
    auto start = chrono::steady_clock::now();
    sql = "UPDATE userinfo1 u1, accountfile ac SET u1.phone = '" + phone + "', ac.lastexchange = '" + tranDate +
          "' WHERE u1.uid = ac.uid AND ac.account = '" + account + "';";
    mysql_query(mysql, sql.c_str());
    auto end = chrono::steady_clock::now();
    p = getStime();
    fprintf(out, "%ld update_phone_num time in milliseconds:%dms\n", chrono::steady_clock::now(),
            chrono::duration_cast<chrono::milliseconds>(end - start).count());
    fprintf(out, "end time:%s\n", p);
    cout << "update_phone_num time in milliseconds: "
         << chrono::duration_cast<chrono::milliseconds>(end - start).count()
         << " ms" << endl;

    result = mysql_store_result(mysql);
    if (!mysql_affected_rows(mysql))
        res = FAILURE;
    string ret =
            "GDRC_002;" + account + ";" + uid + ";" + phone + ";" + (res == SUCCESS ? "000000" : "000001") + ";" + res;

    fclose(out);
    return ret;
}

char *DoData::getStime(void) {
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

// char* DoData::getStime() { 

// 	time_t now = time(0);
// 	tm *ltm = localtime(&now);
// 	// string date = to_string(1900 + ltm->tm_year) + (to_string(1 + ltm->tm_mon).size() < 2 ? "0" : "") + to_string(1 + ltm->tm_mon)
// 	// 			+ (to_string(ltm->tm_mday).size() < 2 ? "0" : "") + to_string(ltm->tm_mday);
//     static char timestr[200] ={0};
//     // struct tm * pTempTm;
//     // struct timeval time;

//     // gettimeofday(&time,NULL);
//     // pTempTm = localtime(&time.tv_sec);
//     if( NULL != ltm )
//     {
//         snprintf(timestr,199,"%04d-%02d-%02d %02d:%02d:%02d",
//             1900 + ltm->tm_year,
//             1 + ltm->tm_mon, 
//             ltm->tm_mday,
//             ltm->tm_hour, 
//             ltm->tm_min, 
//             ltm->tm_sec);
//     }
//     return timestr;
// }