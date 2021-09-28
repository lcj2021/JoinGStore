#include <iostream>
#include <fstream>
#include <set>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <stack>

#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <pthread.h>

#include <cstddef>
#include <cstring>

#include "client.h"
#include "joiner.h"

using namespace std;
#define MAXQUERYCNT 16
vector<GstoreConnector> servers;

string readSPARQL(string filename);
vector<string> getQueriesInPath(const string &path);
void buildQueryTree(const string &path, const string & filename);
unordered_map <string, int> query2ID;

typedef struct QueryNeedVars
{
    int queryId, serverId;
    GstoreConnector *server;
    string dbname;
    string format;
    string query;
    string res;
    long timeCost;
} QNV;

struct TreeNode
{
    int id;
    int lc, rc;
    bool isSql;
    string op;
    string sql;
}treeNode[100];

void *queryThread(void *args);
vector<string> evaluateQueriesInServers(vector<string> &queries, vector<GstoreConnector> &servers, const string &db_name, const string &format, long * queryCost);

void writeAns(string filename, string &ans, string end);

long get_cur_time();

// vector<string> join(vector<string> &res1, vector<string> &res2)
// string join(vector<string> &results)
// string join(string &res1, string &res2)

bool st[100];
vector<string> results;
vector <string> expression;
stack <string> ops;
stack <string> res;
void dfs(TreeNode & u)
{
    // if (!u.isSql)               cout << "( ";
    // if (u.lc != -1 && !st[u.lc])      dfs(treeNode[u.lc]);
    // if (u.isSql == 0)   cout << u.op << " ";
    // else                cout << u.id << " ";
    // if (u.rc != -1 && !st[u.rc])      dfs(treeNode[u.rc]);
    // if (!u.isSql)               cout << " )";

    if (u.lc != -1 && !st[u.lc])      dfs(treeNode[u.lc]);
    if (u.rc != -1 && !st[u.rc])      dfs(treeNode[u.rc]);

    if (u.isSql == 0)           expression.push_back(u.op);
    else                        expression.push_back(results[query2ID[u.sql]]);
}
int calTimes;
void cal ()
{
    ofstream out ("calTimes" + to_string(++ calTimes), ios::trunc );
    string a = res.top();   res.pop();
    string b = res.top();   res.pop();
    string op = ops.top();  ops.pop();
    string c;
// ====================================================
    joiner j;
    if (op == "join")   
    {
        c = j.join (a, b);
        // out << a << endl;   out << "====================================================" << endl;
        // out << b << endl;   out << "====================================================" << endl;
    }
    else
    {
        c = j.Union (a, b);
        // out << a << endl;   out << "====================================================" << endl;
        // out << b << endl;   out << "====================================================" << endl;
    }
    out << "finalRes : " << endl << c << endl;
    res.push(c);
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        cout << "请输入数据库名和查询路径" << endl;
        return -1;
    }
    string dbname(argv[1]);
    string queriesPath(argv[2]);
    string filename(argv[3]);
    if (queriesPath.find_last_of("/") != queriesPath.size() - 1)
        queriesPath += "/";

    // add slaves 
    servers.push_back(GstoreConnector("39.98.70.144", 4002, "root", "123456"));   // 0 slave 4
    servers.push_back(GstoreConnector("47.92.170.33", 4002, "root", "123456"));   // 1 slave 5
    servers.push_back(GstoreConnector("47.92.169.107", 4002, "root", "123456"));  // 2 slave 6
    servers.push_back(GstoreConnector("39.100.146.205", 4002, "root", "123456")); // 3 slave 7
    servers.push_back(GstoreConnector("47.92.120.35", 4002, "root", "123456"));   // 4 slave 8
    servers.push_back(GstoreConnector("39.99.176.42", 4002, "root", "123456"));   // 5 slave 9
    servers.push_back(GstoreConnector("47.92.219.207", 4002, "root", "123456"));  // 6 slave 10
    servers.push_back(GstoreConnector("39.100.139.177", 4002, "root", "123456")); // 7 slave 11

    vector<string> queries = getQueriesInPath(queriesPath);
    
    buildQueryTree(queriesPath, filename);

    cout << "Get queries as follow!" << endl;
    for (int i = 0; i < queries.size(); i++)
    {
        cout << queries[i] << endl;
    }

    // return 0;
    // 查询
    long querytime = get_cur_time(), queryCost = -1;
    results = evaluateQueriesInServers(queries, servers, dbname, "text", & queryCost); // 每一个result，第一行是变量，后面都是变量对应的结果

    cout << "Time of query is: " << queryCost << " ms." << endl;
    
    // for (int i = 0; i < 6; ++ i)
    // {
    //     ifstream in("result" + to_string(i));
    //     string t = "";
    //     string line;
    //     while (getline (in, line))
    //     {
    //         t += line + "\n";
    //     }
    //     results.push_back(t);
    // }

    dfs (treeNode[1]);
    puts("");

    // for (int i = 0; i < results.size(); ++ i)
    // {
    //     string filename = "result" + to_string(i);
    //     ofstream out (filename);
    //     out << results[i] << endl;
    // }

    // 连接
    long jointime = get_cur_time();

    for (auto it : expression)
    {
        if (it == "join" || it == "union")      
        {
            ops.push(it);   
            cal();
        }
        else                                    res.push(it);
    }
    ofstream out ("finalRes");

    long joinfinished = get_cur_time();

    string finalRes = res.top();        
    int cntOfRes = 0;

    auto & t = finalRes;
    t = t.substr(t.find_first_of("\n") + 1);
    for (auto & c : t)  if (c == '\n')  ++ cntOfRes;

    cout << "Time of join : " << joinfinished - jointime << " ms." << endl;
    cout << "Total time : " << joinfinished - querytime << " ms." << endl;
    cout << "Count of final results : " << cntOfRes << endl;
    cout << res.top() << endl;
    cout << "query success." << endl;

    return 0;
}

string readSPARQL(string filename)
{
    cout << "Opening " << filename << endl;
    ifstream ifs;
    ifs.open(filename, ios::in);
    string sparql;
    string s;
    if (!ifs.is_open())
    {
        throw runtime_error("file opened failed");
    }
    while (!ifs.eof())
    {
        getline(ifs, s);
        sparql += s;
        sparql += "\n";
    }
    ifs.close();
    return sparql;
}

vector<string> getQueriesInPath(const string &path)
{
    DIR *dir;
    struct dirent *ptr;

    vector<string> queriesfiles;
    vector<string> queries;

    if ((dir = opendir(path.c_str())) == NULL)
    {
        perror("Open dir error...");
        exit(1);
    }

    while ((ptr = readdir(dir)) != NULL)
    {
        if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0)
            continue;
        else if (ptr->d_type == 8) // 文件(8)、目录(4)、链接文件(10)
        {
            string file = ptr->d_name;
            // cout << file << endl;
            if (file[0] != 'q')     continue;
            queriesfiles.push_back(file);
        }
        else if (ptr->d_type == 10)
            continue;
    }
    closedir(dir);

    sort(queriesfiles.begin(), queriesfiles.end());
    for (int i = 0; i < queriesfiles.size(); ++ i)          query2ID[queriesfiles[i]] = i;
    for (auto & it : queriesfiles)                          cout << it << " " << query2ID[it] << endl;
    for (auto & it : queriesfiles)                          it = path + it;

    for (auto it : queriesfiles)
        queries.push_back(readSPARQL(it));
    
    return queries;
}

void *queryThread(void *args)
{
    long begin = get_cur_time(), timeCost;
    QNV *vars = (QNV *)args;
    GstoreConnector &server = *vars->server;
    vars->res = server.query(vars->dbname, vars->format, vars->query);
    timeCost = get_cur_time() - begin;
    cout << "query " << vars->queryId << " has finished on server " << vars->serverId << ". Take " << timeCost << "ms." << endl;
    vars -> timeCost = timeCost;
    // cout << vars -> timeCost << endl;
    return NULL;
}

vector<string> evaluateQueriesInServers(vector<string> &queries, vector<GstoreConnector> &servers, const string &db_name, const string &format, long * queryCost)
{
    vector<string> results;
    // 并行查询
    pthread_t thread[queries.size()][servers.size()];
    QNV qnv[queries.size()][servers.size()];
    long queryTime[servers.size()];
    // vector<long> queryTime[servers.size()];
    memset(queryTime, 0, sizeof queryTime);

    for (int i = 0; i < queries.size(); i++)
    {
        for (int j = 0; j < servers.size(); j++)
        {
            qnv[i][j].queryId = i;
            qnv[i][j].serverId = j;
            qnv[i][j].server = &servers[j];
            qnv[i][j].dbname = db_name;
            qnv[i][j].format = format;
            qnv[i][j].query = queries[i];
            cout << "query " << i << " on server " << j << " ";
            if (pthread_create(&thread[i][j], NULL, queryThread, &qnv[i][j]) != 0)
            {
                throw runtime_error("creates thread error!");
            }
            else
                cout << "creates thread success!" << endl;
        }
    }
    for (int i = 0; i < queries.size(); i++)
        for (int j = 0; j < servers.size(); j++)
            pthread_join(thread[i][j], NULL);

    // 结果合并
    cout << "Now merge the results..." << endl;
    for (int i = 0; i < queries.size(); i++)
    {
        bool firstRes = true;
        set<string> s; // 去掉重复的结果
        string resOfQuery;
        for (int j = 0; j < servers.size(); j++)
        {
            string &temp_res = qnv[i][j].res;
            if (temp_res.find("[empty result]") != string::npos) // 查出的结果是空集
            {
                cout << "query " << i << " server " << j << " Empty" << endl;
                continue;
            }
            else
            {
                cout << "query " << i << " server " << j << " Get Answer" << endl;
            }

            vector<string> lines;
            joiner::split(temp_res, lines, "\n");
            vector<string>::iterator iter = lines.begin();

            queryTime[j] += stol(*iter);
            // queryTime[j].push_back(stol(*iter));
            ++ iter;
            if (firstRes) // 第一次添加结果
            {
                resOfQuery = lines[1];
                firstRes = false;
            }
            // 非第一次添加结果，需要去掉首行变量
            iter++;
            for (; iter != lines.end(); iter++)
                s.insert(*iter);

        }
        if (firstRes) // 没有结果
        {
            results.push_back("");
        }
        else
        {
            for (set<string>::iterator it = s.begin(); it != s.end(); it++)
                resOfQuery += "\n" + *it;
            results.push_back(resOfQuery);
        }
    }
    // for (auto it : queryTime)   
    // {
    //     for (auto i : it)   cout << i << ' ';
    //     puts("");
    // }
    for (auto it : queryTime)   * queryCost = max(it, * queryCost);
    return results;
}

void buildQueryTree(const string &path, const string & filename)
{
    ifstream in(path + filename);
    int n;
    in >> n;
    for (int i = 1; i <= n; ++ i)
    {
        int id, lc, rc, isSql;
        in >> id >> lc >> rc >> isSql;
        treeNode[i] = {id, lc, rc, isSql};
        if (isSql)
        {
            string t;
            in >> t;
            treeNode[i].sql = t;
            // treeNode[i].sql = readSPARQL(path + t);
        }
        else
            in >> treeNode[i].op;
    }
    // cout << treeNode[7].isSql << endl << treeNode[7].sql << endl;
}

void writeAns(string filename, string &ans, string end)
{
    ofstream ofs;
    ofs.open(filename, ios::app);
    ofs << ans << endl;
    ofs << end << endl;
    ofs.close();
}

long get_cur_time()
{
    timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000 + tv.tv_usec / 1000);
}