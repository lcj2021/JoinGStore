#include <iostream>
#include <set>
#include <map>
#include <vector>
#include <sstream>
#include <sys/time.h>
#include "client.h"

using namespace std;

class joiner
{
private:
    map<string, vector<string>> hashTable;
    map<string, int> varMap;

    long get_cur_time();
    vector<string> intersection(const vector<string> &s1, const vector<string> &s2);
    vector<string> unionSet(const vector<string> &s1, const vector<string> &s2);

public:
    void addItem(string item, vector<string> &elements);
    bool isContained(string item);
    void static split(const string &s, vector<string> &tokens, const string &delimiters = " ");

    string join(vector<string> &results);
    vector<string> join(vector<string> &res1, vector<string> &res2);
    string join(string &res1, string &res2);

    string Union(string &res1, string &res2);
};
