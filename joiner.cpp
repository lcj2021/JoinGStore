#include "joiner.h"

long joiner::get_cur_time()
{
    timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000 + tv.tv_usec / 1000);
}

void joiner::addItem(string item, vector<string> &elements)
{
    if (hashTable.find(item) == hashTable.end())
        hashTable[item] = vector<string>();
    for (vector<string>::const_iterator it = elements.begin(); it != elements.end(); it++)
        hashTable[item].push_back(*it);
}

bool joiner::isContained(string item)
{
    return hashTable.find(item) != hashTable.end();
}

void joiner::split(const string &s, vector<string> &tokens, const string &delimiters)
{
    string::size_type lastPos = s.find_first_not_of(delimiters, 0);
    string::size_type pos = s.find_first_of(delimiters, lastPos);
    while (string::npos != pos || string::npos != lastPos)
    {
        tokens.push_back(s.substr(lastPos, pos - lastPos)); //use emplace_back after C++11
        lastPos = s.find_first_not_of(delimiters, pos);
        pos = s.find_first_of(delimiters, lastPos);
    }
}

//Calculate intersection var cnt
vector<string> joiner::intersection(const vector<string> &s1, const vector<string> &s2)
{
    vector<string> commonVar;
    for (auto it1 : s1)
        for (auto it2 : s2)
            if (it1 == it2)
                commonVar.push_back(it1);

    return commonVar;
}

// Union 2 vector by STL set
vector<string> joiner::unionSet(const vector<string> &s1, const vector<string> &s2)
{
    set<string> varSet;
    for (auto it1 : s1)
        varSet.insert(it1);
    for (auto it2 : s2)
        varSet.insert(it2);

    return vector<string> (varSet.begin(), varSet.end());
}

/**
 * 输入和输出都是按行划分后的
 */
vector<string> joiner::join(vector<string> &res1, vector<string> &res2)
{
    cout << "================== Joining res1 and res2 ==================" << endl;
    cout << "Size of res1 is: " << res1.size() << endl;
    cout << "Size of res2 is: " << res2.size() << endl;
    cout << "================== Joining res1 and res2 ==================" << endl;

    vector<string> ans; // 返回的结果
    if (res1.size() == 0 || res2.size() == 0)
        return ans;

    vector<string> varVec_1;
    vector<string> varVec_2;
    split(res1[0], varVec_1, "\t");
    split(res2[0], varVec_2, "\t");

    map<string, int> varMap_1;
    map<string, int> varMap_2;

    // varMap["?v0"] = 0, varMap["?v2"] = 1, etc.
    for (int i = 0; i < varVec_1.size(); i++)
        varMap_1[varVec_1[i]] = i;
    for (int i = 0; i < varVec_2.size(); i++)
        varMap_2[varVec_2[i]] = i;

    // build vec on the basis of larger one, by grouping the smaller one
    bool choose = res1.size() < res2.size();
    vector<string> *buildVec = choose ? &res2 : &res1;
    map<string, int> *buildVarMap = choose ? &varMap_2 : &varMap_1;
    vector<string> *probeVec = choose ? &res1 : &res2;
    map<string, int> *probeVarMap = choose ? &varMap_1 : &varMap_2;

    cout << "buildVarMap's size is " << buildVarMap->size() << endl;
    for (auto & it : *buildVarMap)
        cout << it.first << " ";
    cout << endl;
    cout << "probeVarMap's size is " << probeVarMap->size() << endl;
    for (auto & it : *probeVarMap)
        cout << it.first << " ";
    cout << endl;

    // 建hash表
    hashTable.clear();
    vector<string> intersectionVar = intersection(varVec_1, varVec_2);
    cout << "intersectionVar Num: " << intersectionVar.size() << endl;
    vector<string> elements;
    for (int i = 1; i < buildVec->size(); i++)
    {
        elements.clear();
        split(buildVec->at(i), elements, "\t");
        string elements_in_line = "";
        for (vector<string>::iterator it = intersectionVar.begin(); it != intersectionVar.end(); it++)
            elements_in_line += elements.at((*buildVarMap)[*it]);
        addItem(elements_in_line, elements);
    }

    // 查hash表，还需要把变量和结果做并操作
    string vars = "";
    vector<string> varsVec = unionSet(varVec_1, varVec_2);
    vars += varsVec[0];
    for (int i = 1; i < varsVec.size(); i++)
        vars += ("\t" + varsVec[i]);
    ans.push_back(vars);

    for (int i = 1; i < probeVec->size(); i++)
    {
        elements.clear();
        split(probeVec->at(i), elements, "\t");
        string elements_in_line = "";
        for (vector<string>::iterator it = intersectionVar.begin(); it != intersectionVar.end(); it++)
            elements_in_line += elements.at((*probeVarMap)[*it]);
        if (isContained(elements_in_line))
        {
            for (int j = 0; j < hashTable[elements_in_line].size(); j += buildVarMap->size())
            {
                string oneLineOfAns = "";
                for (int k = 0; k < varsVec.size(); k++)
                {
                    if (probeVarMap->find(varsVec[k]) != probeVarMap->end())
                        oneLineOfAns += elements[probeVarMap->at(varsVec[k])];
                    else
                        oneLineOfAns += hashTable[elements_in_line][j + buildVarMap->at(varsVec[k])];
                    if (k < varsVec.size() - 1)
                        oneLineOfAns += "\t";
                }
                ans.push_back(oneLineOfAns);
            }
        }
    }

    return ans;
}

//arg results[0] : "<v1>\t<v2>\t<v3> \n <v1>\t<v3>\t<v2> \n ..."
string joiner::join(vector<string> &results)
{
    // if partial has no results or arg:results is empty, return ""
    if (results.empty())        return "";

    for (int i = 0; i < results.size(); ++ i)
    {
        if (results[i] != "")   break;
        if (i == results.size() - 1)    return "";
    }


    string ans0 = results[0];
    vector<string> linesOfAns0;
    split(ans0, linesOfAns0, "\n");

    for (int i = 1; i < results.size(); i++)
    {
        string ansi = results[i];
        vector<string> linesOfAnsi;
        split(ansi, linesOfAnsi, "\n");

        linesOfAns0 = join(linesOfAns0, linesOfAnsi);
    }

    string finalAns;
    if(linesOfAns0.size() == 0 || linesOfAns0.size() == 1)
    {
        cout << "There has answer: " << 0 << endl;
        finalAns = "[empty result]";
    }
    else
    {
        cout << "There has answer: " << linesOfAns0.size() - 1 << endl;
        finalAns = linesOfAns0[0];
        for (int i = 1; i < linesOfAns0.size(); i++)
        {
            finalAns += ("\n" + linesOfAns0[i]);
        }
    }
    
    return finalAns;
}

string joiner::join(string &res1, string &res2)
{
    vector<string> lines_1;
    vector<string> lines_2;

    split(res1, lines_1, "\n");
    split(res2, lines_2, "\n");

    vector<string> ans = join(lines_1, lines_2);

    string finalRes;

    for (auto item : ans)
    {
        finalRes += item;
        finalRes += "\n";
    }

    return finalRes;
}

string joiner::Union(string &res1, string &res2)
{
    set <string> s;
    string head = res1.substr(0, res1.find_first_of("\n"));
    // cout << "head is : " << head << endl;
    res1 = res1.substr(res1.find_first_of("\n") + 1);
    res2 = res2.substr(res2.find_first_of("\n") + 1);

    vector <string> v1, v2;
    split(res1, v1, "\n");
    split(res2, v2, "\n");

    for (auto it : v1)  s.insert(it);
    for (auto it : v2)  s.insert(it);

    string finalRes = head + "\n";

    for (auto it : s)
    {
        finalRes += it;
        finalRes += "\n";
    }
    return finalRes;
}