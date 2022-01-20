#include <fstream>

using namespace std;

void ans2file(string filename, string query_name, long query_time, long quer_join, long query_sum, unsigned int res_cnt)
{
    ofstream out (filename, ios::app);
    out << query_name << "\t"
    << query_time << "\t"
    << quer_join << "\t"
    << query_sum << "\t"
    << res_cnt << "\n";
    out.close();
}