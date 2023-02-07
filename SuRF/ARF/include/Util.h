#ifndef UTIL_H
#define UTIL_H

#include <iostream>
#include <cstdlib>
#include <sys/time.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <random>
#include <list>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>

using namespace std;

namespace arf {

//typedef unsigned long long uint64;
typedef uint64_t uint64;

#define GREEN_COLOR printf("\033[01;32m");
#define WHITE_COLOR printf("\033[01;37m");
//typedef unsigned long long int uint;

struct stats
{
    uint64_t avg;
    uint64_t mean;
    uint64_t std;
};

#define Pi 3.14159265

#define DEBUG true

#define null -1

vector<vector<double> > parseCsv(vector<int> columns,string file);

void tick(struct timeval &start);

double tock(struct timeval &start,struct timeval &end,bool doPrint);

int closestPower2(int n);

std::vector<std::string> inline stringSplit(string &source, const char *delimiter = " ", bool keepEmpty = false) {
    std::vector<std::string> results;

    size_t prev = 0;
    size_t next = 0;

    while ((next = source.find_first_of(delimiter, prev)) != std::string::npos) {
        if (keepEmpty || (next - prev != 0)) {
            results.push_back(source.substr(prev, next - prev));
        }
        prev = next + 1;
    }

    if (prev < source.size()) {
        results.push_back(source.substr(prev));
    }

    return results;
}

static inline uint64_t
rdtscp(void)
{
    return 5; // rdtscp not supported hiar :-(
    uint32_t eax, edx;

    __asm__ __volatile__("rdtscp"
                         : "+a" (eax), "=d" (edx)
                         :
                         : "%ecx", "memory");

    return (((uint64_t)edx << 32) | eax);
}

inline uint64_t time_sum(list<uint64_t> &l)
{
    uint64_t sum = 0.0;
    list<uint64_t>::iterator i;
    vector<uint64_t> copy;
    copy.reserve(l.size());

    for(i=l.begin(); i != l.end(); ++i) {

        copy.push_back(*i);
    }
    sort(copy.begin(),copy.end());
    for(int i=0;i< (int)copy.size();i++) {
        if(sum + copy[i]<sum) {
            cout<<"OVERFLOW!!"<<endl;
            assert(1==0);
        }

        sum+=copy[i];
    }
    /*cout<<"---- TIEMS STORED-----"<<endl;
      for(int j=0;j<copy.size();j++)
      {

      cout<<j<<" : "<<copy[j]<<" cycles"<<endl;
      }

      cout<<"---------------------"<<endl;*/

    return sum;
}

inline struct stats calculate(vector<uint64_t> v) //sort v
{
    sort(v.begin(),v.end());
    struct stats st;
    st.mean = v[0.5*v.size()];
    uint64_t sum =0;
    for(int i=0;i<(int)v.size();i++) {
        if(sum+v[i]<sum)
            assert(1==0);
        sum+=v[i];
    }
    st.avg = sum/v.size();

    sum = 0;

    for(int i=0;i<(int)v.size();i++) {
        if(sum+v[i]<sum)
            assert(1==0);
        sum+=(v[i]-st.avg)*(v[i]-st.avg);
    }
    st.std = sqrt(sum/v.size());
    return st;
}

inline uint64_t getMedian(list<uint64_t> &l)
{
    list<uint64_t>::iterator i;
    vector<uint64_t> copy;
    copy.reserve(l.size());

    for(i=l.begin(); i != l.end(); ++i) {
        copy.push_back(*i);
    }

    sort(copy.begin(),copy.end());
    /*for(int i=0;i<copy.size();i++)
      {
      cout<<i<<":"<<copy[i]<<" cycles"<<endl;
      }*/
    return copy[0.5 * copy.size()];
}
inline uint64_t getStddev(list<uint64_t> &l,int QUERIES)
{
    /* add zeroes */
    vector<uint64_t> copy;
    copy.reserve(l.size());
    list<uint64_t>::iterator it;

    for(it=l.begin(); it != l.end(); ++it) {
        copy.push_back(*it);
    }

    int cs = copy.size();

    for(int i=0;i<QUERIES-cs;i++)
        copy.push_back(0);

    sort(copy.begin(),copy.end());
    assert(copy.size() == QUERIES);

    uint64_t avg = 0;
    for(int i=0 ;i<(int)copy.size();i++) {
        if(avg+copy[i]<avg) {
            assert(1==0);
        }
        avg+=copy[i];
    }
    avg = (avg+0.0)/copy.size();

    long double ssum =0;

    for(int i=0 ;i<(int)copy.size();i++) {
        if(ssum + copy[i]<ssum) {
            cout<<"OVERFLOW!!"<<endl;
            assert(1==0);
        }
        //cout<<"diff:"<<copy[i]-avg<<endl;
        ssum+=(copy[i]-avg) * (copy[i]-avg);
    }

    long double lsum = ssum;
    lsum = lsum/(copy.size());
    lsum = sqrt(lsum);
    uint64_t stddev = lsum;

    return stddev;
}

inline void plot(vector<uint64_t> &v) {
    cout<<"------- PLOT OF TIMES ----------"<<endl;
    uint64_t min = v[0];
    uint64_t max = v[v.size()-1];
    int domain = max - min;
    int partitions = 16.0;
    int step = (domain+1)/partitions;
    int nstars = 100;
    int idx=0;

    for (int i=0; i<partitions; ++i) {
        int count = 0;
        while(v[idx]<min + ((i+1)*step)) {
            count++;
            idx++;
        }

        cout<<"[";
        cout.fill('0');
        cout.width(7);
        cout<<min+(i*step)<<" - ";
        cout.fill('0');
        cout.width(7);
        cout<<min+(i+1)*step<<"]";
        cout << string(count*nstars/v.size(),'*') <<endl;
    }

    cout<<"---------------------------------------------"<<endl;
}

inline vector<uint64_t> list2vector(list<uint64_t> & l) {
    list<uint64_t>::iterator i;
    vector<uint64_t> copy;
    copy.reserve(l.size());

    for(i=l.begin(); i != l.end(); ++i) {
        copy.push_back(*i);
    }
    return copy;
}

inline vector<uint64_t> fptime(list<uint64_t> &l,list<uint64_t> &r) {
    vector<uint64_t> ad = list2vector(l);
    vector<uint64_t> tr = list2vector(r);
    cout<<"Adapt size:"<<ad.size();
    cout<<" trunc size:"<<tr.size()<<endl;
    assert(ad.size() >= tr.size());
    for(int i=0;i<(int)tr.size();i++) {
        ad[i] = ad[i] + tr[i];
    }
    return ad;
}

inline uint64_t process_time(list<uint64_t> &l, uint64_t & avg,uint64_t & max, uint64_t & min) {
    avg = 0;
    max = 0;
    min = l.front();
    list<uint64_t>::iterator i;
    vector<uint64_t> copy;
    copy.reserve(l.size());

    for(i=l.begin(); i != l.end(); ++i) {
        copy.push_back(*i);
    }

    sort(copy.begin(),copy.end());
    avg = copy[0.5 * copy.size()];

    long double ssum =0;

    for(int i=0.16 * copy.size();i<0.84 * copy.size();i++) {
        if(ssum + copy[i]<ssum) {
            cout<<"OVERFLOW!!"<<endl;
            assert(1==0);
        }

        //cout<<"diff:"<<copy[i]-avg<<endl;
        ssum+=(copy[i]-avg) * (copy[i]-avg);
    }

    long double lsum = ssum;
    lsum = lsum/(0.68 * copy.size());
    lsum = sqrt(lsum);
    uint64_t stddev = lsum;

    cout<<"Lookups stored: "<<l.size()<<endl;
    cout<<"Mean cycles: "<<copy[0.5 * copy.size()]<<endl;

    //cout<<"Max cycles "<<max<<endl;
    cout<<"84%th percentile: "<<copy[0.84 * copy.size()]<<endl;
    cout<<"16th percentile: "<<copy[0.16 * copy.size()]<<endl;
    cout<<"stddev high:"<<copy[0.84 * copy.size()] - avg<<endl;
    cout<<"stddev low:"<<-copy[0.16 * copy.size()] + avg<<endl;
    cout<<"stddev calculated:"<<lsum<<endl;

    // uint64_t stddev = (copy[0.84 * copy.size()] -copy[0.16 * copy.size()])/2;
    //cout<<"stddev normal"<<stddev<<endl;
    min = avg - stddev;
    max = avg + stddev;
    return stddev;
}

} // namespace arf

#endif // Util_H
