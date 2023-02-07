// Andrew
// #include "Statistics.h"
// #include <iostream>
// #include "Util.h"

#include "../include/Statistics.h"
#include <iostream>
#include "../include/Util.h"

using namespace std;
using namespace arf;

Statistics::Statistics() {
    tp = 0;
    tn = 0;
    fp = 0;
    q = 0;
    total_ranges = 0;
}

double Statistics::getColdStore() {
    return (tp)*100.0/(fp+tn+tp);
}

void Statistics::updateRange(int r) {
    total_ranges+=r;
}

void Statistics::print() {
    cout<<"true negatives:"<<tn<<endl;
    cout<<"true poz: "<<tp<<endl;
    cout<<"fp: "<<fp<<endl;
    cout<<"Queries:"<<q<<endl;

    /*	cout<<"Average [number of ranges in sfc PER range query]:"<<
        (total_ranges+0.0)/q<<endl;*/
    GREEN_COLOR
        cout<<"False positive rate:"<<fp*100.0/(fp+tn)<<endl;
    WHITE_COLOR
        }

void Statistics::printFp() {
    cout<<"Fp rate:"<<getFpr()<<endl;
}

double Statistics::getFpr() {
    if(tn ==0 && fp ==0)
        return -1;
    /*cout<<"true negatives:"<<tn<<endl;
      cout<<"fp: "<<fp<<endl;
    */

    //getchar();*/
    return (fp*100.0)/(fp+tn+0.0);
}

Statistics::Statistics(int domain) {
    // TODO Auto-generated constructor stub
    fps = vector<int>(domain);
    std::fill(fps.begin(),fps.end(),0);
    tp = 0;
    tn = 0;
    fp =0;
}

void Statistics::reset() {
    tp = 0;
    tn = 0;
    fp =0;
    std::fill(fps.begin(),fps.end(),0);
    q= 0;
    total_ranges =0;
}

void Statistics::update(bool sR,bool qR) {
    q++;
    assert(!(!sR && qR));
    if(sR && !qR)
        (fp)++;
    if(!sR && !qR)
        (tn)++;
    if(sR && qR)
        (tp)++;
}
void Statistics::incrementFP(int low, int high) {
    if(fps.size()<=low)
        return;

    if(low == high)
        fps[low]++;
    else
	{
            //for the time being
            fps[low]++;
	}
}

Statistics::~Statistics() {
    // TODO Auto-generated destructor stub
}
