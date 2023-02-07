// Andrew
// #include "Util.h"
#include "../include/Util.h"

using namespace arf;

vector<vector<double> > parseCsv(vector<int> columns,string file) {
    int dims = columns.size();
    vector<vector<double> > datapoints;
    string line;
    ifstream myfile;
    myfile.open(file);
    getline(myfile,line);

    while ( myfile.good() ) {
        string lien;
        getline(myfile,lien);
        //cout << lien << endl;

        vector<double> datapoint(dims);
        vector<string> tokenized = stringSplit(lien," ");
        // cout<<"tokenized: "<<tokenized.size()<<endl;
        if(tokenized.size()==0)
            break;
        for(int j=0;j<dims;j++) {
            int col = columns[j]-1;
            for(int i=0;i<tokenized.size();i++) {
                //cout<<"col"<<col<<endl;
                if(i==col) {
                    // cout<<"value:"<<tokenized[i]<<endl;
                    datapoint[j] = atof(tokenized[i].c_str());
                }
            }
        }
        datapoints.push_back(datapoint);
    }

    myfile.close();
    return datapoints;
}

void tick(struct timeval &start) {
    gettimeofday(&start, NULL);
}

double tock(struct timeval &start,struct timeval &end,bool doPrint) {
    gettimeofday(&end,NULL);
    double tiem = (end.tv_sec-start.tv_sec) + 1e-6*(end.tv_usec-start.tv_usec);
    if(doPrint)
        printf("Execution Time: %.6f sec\n", tiem);
    return tiem;
}

int closestPower2(int n) {
    if ((n & (n-1)) == 0)
        return n;
    int i = 1;
    while(n>>=1)
        i++;
    return 1<<i;
}
