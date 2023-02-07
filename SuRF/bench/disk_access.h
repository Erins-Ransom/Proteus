#ifndef SURF_DISK_ACCESS_H
#define SURF_DISK_ACCESS_H
#define PRINT 0

// Example Usage:
// DiskAccess disk_access("/workload_gen/my_data/data.txt");
// disk_access.init();
// disk_access.lookup_from_disk(0, 100);
class DiskAccess{
public:
    DiskAccess(string path_to_read_keys){
        path_to_read_keys_=path_to_read_keys;
    }

    std::string uint64ToString(uint64_t key) {
        uint64_t endian_swapped_key = __builtin_bswap64(key);
        return std::string(reinterpret_cast<const char*>(&endian_swapped_key), 8);
    }

    // Just init once
    void init(){
        // set up indexes for keys
        // may consider the granularity of indexing, but current version may be good enough for simulation
        std::ifstream infile(path_to_read_keys_);
//        std::fstream byte_outfile(path_to_read_keys_+".bin", ios::out | ios::binary);
//        std::ofstream byte_outfile(path_to_read_keys_+".bin");
        ofstream byte_outfile(path_to_read_keys_+".bin", std::ofstream::binary);
        std::string key;
        // we assume the keys are sorted in the file
        while (infile.good()) {
            uint64_t int_key;
            infile >> int_key;
//            byte_outfile<<uint64ToString(int_key);
            byte_outfile.write(reinterpret_cast<const char*>(&int_key), sizeof(uint64_t));
//            cout<<int_key<<endl;
//            byte_outfile.write((char*)int_key, sizeof(int_key));
            keys.push_back(int_key);
        }

    }
    vector<uint64_t> lookup_from_disk(uint64_t lower_bound, uint64_t upper_bound){
        vector<uint64_t> res;
        std::vector<uint64_t>::iterator first_pos_greater_than_lower_bound;
        first_pos_greater_than_lower_bound=std::lower_bound (keys.begin(), keys.end(), lower_bound);
        uint64_t offs = first_pos_greater_than_lower_bound-keys.begin();
        std::ifstream infile(path_to_read_keys_+".bin");
        infile.seekg(offs*sizeof(uint64_t));
        std::string key;
        uint64_t int_key;
//        std::cout<<"start lookup"<<std::endl;
        while(infile.good()){
            infile.read((char *)(&int_key), 8);
//            infile >> int_key;
            if(PRINT){
                cout<<int_key<<"\t";
            }
            if(int_key >= lower_bound && int_key <= upper_bound){
                res.push_back(int_key);
            }
            if(int_key > upper_bound) break;
        }
        if(PRINT) cout<<endl;
        if(PRINT){
            cout<<"range: "<<lower_bound<<","<<upper_bound<<endl;
            cout<<"results:"<<endl;
            for(uint64_t tmp: res) cout<<tmp<<"\t"<<endl;
            cout<<endl;

        }
        return res;
    }
private:
    string path_to_read_keys_;
    vector<uint64_t> keys;
    vector<uint64_t> offsets;

};
#endif //SURF_DISK_ACCESS_H
