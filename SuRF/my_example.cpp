#include <iostream>
#include <vector>
#include <random>
#include <map>
#include <fstream>
#include <sys/stat.h> 
#include <sys/types.h> 
#include <string>

#include "include/surf.hpp"

using namespace surf;
using namespace std;

vector<std::string>generateKeys(int no_of_keys, int key_length, bool print)
{
    std::vector<std::string> keys;
    std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    uniform_int_distribution<int> uni_dist(0, pow(2, key_length)-1);
    keys.reserve(no_of_keys);
    while (keys.size() < no_of_keys)
    {
        unsigned long number = uni_dist(gen);
        if ((number >= 0.0) && (number < pow(2, key_length)))
        {
            if(print)
            {
                std::cout << number << " -> " << to_string(number) << std::endl;
            }
            keys.push_back(to_string(number));
        }
    }
    // copy to file
    const char * dir = "my_data"; 
    mkdir(dir, 0777);
    std::ofstream output_file(dir + std::string("/data.txt"));
    std::ostream_iterator<std::string> output_iterator(output_file, "\n");
    std::copy(keys.begin(), keys.end(), output_iterator);
    output_file.close();
    return keys;
}

void generateRangeQueries(int key_length, int type_of_range_query, unsigned long max_range_size, int max_for_a_type, multimap<int, unsigned long>& range_query_spec, bool print)
{
    srand (time(NULL));
    for(int i = 1;i <= type_of_range_query; i++)
    {
        int count = rand()%max_for_a_type;
        unsigned long range_size = pow(4,i); //rand()%max_range_size;
        //cout << max_range_size << std::endl;
        range_query_spec.insert(pair<int, unsigned long>(count, range_size));
    }
    multimap<int, unsigned long>::iterator it;
    if(print)
    {
        for ( it = range_query_spec.begin(); it != range_query_spec.end(); it++ )
        {
            std::cout << it->first  // string (#queries)
              << " : "
              << it->second   // query range
              << std::endl;
        }
    }
}

void getTransactionKeys(int key_length, multimap<int, unsigned long> range_query_spec, multimap<unsigned long, unsigned long>& txn_keys)
{
    multimap<int, unsigned long>::iterator it;
    for ( it = range_query_spec.begin(); it != range_query_spec.end(); it++ )
    {
        unsigned long left_key_domain = pow(2, key_length) - 1000 - it->second; 
        if(left_key_domain == 0)
        {
            left_key_domain = 1;
        }
        for(int i = 1; i<= it->first; i++)
        {
            unsigned long left_key = rand()%(left_key_domain); 
            unsigned long right_key = left_key + it->second; 
            txn_keys.insert(pair<unsigned long, unsigned long>(left_key, right_key)); 
        }
    }
    // copy to file
    multimap<unsigned long, unsigned long>::iterator m_it;
    std::ofstream output_file1(std::string("my_data/txn.txt"));
    std::ofstream output_file2(std::string("my_data/upper_bound.txt"));
    for (m_it = txn_keys.begin(); m_it != txn_keys.end(); m_it++)
    {
        //output_file1<< m_it->first << "\t" << m_it->second << "\n";
        //std::cout << m_it->first << " " << m_it->second << std::endl;
        output_file1<< m_it->first << "\n";
        output_file2<< m_it->second << "\n";
    }
    output_file1.close();
    output_file2.close();
}

int64_t getTruePositives(std::vector<std::string> keys, multimap<unsigned long, unsigned long> txn_keys)
{
    std::map<std::string, bool> ht;
    for (int i = 0; i < (int)keys.size(); i++)
    ht[keys[i]] = true;

    int64_t true_positives = 0;
    std::map<std::string, bool>::iterator ht_iter;
    multimap<unsigned long, unsigned long>::iterator it;
    for ( it = txn_keys.begin(); it != txn_keys.end(); it++ )
    //for (int i = 0; i < (int)txn_keys.size(); i++) 
    {
        ht_iter = ht.lower_bound(to_string(it->first));
        if (ht_iter != ht.end()) {
        std::string fetched_key = ht_iter->first;
        true_positives += (fetched_key.compare(to_string(it->second)) < 0);
        }
    }
    return true_positives;
}

int main() {

    // insert keys into SuRF
    long no_of_keys = 100000;
    int key_length = 32; // try to set length not more than 63
    unsigned long max_range_size = pow(2, key_length/2);
    std::vector<std::string> keys = generateKeys(no_of_keys, key_length, false);

    // generate range queries
    multimap<int, unsigned long> range_query_spec;
    int type_of_range_query = 5;
    int max_for_a_type = 500;
    generateRangeQueries(key_length, type_of_range_query, max_range_size, max_for_a_type, range_query_spec, true);
    // get transaction keys
    multimap<unsigned long, unsigned long> txn_keys;
    getTransactionKeys(key_length, range_query_spec, txn_keys);
    exit(0);
    // basic surf
    /*SuRF* surf = new SuRF(keys);

    // use default dense-to-sparse ratio; specify suffix type and length
    SuRF* surf_hash = new SuRF(keys, surf::kHash, 8, 0);
    SuRF* surf_real = new SuRF(keys, surf::kReal, 0, 8);

    // customize dense-to-sparse ratio; specify suffix type and length
    SuRF* surf_mixed = new SuRF(keys, true, 16,  surf::kMixed, 4, 4);

    // do range lookups
    multimap<unsigned long, unsigned long>::iterator it;
    int64_t positives_basic = 0, positives_hash = 0, positives_real = 0, positives_mixed = 0;
    int64_t true_positives = getTruePositives(keys, txn_keys); 
    for ( it = txn_keys.begin(); it != txn_keys.end(); it++ )
    {
        //std::cout << c << ". " << it->first << " -> " << it->second << " (" << it->second-it->first << ")" << std::endl;
        positives_basic += surf->lookupRange(to_string(it->first), true, to_string(it->second), false);
        positives_hash += surf_hash->lookupRange(to_string(it->first), true, to_string(it->second), false);
        positives_real += surf_real->lookupRange(to_string(it->first), true, to_string(it->second), false);
        positives_mixed += surf_mixed->lookupRange(to_string(it->first), true, to_string(it->second), false);
    } 
    std::cout << "********* SuRF basic ********* " << std::endl;
    int64_t false_positives = true_positives < positives_basic ? positives_basic - true_positives : 0;
    //assert(false_positives >= 0);
    int64_t true_negatives = txn_keys.size() - positives_basic;
    std::cout << "Memory usage is of SuRF " << surf->getMemoryUsage() << std::endl;
    std::cout << "positives: " << positives_basic << std::endl;
    std::cout << "true positives: " << true_positives << std::endl;
    std::cout << "false positives: " << false_positives << std::endl;

    std::cout << "********* SuRF hash ********* " << std::endl;
    false_positives = true_positives < positives_hash ? positives_hash - true_positives : 0;
    true_negatives = txn_keys.size() - positives_hash;
    std::cout << "Memory usage is of SuRF " << surf_hash->getMemoryUsage() << std::endl;
    std::cout << "positives: " << positives_hash << std::endl;
    std::cout << "true positives: " << true_positives << std::endl;
    std::cout << "false positives: " << false_positives << std::endl;

    std::cout << "********* SuRF real ********* " << std::endl;
    false_positives = true_positives < positives_real ? positives_real - true_positives : 0;
    true_negatives = txn_keys.size() - positives_real;
    std::cout << "Memory usage is of SuRF " << surf_real->getMemoryUsage() << std::endl;
    std::cout << "positives: " << positives_real << std::endl;
    std::cout << "true positives: " << true_positives << std::endl;
    std::cout << "false positives: " << false_positives << std::endl;

    std::cout << "********* SuRF mixed ********* " << std::endl;
    false_positives = true_positives < positives_mixed ? positives_mixed - true_positives : 0;
    true_negatives = txn_keys.size() - positives_mixed;
    std::cout << "Memory usage is of SuRF " << surf_mixed->getMemoryUsage() << std::endl;
    std::cout << "positives: " << positives_mixed << std::endl;
    std::cout << "true positives: " << true_positives << std::endl;
    std::cout << "false positives: " << false_positives << std::endl;*/
    return 0;
}


