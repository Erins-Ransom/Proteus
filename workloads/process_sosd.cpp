#include <string>
#include <cstring>
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <random>
#include <cassert>
#include <iterator>
#include <experimental/filesystem> // C++14 compatible

int main(void) {
    const std::string SOSD_DIR = "./SOSD";
    assert(std::experimental::filesystem::exists(SOSD_DIR));
    for (auto const & filename : std::experimental::filesystem::directory_iterator{SOSD_DIR}) {
        std::fstream file;
        std::cout << "Processing " << filename.path().c_str() << std::endl;
        file.open(filename.path().c_str(), std::ios::out | std::ios::in | std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Unable to open " << filename << std::endl;
            exit(EXIT_FAILURE);
        }

        std::vector<uint64_t> data;

        // Read size
        uint64_t size;
        file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
        data.resize(size);

        // Read values
        file.read(reinterpret_cast<char*>(data.data()), size * sizeof(uint64_t));

        // Shuffle data
        std::default_random_engine rng(2021);
        std::shuffle(std::begin(data), std::end(data), rng);

        // Seek to start and write data back to file
        file.seekg(sizeof(uint64_t), file.beg);
        file.write(reinterpret_cast<char*>(data.data()), size * sizeof(uint64_t));
        file.close();
    }

    return 0;
}