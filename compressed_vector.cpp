#include <iostream>
#include "file_vector.hpp"

#define page_size 4096

struct compressed_block {
    char data[page_size];
    size_t n_records;
    size_t next_block;
};

template <typename K>
struct compressed_vector {
    file_vector<compressed_block> *vec;

    compressed_vector(std::string filename) {

    }

    compress(file_vector<K> *src, size_t page_size) {
        size_t compressed_chunk_size = page_size / sizeof(K);
        K data[compressed_chunk_size];

        size_t i=0;
        for(auto &rec : *src) {
            data[i++] = rec;
            if(i == compressed_chunk_size) {
                // TODO: Compress data vector
                i = 0;
            }
        }
        // TODO: handle left-overs
    }
};

int main(int argc, char **argv) {

}
