#include <iostream>
#include <cassert>
#include <chrono> 
#include "file_vector.hpp"

extern "C" {
    #include <unistd.h>
}

using namespace std;

int main() {
    // Initialize random number generator
    srand(1234);
    // Remove previous - if any - memory mapped file
    system("rm test_queries");
    // Create memory mapped vector
    file_vector<int> vector_test_queries("test_queries", file_vector<int>::create_file);
    // Insert 100K numbers in the range of 0 to 1M
    for(size_t i=0; i<100000; i++) {
        vector_test_queries.push_back(rand() % 1000000);
    }
    // Sort the vector
    std::sort(vector_test_queries.begin(), vector_test_queries.end());
    // Construct zone-maps
    auto zonemaps = vector_test_queries.create_zonemaps(getpagesize(), 0);
       

    // Create a set of queries (the data that we are looking for)
    vector<int> queries = vector<int>({0, 31, 500, 677538,0, 31, 500, 677538,0, 31, 500, 677538,0, 31, 500, 677538,0, 31, 500, 677538});
    
    // Find each query with a full scan (would never be used in practice given that this is a sorted array, but used for simplicity)
    auto start = std::chrono::high_resolution_clock::now(); 
    int found = 0;
    for(auto &query : queries) {
        for(auto &v : vector_test_queries) {
            if(v == query) {
                found++;
                break;
            }
        }
    }
    auto stop = std::chrono::high_resolution_clock::now(); 
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); 
    std::cout << "[SCAN]\t\t Found " << found << " matches in " << duration.count() << " μs" << std::endl;

    // Find each query using a zone-map based skip-sequential scan. 
    start = std::chrono::high_resolution_clock::now(); 
    found = 0;
    for(auto &query : queries) {
        bool found_it = false;
        for(size_t i=0; i<zonemaps->Size(); i++) {
            auto zm = zonemaps->Get(i);
            if(zm->Intersects(query, query)) {
                for(size_t j=zm->start_loc; j<zm->end_loc; j++) {
                    if(vector_test_queries.at(j) == query) {
                        found++;
                        found_it = true;
                        break;
                    }
                }
            }
            if(found_it)
                break;
        }
    }
    stop = std::chrono::high_resolution_clock::now(); 
    duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); 
    std::cout << "[ZONEMAPS]\t Found " << found << " matches in " << duration.count() << " μs" << std::endl;
}
