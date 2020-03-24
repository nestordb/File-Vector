#include <iostream>
#include <cassert>
#include <chrono> 
#include "file_vector.hpp"

extern "C" {
    #include <unistd.h>
}

using namespace std;
using fv_int = file_vector<int>;

void test_out_of_range(fv_int& fv, int const i) {
    try { 
        int const tmp = fv.at(i);
    } catch (out_of_range const& e) {
        return;
    } catch (exception const& e) {
        throw runtime_error("unexpected exception.");
    }
    throw runtime_error("Did not get out-of-range exception.");
}

int main() {
    srand(1234);
    system("rm test_queries");
    fv_int vector_test_queries("test_queries", fv_int::create_file);
    for(size_t i=0; i<100000; i++) {
        vector_test_queries.push_back(rand() % 1000000);
    }
    std::sort(vector_test_queries.begin(), vector_test_queries.end());
    auto zonemaps = vector_test_queries.create_zonemaps(getpagesize(), 0);
    // zonemaps->Print();

        
    vector<int> queries = vector<int>({0, 31, 500, 677538,0, 31, 500, 677538,0, 31, 500, 677538,0, 31, 500, 677538,0, 31, 500, 677538});
    
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
