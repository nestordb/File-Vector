/*
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 *
 * Page-wise ZoneMap (min-max filter) implementation. Keeps min max 
 * information for every page of the memory mapped file.
 * 
 * Author: Kostas Zoumpatianos <kostas@seas.harvard.edu>
 */
#pragma once
#include <iostream>
#include <memory>
#include <vector>
#include <map>
#include <set>
#include <math.h>

const float kZoneMapsPageSize = 4096;


/////////////////////////////////////////////////////////////////////////////////////////////////
//////                                  numerical  limits                                  //////
/////////////////////////////////////////////////////////////////////////////////////////////////
template <typename C>
class NumericalLimits {
    public:
    static C Min();
    static C Max();
};

template <>
uint64_t NumericalLimits<uint64_t>::Max() {
    return std::numeric_limits<uint64_t>::max();
}
template <>
uint64_t NumericalLimits<uint64_t>::Min() {
    return std::numeric_limits<uint64_t>::min();
}
template <>
int64_t NumericalLimits<int64_t>::Max() {
    return std::numeric_limits<int64_t>::max();
}
template <>
int64_t NumericalLimits<int64_t>::Min() {
    return std::numeric_limits<int64_t>::min();
}
template <>
double NumericalLimits<double>::Max() {
    return std::numeric_limits<double>::max();
}
template <>
double NumericalLimits<double>::Min() {
    return std::numeric_limits<double>::min();
}
template <>
int NumericalLimits<int>::Max() {
    return std::numeric_limits<int>::max();
}
template <>
int NumericalLimits<int>::Min() {
    return std::numeric_limits<int>::min();
}

template<typename T>
struct ZoneMap {
    T min;
    T max;
    uint64_t start_loc;
    uint64_t end_loc;   
    ZoneMap() {
        this->min = NumericalLimits<T>::Min();
        this->max = NumericalLimits<T>::Max();
        this->start_loc = 0;
        this->end_loc = 0;
    }
    ZoneMap(uint64_t end_loc) {
        this->min = NumericalLimits<T>::Min();
        this->max = NumericalLimits<T>::Max();
        this->start_loc = 0;
        this->end_loc = end_loc;
    }
    bool Intersects(T min_, T max_) {
        auto mn = min > min_ ? min : min_; 
        auto mx = max < max_ ? max : max_; 
        return mn <= mx; 
    }
    friend std::ostream& operator<<(std::ostream& os, const ZoneMap<T>& zm) {
        os << zm.start_loc << ":" << zm.end_loc << " [" << zm.min << ", " << zm.max << "]";
        return os;
    }
};


template<typename T>
struct ZoneMapSet {
    std::vector<ZoneMap<T>> zone_maps;
    uint64_t page_slots;

    ZoneMapSet(uint64_t page_size, size_t sz) {
        this->page_slots = page_size / sizeof(T);
        uint64_t total_zone_maps = ceil((double) sz / (double) this->page_slots);
        if(!total_zone_maps)
            total_zone_maps = 1;
        this->zone_maps.resize(total_zone_maps);
    }

    ZoneMapSet() {
        this->zone_maps.resize(1);
        this->page_slots = kZoneMapsPageSize / sizeof(T);
    }

    void Extend(uint64_t end_loc) {
        this->zone_maps[this->zone_maps.size() - 1].end_loc = end_loc;
        this->zone_maps[this->zone_maps.size() - 1].min = NumericalLimits<T>::Min();
        this->zone_maps[this->zone_maps.size() - 1].max = NumericalLimits<T>::Max();
    }

    void CheckExtend(uint64_t end_loc) {
        if(this->zone_maps[this->zone_maps.size() - 1].end_loc < end_loc) {
            this->zone_maps[this->zone_maps.size() - 1].end_loc = end_loc;
            this->zone_maps[this->zone_maps.size() - 1].min = NumericalLimits<T>::Min();
            this->zone_maps[this->zone_maps.size() - 1].max = NumericalLimits<T>::Max();
        }
    }

    void InitFromData(T* values, size_t sz) {
        auto nzonemaps = this->zone_maps.size();
        for(uint64_t i=0; i<nzonemaps; i++) {
            this->zone_maps[i].min = NumericalLimits<T>::Max();
            this->zone_maps[i].max = NumericalLimits<T>::Min();
            this->zone_maps[i].start_loc = i*this->page_slots;
            this->zone_maps[i].end_loc = ((i+1)*this->page_slots) < sz ? ((i+1)*this->page_slots) : sz;
        }
        
        for(uint64_t i=0; i<nzonemaps; i++) {
            for(uint64_t j=this->zone_maps[i].start_loc; j<this->zone_maps[i].end_loc; j++) {
                if(values[j] < this->zone_maps[i].min ) {
                    this->zone_maps[i].min = values[j];
                }
                if(values[j] > this->zone_maps[i].max) {
                    this->zone_maps[i].max = values[j];
                }
            }
        }
    }

    ZoneMap<T>* Get(uint64_t zmap_id) {
        return &(this->zone_maps[zmap_id]);
    }

    uint64_t Size() {
        return this->zone_maps.size();
    }

    void Print() {
        for(auto &zm : this->zone_maps) {
            std::cout << zm << std::endl;
        }
    }

     void Serialize(std::ostream& out) {
        uint64_t nzmaps = this->zone_maps.size();
        out.write((char *) &nzmaps, sizeof(uint64_t));
        out.write((char *) &(this->page_slots), sizeof(uint64_t));
        for(auto &zm : (this->zone_maps)) {
            out.write((char *) &zm, sizeof(ZoneMap<T>));
        }
    }

    void Deserialize(std::istream& in) {
        this->zone_maps.clear();
        uint64_t nzmaps = 0;
        in.read((char *) &nzmaps, sizeof(uint64_t));
        in.read((char *) &(this->page_slots), sizeof(uint64_t));
        this->zone_maps.reserve(nzmaps);
        for(uint64_t i=0; i<nzmaps; i++) {
            ZoneMap<T> zm;
            in.read((char *) &zm, sizeof(ZoneMap<T>));
            this->zone_maps.push_back(zm);
        }
    }

};
