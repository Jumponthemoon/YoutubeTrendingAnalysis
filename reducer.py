#!/usr/bin/python3

import sys

def read_map_output(file):
    for line in file:
        yield line.strip().split("\t",2)

def reducer():
    firstCountry=sys.argv[1]
    secondCountry=sys.argv[2]
    firstCountryDic={}
    secondCountryDic={}

    for country, category,video_id in read_map_output(sys.stdin):
        if country==firstCountry:
            firstCountryDic.setdefault(category,set()).add(video_id)
        if country==secondCountry:
            secondCountryDic.setdefault(category,set()).add(video_id)

    for category1,video_id1 in firstCountryDic.items():
        for category2,video_id2 in secondCountryDic.items():
            if category1==category2:
                repeat=set(video_id1 & video_id2)
                repeatPercent=len(repeat)/len(video_id1)*100
        print("{}; total: {}; {}% in {} ".format(category1,len(video_id1),round(repeatPercent,2),secondCountry))


if __name__ == "__main__":
    reducer()
