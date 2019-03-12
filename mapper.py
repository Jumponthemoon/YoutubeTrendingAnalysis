#!/usr/bin/python3

import sys
import csv

def mapper(country_name):
    '''Input format:
    video_id,trending_date,title,channel_title,category_id,category,publish_time,
    tags,views,likes,dislikes,comment_count,thumbnail_link,
    comments_disabled,ratings_disabled,video_error_or_removed,description,country
    '''
    for line in sys.stdin:
        csv_list= list(csv.reader([line]))
        video_id=csv_list[0][0]
        category=csv_list[0][5]
        country=csv_list[0][17]

        # In hadoop streaming, the output is sys.stdout, thus the print command
        # Now print out the data that will be passed to the reducer
        if country in country_name:
            print("{}\t{}\t{}".format(country,category,video_id))

if __name__ == "__main__":
    python_arguments=sys.argv
    country_name=["GB","US"]
    if len(python_arguments)>2:
        country_name[0]=python_arguments[1]
        country_name[1]=python_arguments[2]
    mapper(country_name)
