# Calculate the average rating of each genre
# In order to run this, we use spark-submit, below is the
# spark-submit  \
#   --master local[2] \
    #   AverageRatingPerGenre.py
#   --input input-path
#   --output outputfile

from pyspark import SparkContext
from ml_utils import *
import argparse
from collections import OrderedDict


if __name__ == "__main__":
    sc = SparkContext(appName="Average Rating per Genre")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='hdfs://soit-hdp-pro-1.ucc.usyd.edu.au/share/')
    parser.add_argument("--output", help="the output path",
                        default='33')
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    allVideosContent = sc.textFile(input_path + "ALLvideos.csv")
    header=allVideosContent.first()
    allVideosContent=allVideosContent.filter(lambda x:x !=header)
    data=allVideosContent.map(extractViewsToVideoPerCountry)
    dataGrouped=data.groupByKey().map(lambda x:(x[0],list(x[1])))
    keywithViewsPercent=dataGrouped.map(getViewsPercent)
    viewsFilter=keywithViewsPercent.filter(getPercentOver)#.map(printOut)
    ############################################################
    # l=viewsFilter.sortByKey(False)
    #l=viewsFilter.sortBy(lambda x:x[1],False,None)
    #l=viewsFilter.sortBy(lambda x:x[0],False,None)
    viewsSorted=viewsFilter.map(getCountryPercent).sortBy(lambda x:x[1],False,None)
    finalResult=viewsSorted.map(remap).map(lambda x:(x[0]+" ; " +x[1]+" ; "+ x[2]+"%"))
    finalResult.saveAsTextFile(output_path)
