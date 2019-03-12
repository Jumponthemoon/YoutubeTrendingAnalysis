def extractViewsToVideoPerCountry(record):
    """This function converts entries of ALLvideos.csv into key,value pair of the following format
    (Country,video_id Views)
    since there may be multiple view per country ,video_id, this function returns a list of tuples
    Args:
        record (str): A row of CSV file, with columns separated by comma
    Returns:
        The return value is a list of tuples, each tuple contains (Country,Video_id,Views)
    """
    try:
        parts= record.strip().split(",")
        views=""
        for i in range(len(parts)):
            if len(parts[i])>4:
                if parts[i][-5:]==".000Z":
                    views=parts[i+2]
                    break
        video_id=parts[0].strip()
        country=parts[-1].strip()
        return ((country,video_id),views)
    except:
        return ()


def getViewsPercent(pair):
    key=pair[0]
    views=pair[1]
    if len(views)>=2:
        percent=(int(views[1])-int(views[0]))/int(views[0])*100
        return (key, percent)
    else:
        return (key,0)

def getPercentOver(pair):
    countryVideo=pair[0]
    viewsPercent=pair[1]
    if viewsPercent>1000:
        return (countryVideo,round(viewsPercent,1))

def printOut(line):
    key,percent=line
    return(key,round(percent,1))

def getCountryPercent(line):
    key,percent=line
    country=key[0]
    video_id=key[1]
    return(video_id,(country,percent))

def remap(line):
    video_id=line[0]
    key=line[1]
    country=key[0]
    percent=key[1]
    return(country,video_id,str(percent))
