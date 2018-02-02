# Big Data Analytics (732A54) - Exercise 3 (Spark ML)
# Function Blocks

def gaussian_k(dist, h):
    import math, collections
    if isinstance(dist, collections.Iterable):
        res = []
        for x in dist:
            res.append(math.exp(float(-(x**2))/float((2*(h**2)))))
    else:
        res = math.exp(float(-(dist**2))/float((2*(h**2))))
    return res

def distHaversine(lon1, lat1, lon2,lat2, radians = 6371):
    import math

    # Convert decimal degrees to radians
    lon1, lat1, lon2,lat2 = map(math.radians, [lon1, lat1, lon2,lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat2
    a = math.sin(dlat/2)**2 + math.cos(lat1) * \
        math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return c * radians

def timeCorr(time):
    import math, collections
    if isinstance(time, collections.Iterable):
        res = []
        for x in time:
            if x <= -12:
                res.append(24 + x)
            else:
                res.append(math.fabs(x))
    else:
        if time <= -12:
            res = 24 + time
        else:
            res = math.fabs(time)

    return res


def distHours(time1, time2):
    import datetime
    a = datetime.datetime.strptime(time1, '%H:%M:%S')
    b = datetime.datetime.strptime(time2, '%H:%M:%S')
    diff_temp = (a-b).total_seconds()/3600
    diff_temp = timeCorr(diff_temp)
    return diff_temp


def distDays(day1, day2):
    import datetime
    a = datetime.datetime.strptime(day1, '%Y-%m-%d')
    b = datetime.datetime.strptime(day2, '%Y-%m-%d')
    delta = a-b
    return delta.days
