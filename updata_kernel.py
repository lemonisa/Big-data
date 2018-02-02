from math import radians, cos, sin, asin, sqrt, exp, fabs
from datetime import datetime
from pyspark import SparkContext

sc = SparkContext(appName = "Kernel")

# (Station, (lat, long))
stations = sc.textFile("/user/x_yumli/stations.csv") \
                .map(lambda line: line.split(";")) \
                .map(lambda x: (x[0], (float(x[3]),float(x[4]))))

# broadcast smaller RDD to join
rdd = stations.collectAsMap()
stations_bc = sc.broadcast(rdd)

# (station, (date, time, temp))
TempReadings = sc.textFile("/user/x_yumli/temperature-readings.csv") \
                 .map(lambda line: line.split(";")) \
                 .map(lambda x: (x[0], (str(x[1]), str(x[2]), float(x[3]))))

def joinfunc(x):
    dictValues = list(stations_bc.value[x[0]])
    values = list(x[1])
    values.extend((dictValues[0],dictValues[1]))
    res = (x[0],tuple(values))
    return res

# (station, (date, time, temp, lat, long))
# [(u'102170', ('2013-11-01', '06:00:00', 6.8, 60.2788, 12.8538))
training = TempReadings.map(lambda row: joinfunc(row))
#print training.take(10)

# def function()
def gaussian_k(dist, h):
    import collections
    if isinstance(dist, collections.Iterable):
        res = []
        for x in dist:
            res.append(exp(float(-(x**2))/float((2*(h**2)))))
    else:
        res = exp(float(-(dist**2))/float((2*(h**2))))
    return res

def distHaversine(lon1, lat1, lon2,lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2,lat2 = map(radians, [lon1, lat1, lon2,lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat2
    a = sin(dlat/2)**2 + cos(lat1) * \
        cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def time(time):
    import collections
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
            res = fabs(time)
    return res

def distHours(time1, time2):
    x = datetime.strptime(time1, '%H:%M:%S')
    y = datetime.strptime(time2, '%H:%M:%S')
    dist = (x-y).total_seconds()/3600
    dist = time(dist)
    return dist

def distDays(day1, day2):
    x = datetime.strptime(day1, '%Y-%m-%d')
    y = datetime.strptime(day2, '%Y-%m-%d')
    dist = x-y
    return dist.days

def mainfun(pred, training):
    start_time = datetime.now()
    temp = training.filter(lambda x: \
                datetime.strptime(x[1][0], '%Y-%m-%d') < datetime.strptime(pred[1], '%Y-%m-%d')).cache() \
            .map(lambda x: (x[1][2], ( \
                        distHours(pred[0],x[1][1]), \
                        distDays(pred[1], x[1][0]), \
                        distHaversine(lon1 = pred[2], lat1 = pred[3], lon2 = x[1][4], lat2 = x[1][3])))) \
            .map(lambda (temp, (distTime, distDays, distKM)): \
                (temp,(gaussian_k(distTime, h = 5), gaussian_k(distDays, h = 5), gaussian_k(distKM, h = 5)))) \
            .map(lambda (temp, (k1, k2, k3)): (temp, k1 + k2 + k3)) \
            .map(lambda (temp, sumK): (temp, (temp*sumK, sumK))) \
            .map(lambda (temp, (up, down)): (None, (up, down))) \
            .reduceByKey(lambda (u1, d1), (u2, d2): (u1 + u2, d1 + d2)) \
            .map(lambda (key,(sumup,sumdown )): (float(sumup)/float(sumdown)))

    return temp

# test point
# change time from 4 to 24
pred = ('4:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('6:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('8:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('10:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('12:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('14:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('16:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('18:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('20:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('22:00:00', '2013-11-04',float(58.4274), float(14.826))
#      ('0:00:00', '2013-11-05',float(58.4274), float(14.826))

test = mainfun(pred = pred, training = training)
print test.take(1)
#[3.755000774143026]
#[4.124893796269659]
#[4.604289855520156]
#[5.0571154410941555]
#[5.3742917018759115]
#[5.507777560126942]
#[5.462898361114774]
#[5.288262380654837]
#[5.046578500616714]
#[4.7878195623542865]
#[3.685973850412136]
