import redis
import json
import datetime

from dateutil.relativedelta import relativedelta
from trimtab import settings

# TIME SERIES DEPTHS
YEAR   = 0
MONTH  = 1
DAY    = 2
HOUR   = 3
MINUTE = 4
SECOND = 5

# TIME SERIES CHARACTER LENGTHS
TIME_LEN = [4,6,8,10,12,14]

class Statis(object):

    _redis = None

    PREFIX = "redis_stats"
    ADD_PREFIX = True

    PATH_DELIMITER = "/"
    TIME_DELIMITER = ":"


    # CLASS METHODS :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

    @classmethod
    def get_time_key(cls, dt=datetime.datetime.now(), depth=SECOND):
        return dt.strftime('%Y%m%d%H%M%S')[0:TIME_LEN[depth]]

    @classmethod
    def get_time_keys(cls, starttime=None, endtime=None, depth=HOUR):

        # Begin iterating on start datetime
        time = starttime

        # Iterate to build time keys
        times = []
        while True:
            times.append(cls.get_time_key(time, depth))

            # Iterate on timedelta
            time =  time + cls.timedelta(1, depth)

            # Break if passing the end datetime
            if time > endtime: break;

        return times

    @classmethod
    def get_time_series(cls, dt=datetime.datetime.utcnow(), depth=SECOND):
        """Builds a tuple of strings to use for redis date key storage"""

        return (
            dt.strftime('%Y'),
            dt.strftime('%Y%m'),
            dt.strftime('%Y%m%d'),
            dt.strftime('%Y%m%d%H'),
            dt.strftime('%Y%m%d%H%M'),
            dt.strftime('%Y%m%d%H%M%S')
            )[0:depth+1]

    @classmethod
    def get_path_series(cls, path=''):
        """Builds a tuple of strings to use for redis path key storage"""

        parts = path.strip(' /').split('/')
        rtn = []
        for i in range(len(parts)):
            rtn.append('/'.join(parts[0:i+1]))
        return tuple(rtn)

    @classmethod
    def mk_key(cls, pathkey, timekey):
        return "%s%s%s%s" % ((cls.ADD_PREFIX and cls.PREFIX+cls.PATH_DELIMITER or ''), pathkey, cls.TIME_DELIMITER, timekey)

    @classmethod
    def get_key_series(cls, path='', dt=datetime.datetime.utcnow(), depth=HOUR):
        """Builds all keys used to store path-based time series data in redis"""

        times = cls.get_time_series(dt, depth=depth)
        paths = cls.get_path_series(path)

        keys = []
        for pathkey in paths:
            for timekey in times:
                keys.append(cls.mk_key(pathkey,timekey))

        return tuple(keys)

    @classmethod
    def build_stat_dict(cls, data):
        """Breaks out the data paths to store in redis"""

        rtn = dict()
        for key, value in data.iteritems():
            keys = cls.get_path_series(key)
            for k in keys:
                rtn[k] = value

        return rtn

    @classmethod
    def timedelta(cls, num, interval=HOUR):
        """Abstracted timedelta method for handling date intervals"""

        if interval == YEAR:
            rtn = relativedelta(years=num)

        elif interval == MONTH:
            rtn = relativedelta(months=num)

        elif interval == DAY:
            rtn = datetime.timedelta(num, 0)

        elif interval == HOUR:
            rtn = datetime.timedelta(0, num * 3600)

        elif interval == MINUTE:
            rtn = datetime.timedelta(0, num * 60)

        else:
            rtn = datetime.timedelta(0, num)

        return rtn


    # INSTANCE METHODS ::::::::::::::::::::::::::::::::::::::::::::::::::::::::

    def connect(self, *args, **kargs):
        self._redis = redis.StrictRedis(*args, **kargs)
        return self._redis

    def __init__(self, *args, **kargs):
        self.connect(*args, **kargs)

    def store(self, path='', data=dict(), dt=datetime.datetime.utcnow(), depth=HOUR):
        keys = self.get_key_series(path=path, dt=dt, depth=depth)
        data = self.build_stat_dict(data)

        # do all the redis shit here
        pipeline = self._redis.pipeline()
        for key in keys:
            for k,v in data.iteritems():
                pipeline.hincrby(key, k, amount=v)

        return pipeline.execute()

    def fetch(self, path='', start=0, end=0, starttime=None, endtime=None, interval=HOUR, depth=MINUTE):
        """Fetches a range of data by the given time intervals"""

        # Determine start and end dates by delta
        if not starttime:
            start = self.timedelta(start, interval)
            end = self.timedelta(end, interval)

        # Set start and end dates by delta or datetime
        starttime = starttime or (datetime.datetime.utcnow() + start)
        endtime = endtime or (datetime.datetime.utcnow() + end)

        # Get time keys
        times = self.get_time_keys(starttime, endtime, depth)

        return times

