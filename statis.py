import redis
import json
import urllib
import datetime
import logging

from dateutil.relativedelta import relativedelta

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

    KEY_PREFIX     = "statis"
    PATH_DELIMITER = "/"
    TIME_DELIMITER = ":"
    DATEKEY_NAME   = 'datekey'

    TIMEKEY_LIMIT = 3601 # 1 hour in seconds (if 0 then no-limit)

    # CLASS METHODS :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

    @classmethod
    def get_timekey(cls, dt=datetime.datetime.now(), depth=SECOND):
        return dt.strftime('%Y%m%d%H%M%S')[0:TIME_LEN[depth]]

    @classmethod
    def get_timekeys(cls, starttime=None, endtime=None, depth=HOUR, limit=3601):

        # Begin iterating on start datetime
        time = starttime

        # Iterate to build time keys
        times = []
        i = 1
        while True:
            times.append(cls.get_timekey(time, depth))

            # Iterate on timedelta
            time = time + cls.timedelta(1, depth)

            # Break if passing the end datetime
            if time > endtime: break;

            # Break if we hit our safety limit
            if limit and i >= limit: break;
            i += 1

        return times

    @classmethod
    def timekey_to_datetime(cls, timekey):

        rtn = None
        if len(timekey) == 4:
            rtn = datetime.datetime(int(timekey[0:4]),1,1,0,0,0)
        elif len(timekey) == 6:
            rtn = datetime.datetime(int(timekey[0:4]),int(timekey[4:6]),1,0,0,0)
        elif len(timekey) == 8:
            rtn = datetime.datetime(int(timekey[0:4]),int(timekey[4:6]),int(timekey[6:8]),0,0,0)
        elif len(timekey) == 10:
            rtn = datetime.datetime(int(timekey[0:4]),int(timekey[4:6]),int(timekey[6:8]),int(timekey[8:10]),0,0)
        elif len(timekey) == 12:
            rtn = datetime.datetime(int(timekey[0:4]),int(timekey[4:6]),int(timekey[6:8]),int(timekey[8:10]),int(timekey[10:12]),0)
        elif len(timekey) == 14:
            rtn = datetime.datetime(int(timekey[0:4]),int(timekey[4:6]),int(timekey[6:8]),int(timekey[8:10]),int(timekey[10:12]),int(timekey[12:14]))

        return rtn

    @classmethod
    def get_keys(cls, path="", starttime=None, endtime=None, depth=HOUR):
        """Builds keys based on a path and time series"""
        keys = cls.get_timekeys(starttime=starttime, endtime=endtime, depth=depth)
        for i in range(0,len(keys)):
            keys[i] = cls.make_key(path, keys[i])

        return keys

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
    def make_path(cls, *args):
        """Builds a storage path with correct serialization from a list of parts"""
        parts = list(args)
        for i in range(len(parts)):
            parts[i] = parts[i] and urllib.quote_plus(parts[i]) or ''
        return parts and '/'.join(parts) or ''

    @classmethod
    def get_path_series(cls, path=''):
        """Builds a tuple of strings to use for redis path key storage"""

        parts = path.strip(' /').split('/')
        rtn = []
        for i in range(len(parts)):
            rtn.append('/'.join(parts[0:i+1]))
        return tuple(rtn)

    @classmethod
    def make_key(cls, pathkey, timekey):
        return "%s%s%s%s" % (cls.KEY_PREFIX + cls.PATH_DELIMITER , pathkey, cls.TIME_DELIMITER, timekey)

    @classmethod
    def parse_key(cls, key):
        """Returns a tuple of pathkey and timekey from a given key"""
        return key.replace(cls.KEY_PREFIX + cls.PATH_DELIMITER, '', 1).split(cls.TIME_DELIMITER)

    @classmethod
    def get_key_series(cls, path='', dt=datetime.datetime.utcnow(), depth=HOUR):
        """Builds all keys used to store path-based time series data in redis"""

        times = cls.get_time_series(dt, depth=depth)
        paths = cls.get_path_series(path)

        keys = []
        for pathkey in paths:
            for timekey in times:
                keys.append(cls.make_key(pathkey,timekey))

        return tuple(keys)

    @classmethod
    def build_stats_dict(cls, stats):
        """Breaks out the data paths to store in redis"""

        rtn = dict()
        if isinstance(stats,(list,tuple)):
            for key in stats:
                keys = cls.get_path_series(key)
                for k in keys:
                    rtn[k] = 1
        else:
            for key, value in stats.iteritems():
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

    def store(self, path='', stats=dict(), dt=datetime.datetime.utcnow(), depth=HOUR):
        keys = self.get_key_series(path=path, dt=dt, depth=depth)
        stats = self.build_stats_dict(stats)

        # do all the redis shit here
        pipeline = self._redis.pipeline()
        for key in keys:
            for k,v in stats.iteritems():
                pipeline.hincrby(key, k, amount=v)

        return pipeline.execute()

    @staticmethod
    def _hmget_to_integers(values):
        """Provides a callback to the redis hmget command and casts all results as integers"""
        for i in range(0,len(values)):
            values[i] = values[i] != None and int(values[i]) or  0

        return values

    def fetch_stats(self, path='', stats=[], starttime=None, endtime=None, depth=HOUR):
        """Fetches stats for a given time range and interval"""

        # get keys
        keys = self.get_keys(path, starttime, endtime, depth)

        pipeline = self._redis.pipeline()
        pipeline.set_response_callback('HMGET', self._hmget_to_integers)
        for key in keys:
            pipeline.hmget(key, stats)

        values = pipeline.execute()
        for i in range(0,len(values)):
            values[i].insert(0, self.parse_key(keys[i])[1])

        return values

    def fetch_all(self, path='', starttime=None, endtime=None, depth=HOUR):
        """Fetches all stats for a given time range and interval"""

        # get keys
        keys = self.get_keys(path, starttime, endtime, depth)

        pipeline = self._redis.pipeline()
        for key in keys:
            pipeline.hgetall(key)
            logging.debug('FETCHING ALL DATA IN %s' % key)

        values = pipeline.execute()
        for i in range(0,len(values)):
            for k,v in values[i].iteritems():
                values[i][k] = v.isdigit() and int(v) or v
            values[i][self.DATEKEY_NAME] = self.parse_key(keys[i])[1]

        return values

    def fetch(self, path='', stats=[], starttime=None, endtime=None, depth=HOUR, start=0, end=0, interval=DAY) :
        """Fetches a range of data by the given time intervals"""

        # Determine start and end dates by delta
        if not starttime:
            start = self.timedelta(start, interval)
            end = self.timedelta(end, interval)

        # Set start and end dates by delta or datetime
        starttime = starttime or (datetime.datetime.utcnow() + start)
        endtime = endtime or (datetime.datetime.utcnow() + end)

        if stats:
            return self.fetch_stats(path=path, stats=stats, starttime=starttime, endtime=endtime, depth=depth)
        else:
            return self.fetch_all(path=path, starttime=starttime, endtime=endtime, depth=depth)

