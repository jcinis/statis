import datetime
import statis
stats = statis.Statis(host='192.168.9.42',port=6379,db=11)
statis.Statis.PREFIX = 'tracker_stats'
keys = stats.get_keys(
    path="188a39c9-043a-4afa-887c-ee00fcc3d65d/TURNINC",
    starttime=datetime.datetime.now() + datetime.timedelta(-1,0),
    endtime=datetime.datetime.now(),
    depth=statis.HOUR
)
print keys

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

import datetime
import statis
stats = statis.Statis(host='192.168.9.42',port=6379,db=11)
statis.Statis.PREFIX = 'tracker_stats'
data = stats.fetch(
    path="188a39c9-043a-4afa-887c-ee00fcc3d65d",
    starttime=datetime.datetime.now() + datetime.timedelta(-1,0),
    endtime=datetime.datetime.now(),
    depth=statis.HOUR
)
print data

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

import datetime
import statis
stats = statis.Statis(host='192.168.9.42',port=6379,db=11)
statis.Statis.PREFIX = 'tracker_stats'
data = stats.fetch(
    path="188a39c9-043a-4afa-887c-ee00fcc3d65d",
    stats=["respondent","impression"],
    starttime=datetime.datetime.now() + datetime.timedelta(-1,0),
    endtime=datetime.datetime.now(),
    depth=statis.HOUR
)
print data
