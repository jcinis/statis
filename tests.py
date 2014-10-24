import datetime
import statis
stats = statis.Statis(host='192.168.9.42',port=6379,db=11)
statis.Statis.KEY_PREFIX = 'tracker_stats'
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
statis.Statis.KEY_PREFIX = 'tracker_stats'
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
statis.Statis.KEY_PREFIX = 'tracker_stats'
data = stats.fetch(
    path="188a39c9-043a-4afa-887c-ee00fcc3d65d",
    stats=["impression"],
    starttime=datetime.datetime.now() + datetime.timedelta(-1,0),
    endtime=datetime.datetime.now(),
    depth=statis.HOUR
)
print data

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

import datetime
import statis
stats = statis.Statis(host='192.168.9.42',port=6379,db=11)
statis.Statis.KEY_PREFIX = 'tracker_stats'
data = stats.fetch(
    path="188a39c9-043a-4afa-887c-ee00fcc3d65d",
    stats=["respondent","impression"],
    starttime=datetime.datetime.now() + datetime.timedelta(-1,0),
    endtime=datetime.datetime.now(),
    depth=statis.HOUR
)
print data

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

import datetime
import statis
import json

stats = statis.Statis(host='192.168.9.42',port=6379,db=11)
statis.Statis.KEY_PREFIX = 'tracker_stats'
stats = stats.fetch_all(
    path="188a39c9-043a-4afa-887c-ee00fcc3d65d",
    starttime=datetime.datetime.now() + datetime.timedelta(-3,0),
    endtime=datetime.datetime.now() + datetime.timedelta(-2,0),
    depth=statis.HOUR
)
print json.dumps(stats, indent=4)

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

import datetime
import statis

stats = statis.Statis(host='192.168.9.42',port=6379,db=11)
statis.Statis.KEY_PREFIX = 'tracker_stats'
stats = stats.build_stats_dict([
	'browser/chrome/44',
	'usa/la/nola',
])
print stats


# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::



stats.store('188a39c9-043a-4afa-887c-ee00fcc3d65d', { 'impression': 1, 'respondent': 1})

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

import datetime
import statis

stats = statis.Statis(host='192.168.9.42',port=6379,db=11)
statis.Statis.KEY_PREFIX = 'tracker_stats'
results = stats.fetch_stats(
    path="188a39c9-043a-4afa-887c-ee00fcc3d65d",
	stats=['impression','respondent'],
    starttime=datetime.datetime.now() + datetime.timedelta(-1,0),
    endtime=datetime.datetime.now(),
    depth=statis.HOUR
)
print results

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

stats.store('188a39c9-043a-4afa-887c-ee00fcc3d65d', { 'impression': 1, 'respondent': 1})

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

import datetime
import statis
import sys

limit = 100
i = 0

def parse_line(line):
	"""
	id CHAR(36) ENCODE lzo,
    campaign_id CHAR(36) ENCODE lzo,
    visitor_id VARCHAR(80) ENCODE lzo,
    has_ftr INTEGER ENCODE bytedict,
    referrer VARCHAR(1000) ENCODE lzo,
    respondent_id CHAR(36) ENCODE lzo,
    placement_id VARCHAR(255) ENCODE lzo,
    placement_name VARCHAR(255) ENCODE lzo,
    site_id VARCHAR(255) ENCODE lzo,
    creative_id VARCHAR(255) ENCODE lzo,
    dfa_id VARCHAR(255) ENCODE lzo,
    pagetype VARCHAR(255) ENCODE lzo,
    order_id VARCHAR(255) ENCODE lzo,
    revenue VARCHAR(255) ENCODE lzo,
    product_id VARCHAR(255) ENCODE lzo,
    dfa_id_p10 VARCHAR(255) ENCODE lzo,
    created_dt TIMESTAMP,

	['d0314f3c-4fb4-469f-8bbe-5cf6812183e1', '161bdadb-1a5e-462e-93b7-45b70e28ae28', '38bc8a9e-4a2d-40c3-9ddd-6fd7290080ed', '0', '', '', '111977682', '8248251', 'N37601.152854POPSUGARMEDIASUGARI', '59815312', '2497145872907894921', 'NA', 'NA', 'NA', 'NA', 'NA', '2014-10-13 00:02:22']
	"""
	
	keys = [
	    'id',
	    'campaign_id',
	    'visitor_id',
	    'has_ftr',
	    'referrer',
	    'respondent_id',
	    'placement_id',
	    'placement_name',
	    'site_id',
	    'creative_id',
	    'dfa_id',
	    'pagetype',
	    'order_id',
	    'revenue',
	    'product_id',
	    'dfa_id_p10',
	    'created_dt',
	]
		
	args = [v.strip('"') for v in line.strip("\n").split("\t")]
	kargs = {}
	for i in range(0,len(args)):
		key = keys[i]
		kargs[key] = args[i]
		if kargs[key] == 'created_dt':
			
		
	return kargs

f = open("data/20141013_0000_part_00", "r")
for line in f:
	i += 1
	#sys.stdout.write("\rImporting records: %s" % i)
	#sys.stdout.flush()
	
	print parse_line(line)
	
	if i >= limit:
		break
	
	sys.stdout.write("\r")

f.close()

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::


with open("data/20141013_0000_part_00", "r") as lines:
	while line = next(lines):
		print line
	
i = 1
while line in f:k
	print line
	i += 1
	if i > limit:
		break;
		
f.close()


import sys
for i in range(0,100):
	sys.stdout.write("\r%s" % i)
	sys.stdout.flush()




stats = statis.Statis(host='192.168.9.42',port=6379,db=11)
statis.Statis.KEY_PREFIX = 'tracker_stats'
results = stats.fetch_stats(
    path="188a39c9-043a-4afa-887c-ee00fcc3d65d",
	stats=['impression','respondent'],
    starttime=datetime.datetime.now() + datetime.timedelta(-1,0),
    endtime=datetime.datetime.now(),
    depth=statis.HOUR
)
print results














