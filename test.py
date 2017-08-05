import os
import unittest
import logging
import datetime
import statis
import json
from dateutil.parser import parse

REDIS_HOST = os.environ.get('STATIS_REDIS_HOST','statis-redis')
REDIS_PORT = os.environ.get('STATIS_REDIS_PORT','6379')

class StatisTestCase(unittest.TestCase):

    filepath = 'tests/ufos.tsv'
    counts = {
        '20160101':40,
        '20160102':21,
        '20160103':15,
        '20160104':15,
        '20160105':15,
        '20160106':21,
        '20160107':8,
        '20160108':9,
        '20160109':7,
        '20160110':5,
        '20160111':15,
        '20160112':11,
        '20160113':16,
        '20160114':18,
        '20160115':10,
        '20160116':16,
        '20160117':7,
        '20160118':10,
        '20160119':8,
        '20160120':6,
        '20160121':13,
        '20160122':7,
        '20160123':15,
        '20160124':16,
        '20160125':10,
        '20160126':7,
        '20160127':12,
        '20160128':17,
        '20160129':18,
        '20160130':16,
        '20160131':14,
    }

    def setUp(self):
        '''Loading data into statis'''

        self.stats = statis.Statis(host=REDIS_HOST, port=REDIS_PORT)
        statis.Statis.KEY_PREFIX = 'test'

        with open(self.filepath, 'r') as fp:
            for line in fp:
                line = line.split("\t")

                date = parse(line[0])
                city = line[1].strip().lower()
                state = line[2].strip().lower()
                shape = line[3].strip().lower()
                duration = line[4].strip().lower()

                path = self.stats.make_path('sightings',state,city)
                stat = ['sighting',]
                stat.append(self.stats.make_path('shapes',shape))
                stat.append(self.stats.make_path('duration',duration))

                logging.info('loading', date, path, stat)
                self.stats.store( path, stat , dt=date, depth=statis.HOUR)

    def test_daily_aggregations(self):

        start_dt = parse('2016-01-01 00:00:00')
        end_dt   = parse('2016-01-31 23:59:59')
        sightings = self.stats.fetch('sightings',[],start_dt,end_dt, statis.DAY)

        for day in sightings:
            logging.info(
                self.stats.timekey_to_datetime(day.get('datekey')),
                day.get('datekey'),
                day.get('sighting',''),
            )

            self.assertEqual(
                day.get('sighting',0),
                self.counts.get(day.get('datekey'),0)
            )

if __name__ == "__main__":
    unittest.main()
