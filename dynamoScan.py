import boto3
import json
import geojson
import decimal
import time
from bson.json_util import loads
from dateutil.parser import parse
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
import HTMLParser # parsing unicode text/symbols in py2.7
html_parser = HTMLParser.HTMLParser()

import settings

def get_table_metadata(table_name):
    table = dynamodb_resource.Table(table_name)
    return {
        'num_items': table.item_count,
        'primary_key_name': table.key_schema[0],
        'status': table.table_status,
        'bytes_size': table.table_size_bytes,
        'global_secondary_indices': table.global_secondary_indexes
    }

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def scan_table_filtered(table_name, filter_key, filter_value):
    table = dynamodb_resource.Table(table_name)
    filtering_exp = Key(filter_key).gt(filter_value)
    response = table.scan(FilterExpression=filtering_exp)
    items = response['Items']
    while True:
     time.sleep(10)
     print(len(items))
     if response.get('LastEvaluatedKey'):
      response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],FilterExpression=filtering_exp)
      items += response['Items']
     else:
      break
    return items


dynamodb_resource = boto3.resource('dynamodb', 
 aws_access_key_id = ''.join(settings.access_key_id),
 aws_secret_access_key = ''.join(settings.secret_access_key),
 region_name=''.join(settings.region))

table_name = ''.join(settings.table_name)
filter_key = ''.join(settings.filter_key)

recordLag = datetime.utcnow() - timedelta(days=1) 
filter_value  = recordLag.strftime('%Y-%m-%dT%H:%M:%S')

table = dynamodb_resource.Table(table_name)
metadat = get_table_metadata(table_name)
dbOut = scan_table_filtered(table_name, filter_key, filter_value)

recCount = 0
feature_list = []
for rec in dbOut:
 recCount += 1
 tweet_payload  = loads(geojson.dumps(rec,cls=DecimalEncoder))
 feature_properties = dict(lang = tweet_payload['lang'],
 twitter_handle = ur"@{}".format(tweet_payload['user']['screen_name']),
 time = parse(tweet_payload['created_at']).isoformat())
 payload_text = html_parser.unescape(tweet_payload['text'].replace("\n", " "))
 
 if len(tweet_payload['entities']['urls']) > 0:
  for link in tweet_payload['entities']['urls']:
   tweet_link = ur"{}".format(link['url'])
   payload_text = payload_text.replace(tweet_link,"")
 feature_properties['tweet_text'] = payload_text
 
 tweet_geom = tweet_payload['coordinates']
 if tweet_geom is None: # Redundant check
  tweet_geom = tweet_payload['place']['bounding_box']
  
 json_feature =  geojson.Feature(id = str(recCount), geometry = tweet_geom, properties = feature_properties)
 feature_list.append(json_feature)

feature_collection = geojson.FeatureCollection(feature_list)
with open('dumpLast24.geojson', 'w') as outfile:
  geojson.dump(feature_collection, outfile)