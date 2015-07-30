import os
import redis

# lastfm API key
api_key = '2e52102c91581798789358b40038a80c'
# API base url
base_url = 'http://ws.audioscrobbler.com/2.0/'
# max retries for missing artist/tag data
max_retries = 5

ARTIST_PREFIX = 'artist-'
TAG_SIM_PREFIX = 'tagsim-'

artist_tag_store = redis.StrictRedis('localhost', 6379, db=0, decode_responses=True) # decode_responses returns keys in the default encoding

# output file names
output_path = '/home/mel/Desktop'
artist_tag_filename = os.path.join(output_path,'artist_tags.csv')
tag_sim_filename = os.path.join(output_path,'tag_similarity.csv')
artist_sim_filename =  os.path.join(output_path,'artist_similarity.csv')