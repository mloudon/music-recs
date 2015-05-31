import redis

# lastfm API key
api_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxx'
# API base url
base_url = 'http://ws.audioscrobbler.com/2.0/'
# max retries for missing artist/tag data
max_retries = 5

# three separate redis databases so that I can iterate over all keys, probably not necessary
artist_tag_store = redis.StrictRedis('localhost', 6379, db=0, decode_responses=True) # decode_responses returns keys in the default encoding
tag_sim_store = redis.StrictRedis('localhost', 6379, db=1, decode_responses=True)
artist_sim_store = redis.StrictRedis('localhost', 6379, db=2, decode_responses=True)

# output file names
artist_tag_filename = 'artist_tags.csv'
tag_sim_filename = 'tag_similarity.csv'
artist_sim_filename =  'artist_similarity.csv'