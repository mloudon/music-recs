import logging
from time import sleep

import redis
import requests
from requests.exceptions import HTTPError, ConnectionError

from settings import api_key


base_url = 'http://ws.audioscrobbler.com/2.0/'
artist_tag_store = redis.StrictRedis('localhost', 6379, db=0, decode_responses=True) # decode_responses returns keys in the default encoding
tag_sim_store = redis.StrictRedis('localhost', 6379, db=1, decode_responses=True)
artist_sim_store = redis.StrictRedis('localhost', 6379, db=2, decode_responses=True)
max_retries = 5

def get_artists(count = 1000):
    try:
        r = requests.get(base_url,params={'method':'chart.gettopartists','api_key':api_key,'format':'json','limit':count})
        r.raise_for_status()
        
        artists_dict = r.json()
        return artists_dict
    
    except (ConnectionError,HTTPError) as e:
        logging.error(e)
    
    return

def get_top_tags(artist):
    try:
        r = requests.get(base_url,params={'method':'artist.gettoptags','artist':artist,'api_key':api_key,'format':'json'})
        r.raise_for_status()
        
        tags_dict = r.json()
        return tags_dict
    
    except (ConnectionError,HTTPError) as e:
        logging.error(e)
    
    return

def lastfm_fetch ():
    artists = get_artists(30)
    artist_names = [a['name'] for a in artists['artists']['artist']]
    save_tags(artist_names)
    
    retries = 0
    incomplete = True
    
    while retries <= max_retries and incomplete:
        saved_artists = [k for k in artist_tag_store.scan_iter()]
        missing_artists = list(set(artist_names) - set(saved_artists))
        
        if not missing_artists:
            incomplete = False
            logging.info('No missing artists.')
        
        elif retries < max_retries:
            logging.warn('Missing tag data for artists: %s' % ','.join(missing_artists))
            save_tags(missing_artists)
            
        retries = retries + 1
            
    if incomplete:
        logging.error('failed to get data for all artists')
        

def save_tags(artist_names):
    if not artist_names:
        logging.error('no artist data')
        return
    
    for artist in artist_names:
        tags = get_top_tags(artist)

        if not tags:
            logging.warn('no tag data for artist %s' % artist)
        else:
            try:
                tag_list = [t['name'] for t in tags['toptags']['tag']]
                print ('artist: %s, tags: %s' % (artist,','.join(tag_list)))
                artist_tag_store.rpush(artist, *tag_list)
            except (KeyError, TypeError) as e:
                logging.error('No data saved for artist %s' % artist)
                logging.error(e)
                logging.error(tags)
        sleep(1)
        
def artists_tags_print():
    for key in artist_tag_store.scan_iter():
        print ('artist: %s, tags: %s' % (key,','.join(artist_tag_store.lrange(key, 0, -1))))
        
def clear_r_stores():
    for r_store in [artist_tag_store,tag_sim_store,artist_sim_store]:
        r_store.flushdb()
    
def get_co_matrix():
    return
    
def get_co (co_matrix, tag1, tag2):
    return
    
        
def main():
    return

if __name__ == "__main__": main()
        