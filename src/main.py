import logging
from time import sleep

import redis
import requests
from requests.exceptions import HTTPError, ConnectionError

from settings import api_key


base_url = 'http://ws.audioscrobbler.com/2.0/'
r_store = redis.StrictRedis('localhost', 6379, decode_responses=True) # decode_responses returns keys in the default encoding
max_retries = 3

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

def create_data_store ():
    clear_r_store()
    artists = get_artists(1000)
    artist_names = [a['name'] for a in artists['artists']['artist']]
    save_data(artist_names)
    
    retries = 0
    incomplete = True
    while retries <= max_retries and incomplete:
        saved_artists = [k for k in r_store.scan_iter()]
        missing_artists = list(set(artist_names) - set(saved_artists))
        
        if not missing_artists:
            incomplete = False
        
        elif retries < max_retries:
            logging.warn('Missing tag data for artists: %s' % ','.join(missing_artists))
            save_data(missing_artists)
            retries = retries + 1
            
    if incomplete:
        logging.error('failed to get data for all artists')
        

def save_data(artist_names):
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
                r_store.rpush(artist, *tag_list)
            except KeyError, e:
                logging.error('No data saved for artist %s' % artist)
                logging.error(e)
        sleep(1)
        
def print_r_store():
    for key in r_store.scan_iter():
        print ('artist: %s, tags: %s' % (key,','.join(r_store.lrange(key, 0, -1))))
        
def clear_r_store():
    r_store.flushdb()
    
def get_co_matrix():
    return
    
def get_co (co_matrix, tag1, tag2):
    return
    
        
def main():
    return

if __name__ == "__main__": main()
        