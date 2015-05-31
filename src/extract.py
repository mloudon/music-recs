import logging
from time import sleep

import requests
from requests.exceptions import ConnectionError, HTTPError

from settings import base_url, api_key, max_retries, artist_tag_store


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