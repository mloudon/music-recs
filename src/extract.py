import csv
import logging
from time import sleep

import requests
from requests.exceptions import ConnectionError, HTTPError

from settings import base_url, api_key, max_retries, artist_tag_store, \
    artist_tag_filename, ARTIST_PREFIX


def get_artists(count=1000):
    '''
    get top artists from last.fm API
    :param count: limit param in API call: how many artist records to fetch
    '''
    try:
        r = requests.get(base_url, params={'method':'chart.gettopartists', 'api_key':api_key, 'format':'json', 'limit':count})
        r.raise_for_status()
        
        artists_dict = r.json()
        return artists_dict
    
    except (ConnectionError, HTTPError) as e:
        logging.error(e)
    
    return

def get_top_tags(artist):
    '''
    get top tags for artist from last.fm API
    :param artist: the artist name
    '''
    try:
        r = requests.get(base_url, params={'method':'artist.gettoptags', 'artist':artist, 'api_key':api_key, 'format':'json'})
        r.raise_for_status()
        
        tags_dict = r.json()
        return tags_dict
    
    except (ConnectionError, HTTPError) as e:
        logging.error(e)
    
    return

def lastfm_fetch (count=30):
    '''
    fetch top artists and top tags for each artist, then save to artist_tag_store
    retry several times if some tag data is not fetched (set number of retries in settings)
    :param count: number of top artists to fetch
    '''
    artists = get_artists(count)
    artist_names = [a['name'] for a in artists['artists']['artist']]
    save_tags(artist_names)
    
    retries = 0
    incomplete = True
    
    while retries <= max_retries and incomplete:
        saved_artists = [k.lstrip(ARTIST_PREFIX) for k in artist_tag_store.scan_iter()]
        missing_artists = list(set(artist_names) - set(saved_artists))
        
        if not missing_artists:
            incomplete = False
        
        elif retries < max_retries:
            logging.warn('Missing tag data for artists: %s' % ','.join(missing_artists))
            save_tags(missing_artists)
            
        retries = retries + 1
            
    if incomplete:
        logging.error('failed to get data for all artists')
    else:
        logging.info('success! we have tag data for all artists')
        

def save_tags(artist_names):
    '''
    fetch and save top tags for a list of artists
    :param artist_names: list of artist names to fetch tags for
    '''
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
                artist_tag_store.rpush(ARTIST_PREFIX + artist, *tag_list)
                logging.info('saved tags for artist: %s, tags: %s' % (artist, ','.join(tag_list)))
            except (KeyError, TypeError) as e:
                logging.error('No data saved for artist %s' % artist)
                logging.error(e)
                logging.error(tags)
        sleep(1)  # be nice to the API
        
def output_artist_tags():
    '''
    write artists and tags to a csv file specified in settings. format is artist,tag1;tag2;tag3
    '''
    with open(artist_tag_filename, 'wb') as csvfile:
        w = csv.writer(csvfile)
        w.writerow([s.encode('utf-8') for s in ['artist', 'tags']])
        for key in artist_tag_store.scan_iter():
            row = ([s.encode('utf-8') for s in [key.lstrip(ARTIST_PREFIX), ';'.join(artist_tag_store.lrange(key, 0, -1))]])
            logging.debug(row)
            w.writerow (row)
