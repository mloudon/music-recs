import logging

import requests
from requests.exceptions import HTTPError, ConnectionError

from settings import api_key


base_url = 'http://ws.audioscrobbler.com/2.0/'

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

def main():
    artists = get_artists(10)
    
    if not artists:
        logging.error('no artist data')
        return
    
    for artist in artists['artists']['artist']:
        a_name = artist['name']
        tags = get_top_tags(a_name)

        if not tags:
            logging.error('no tag data for artist %s' % a_name)
            return
        
        print ('artist: %s, tags: %s' % (a_name,','.join([t['name'] for t in tags['toptags']['tag']])))
        
if __name__ == "__main__": main()
        