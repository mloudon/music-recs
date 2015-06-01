import logging

from extract import lastfm_fetch, output_artist_tags
from network_calcs import ARTIST_MODE, TAG_MODE, output_sims

logging.getLogger().setLevel(logging.INFO)


def main():
    
    logging.info('Fetching Last.fm data...')
    lastfm_fetch(30)
    output_artist_tags()
    
    for mode in [ARTIST_MODE, TAG_MODE]:
        logging.info('Getting similarity for mode %d' % mode)
        output_sims(mode)
        
    logging.info('Done')

if __name__ == "__main__": main()
        
