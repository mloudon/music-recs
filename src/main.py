import logging

from extract import lastfm_fetch, output_artist_tags
from network_calcs import ARTIST_MODE, TAG_MODE, get_jaccard_sims, output_sims
from settings import artist_tag_store, \
    tag_sim_store, artist_sim_store

logging.getLogger().setLevel(logging.INFO)


def clear_r_stores():
    '''
    delete all artist, tag and similarity data
    '''
    for r_store in [artist_tag_store, tag_sim_store, artist_sim_store]:
        r_store.flushdb()
          
def main():
    
    clear_r_stores()
    
    logging.info('Fetching Last.fm data...')
    lastfm_fetch(30)
    output_artist_tags()
    
    for mode in [ARTIST_MODE, TAG_MODE]:
        logging.info('Getting similarity for mode %d' % mode)
        get_jaccard_sims(mode)
        logging.info('Writing similarity csv files for mode %d' % mode)
        output_sims(mode)
        
    logging.info('Done')

if __name__ == "__main__": main()
        
