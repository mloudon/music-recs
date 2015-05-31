from extract import lastfm_fetch
from network_calcs import ARTIST_MODE, TAG_MODE, get_jaccard_sims, output_sims
from settings import artist_tag_store, \
    tag_sim_store, artist_sim_store


def clear_r_stores():
    for r_store in [artist_tag_store,tag_sim_store,artist_sim_store]:
        r_store.flushdb()

          
def main():
    clear_r_stores()
    lastfm_fetch(30)
    
    for mode in [ARTIST_MODE,TAG_MODE]:
        get_jaccard_sims(mode)
        output_sims(mode)

if __name__ == "__main__": main()
        