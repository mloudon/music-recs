import csv
import logging

from settings import artist_tag_store, \
    tag_sim_store, artist_sim_store

def write_artists_tags(f):
    with open(f, 'wb') as csvfile:
        w = csv.writer(csvfile)
        w.writerow('artist','tags')
        for key in artist_tag_store.scan_iter():
            w.writerow (key,';'.join(artist_tag_store.lrange(key, 0, -1)))
        
def clear_r_stores():
    for r_store in [artist_tag_store,tag_sim_store,artist_sim_store]:
        r_store.flushdb()

          
def main():
    return

if __name__ == "__main__": main()
        