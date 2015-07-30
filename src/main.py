import csv
import logging
import os

from extract import lastfm_fetch, output_artist_tags
from network_calcs import ARTIST_MODE, TAG_MODE, output_sims, \
    get_artists_tags_graph, get_top_n
from settings import output_path

import networkx as nx


logging.getLogger().setLevel(logging.INFO)

def output_similar_artists(tag, n):
    '''
    write all artists tagged with tag, and their top n similar artists, to a csv file
    :param tag: the tag name
    :param n: number of similar artists to include
    '''
    g = get_artists_tags_graph()
    artists_for_tag = g.neighbors(('tag',tag))
    with open(os.path.join(output_path,'similar_artists.csv'),'wb') as outfile:
        w = csv.writer(outfile)
        for artist in artists_for_tag:
            top_n = [artist[1]] + [key for key in get_top_n(g,artist,n)]
            logging.info('top similar artists for %s: %s' % (top_n[0],','.join(top_n[1:])))
            w.writerow([s.encode('utf-8') for s in top_n])
            
def output_similar_artists_for_artist(artist,n):
    g = get_artists_tags_graph()
    
    with open(os.path.join(output_path,'artist_net.csv'),'wb') as outfile:
        w = csv.writer(outfile)
        my_neighbours = get_top_n(g,('artist',artist), n)
        for k in my_neighbours.keys():
            their_neighbours = get_top_n(g,('artist',k), n)
            their_neighbours = [k] + [key for key in their_neighbours]
            logging.info('top similar artists for %s: %s' % (their_neighbours[0],','.join(their_neighbours[1:])))
            w.writerow([s.encode('utf-8') for s in their_neighbours])
        
        my_neighbours = [artist] + [key for key in my_neighbours]
        logging.info('top similar artists for %s: %s' % (my_neighbours[0],','.join(my_neighbours[1:])))
        w.writerow([s.encode('utf-8') for s in my_neighbours])
        


def main():
    
    logging.info('Fetching Last.fm data...')
    lastfm_fetch(1000)
    output_artist_tags()
    
    for mode in [ARTIST_MODE, TAG_MODE]:
        logging.info('Getting similarity for mode %d' % mode)
        output_sims(mode)
        
    logging.info('Done')

if __name__ == "__main__": main()
        
