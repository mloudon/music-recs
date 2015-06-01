import csv
from itertools import combinations
import json
import logging
from math import factorial

from networkx.algorithms.link_prediction import jaccard_coefficient

import networkx as nx
from settings import artist_tag_store, artist_sim_filename, tag_sim_filename


# mode constants for the artist and tag node sets in the bipartite artist-tag network - see networkx bipartite docs
ARTIST_MODE = 0
TAG_MODE = 1

def get_artists_tags_graph():
    '''
    read artist tag data from artist tag store
    returns a networkx bipartite graph with an edge between each artist and the top tags applied to them
    '''
    g = nx.Graph()
    for artist in artist_tag_store.scan_iter():
        g.add_node(('artist', artist), bipartite=ARTIST_MODE)
        for tag in artist_tag_store.lrange(artist, 0, -1):
            if not ('tag', tag) in g:
                g.add_node(('tag', tag), bipartite=TAG_MODE)
            g.add_edge(('artist', artist), ('tag', tag))
    logging.info('artists-tags graph has %d nodes and %d edges' % (g.number_of_nodes(), g.number_of_edges()))
    return g

def jaccard_sims(g, bipartite_mode, pairs):

    if bipartite_mode not in [ARTIST_MODE, TAG_MODE]:
        logging.error('invalid value for bipartite mode: %d' % bipartite_mode)
        return
    
    counter = 0
    for (a1, a2) in pairs:
        sim_iter = jaccard_coefficient(g, [(a1, a2)])
        (u, v, sim) = sim_iter.next()
        
        counter = counter + 1
        if (counter % 10000 == 0):
            logging.info('Calculating similarity for pair %d, mode %d' % (counter, bipartite_mode))
        
        yield(u[1], v[1], sim)
            

def output_sims(bipartite_mode):
    '''
    save pairwise similarity for all nodes in a particular mode to csv file specified in settings
    file format artist1,artist2,sim (or tag1,tag2,sim)
    :param bipartite_mode: which set of nodes to calculate similarity for: ARTIST_MODE or TAG_MODE
    '''
    if bipartite_mode not in [ARTIST_MODE, TAG_MODE]:
        logging.error('invalid value for bipartite mode')
        return
    f = artist_sim_filename if bipartite_mode == ARTIST_MODE else tag_sim_filename
    
    g = get_artists_tags_graph()
    n_set = set(n for n, d in g.nodes(data=True) if d['bipartite'] == bipartite_mode)
    pairs = combinations(n_set, 2) # all pairs of nodes, unordered i.e. nodeset choose 2 
    sims = jaccard_sims(g, bipartite_mode, pairs)
    
    logging.info('calculating similarity for unique unorderd pairs of %d nodes' % len(n_set))
    counter = 0
    
    with open(f, 'wb') as csvfile:
        w = csv.writer(csvfile)
        for (tag1, tag2, sim) in sims:
            row = [tag1, tag2]
            row.append('%.8f' % sim)
            row = ([s.encode('utf-8') for s in row])
            logging.debug(row)
            w.writerow (row)
            
            counter = counter + 1
            if (counter % 100 == 0):
                logging.info('Writing similarity data for pair %d, mode %d' % (counter, bipartite_mode))
