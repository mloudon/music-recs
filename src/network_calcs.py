import csv
from itertools import combinations
import json
import logging
from math import factorial

from networkx.algorithms.link_prediction import jaccard_coefficient

import networkx as nx
from settings import artist_tag_store, artist_sim_store, tag_sim_store, \
    artist_sim_filename, tag_sim_filename


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
    logging.info('artists-tags graph has %d nodes and %d edges' % (g.number_of_nodes(),g.number_of_edges()))
    return g

def pair_to_key (p1, p2):
    '''
    sort a pair of strings, dumps to json, returns json string
    used to create unique keys for pairs of artists or tags, with predictable ordering
    :param p1: first string
    :param p2: second string
    '''
    return json.dumps(sorted(list((p1, p2))))

def get_jaccard_sims(bipartite_mode):
    '''
    get the jaccard similarity for all pairs of nodes of the same mode and save to data store
    jaccard similarity calc: intersection of sets of neighbours / union of sets of neighbours for a pair of nodes
    similarity for each pair is calculated only once.
    :param bipartite_mode: which set of nodes to calculate similarity for: ARTIST_MODE or TAG_MODE
    '''
    if bipartite_mode not in [ARTIST_MODE, TAG_MODE]:
        logging.error('invalid value for bipartite mode: %d' % bipartite_mode)
        return
    
    g = get_artists_tags_graph()
    n_set = set(n for n, d in g.nodes(data=True) if d['bipartite'] == bipartite_mode)
    pairs = combinations(n_set, 2)
    
    logging.info('calculating similarity for unique unorderd pairs of %d nodes' % len(n_set))
    counter = 0
    
    for (a1, a2) in pairs:
        sim_iter = jaccard_coefficient(g, [(a1, a2)])
        (u, v, sim) = sim_iter.next()
        if (bipartite_mode == ARTIST_MODE):
            store = artist_sim_store
        elif (bipartite_mode == TAG_MODE):
            store = tag_sim_store
        store.set(pair_to_key(u[1], v[1]), sim)
        
        counter = counter +1
        if (counter % 1000 == 0):
            logging.info('Calculating similarity for pair %d' % (counter))
            
def output_sims(bipartite_mode):
    '''
    save pairwise similarity for all nodes in a particular mode to csv file specified in settings
    file format artist1,artist2,sim (or tag1,tag2,sim)
    :param bipartite_mode: which set of nodes to calculate similarity for: ARTIST_MODE or TAG_MODE
    '''
    if bipartite_mode not in [ARTIST_MODE, TAG_MODE]:
        logging.error('invalid value for bipartite mode')
        return
    
    if (bipartite_mode == ARTIST_MODE):
        store = artist_sim_store
        f = artist_sim_filename
    else:  # tag mode
        store = tag_sim_store
        f = tag_sim_filename

    store.set_response_callback('GET', float)  # cast all values returned with get to float
    
    num_pairs = store.dbsize()
    logging.info('writing csv file with similarity data for %d pairs' % num_pairs)
    counter = 0
    
    with open(f, 'wb') as csvfile:
        w = csv.writer(csvfile)
        for key in store.scan_iter():
            row = json.loads(key)
            row.append('%.8f' % store.get(key))
            row = ([s.encode('utf-8') for s in row])
            logging.debug(row)
            w.writerow (row)
            
            counter = counter +1
            if (counter % 1000 == 0):
                logging.info('Writing pair %d of %d' % (counter,num_pairs))
