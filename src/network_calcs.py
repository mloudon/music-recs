import csv
from itertools import combinations
import logging
import json

from networkx.algorithms.link_prediction import jaccard_coefficient

import networkx as nx
from settings import artist_tag_store, artist_sim_store, tag_sim_store, \
    artist_sim_filename, tag_sim_filename


# mode constants for the artist and tag node sets in the bipartite artist-tag network
ARTIST_MODE = 0
TAG_MODE = 1

def get_artists_tags_graph():
    g = nx.Graph()
    for artist in artist_tag_store.scan_iter():
        g.add_node(('artist',artist), bipartite=ARTIST_MODE)
        for tag in artist_tag_store.lrange(artist, 0, -1):
            if not ('tag',tag) in g:
                g.add_node(('tag',tag), bipartite=TAG_MODE)
            g.add_edge(('artist',artist), ('tag',tag))
    return g

def pair_to_key (p1,p2):
    return json.dumps(sorted(list((p1,p2))))

def get_jaccard_sims(bipartite_mode):
    if bipartite_mode not in [ARTIST_MODE,TAG_MODE]:
        logging.error('invalid value for bipartite mode: %d' % bipartite_mode)
        return
    
    g = get_artists_tags_graph()
    n_set = set(n for n,d in g.nodes(data=True) if d['bipartite']==bipartite_mode)
    pairs = combinations(n_set,2)
    
    for (a1,a2) in pairs:
        sim_iter = jaccard_coefficient(g,[(a1,a2)])
        (u,v,sim) = sim_iter.next()
        if (bipartite_mode == ARTIST_MODE):
            store = artist_sim_store
        elif (bipartite_mode == TAG_MODE):
            store = tag_sim_store
        store.set(pair_to_key(u[1],v[1]),sim)
            
def output_sims(bipartite_mode):
    if bipartite_mode not in [ARTIST_MODE,TAG_MODE]:
        logging.error('invalid value for bipartite mode')
        return
    
    if (bipartite_mode == ARTIST_MODE):
        store = artist_sim_store
        f = artist_sim_filename
    else: #tag mode
        store = tag_sim_store
        f = tag_sim_filename

    store.set_response_callback('GET', float) # cast all values returned with get to float
    
    with open(f, 'wb') as csvfile:
        w = csv.writer(csvfile)
        for key in store.scan_iter():
            row = json.loads(key)
            row.append('%.8f' % store.get(key))
            row = ([s.encode('utf-8') for s in row])
            w.writerow (row)