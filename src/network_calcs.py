from itertools import combinations

import networkx as nx
from networkx.algorithms.link_prediction import jaccard_coefficient

from settings import artist_tag_store

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

def get_jaccard_sims(bipartite_mode):
    g = get_artists_tags_graph()
    n_set = set(n for n,d in g.nodes(data=True) if d['bipartite']==bipartite_mode)
    pairs = combinations(n_set,2)
    
    for (a1,a2) in pairs:
        sim_iter = jaccard_coefficient(g,[(a1,a2)])
        (u,v,sim) = sim_iter.next()
        print('%s:%s = %.8f'%(u[1],v[1],sim))
    return