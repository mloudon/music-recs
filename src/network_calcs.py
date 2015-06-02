import csv
import logging
import operator

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
    '''
    return a generator that yields tuples of form (label1,label2,sim) for all similarities in the given node pairs
    :param g: the artists tags graph
    :param bipartite_mode: which set of nodes to calculate similarity for: ARTIST_MODE or TAG_MODE
    :param pairs: tuple of pairs of artist or tag nodes (an ebunch in networkx)
    '''

    if bipartite_mode not in [ARTIST_MODE, TAG_MODE]:
        logging.error('invalid value for bipartite mode: %d' % bipartite_mode)
        return
    
    for counter,(a1, a2) in enumerate(pairs):
        sim_iter = jaccard_coefficient(g, [(a1, a2)])
        (u, v, sim) = sim_iter.next()
        yield(u[1], v[1], sim)
            
        if (counter % 10000 == 0):
            logging.info('Calculated similarity for pair %d, mode %d' % (counter, bipartite_mode))
            
def get_top_n(g, node, n=5):
    '''
    get the top n most similar nodes for a given node
    returns a dict of at most n node:similiarity pairs
    :param g: the artists tags graph
    :param node: the node ('artist',name) or ('tag',name)
    :param n: the maximum number of similar tags to be returned
    '''
    if not node in g:
        logging.error('Node not in graph: %s' % node[1])
        return
    mode = g.node[node]['bipartite']
    
    n_set = set(n for n, d in g.nodes(data=True) if d['bipartite'] == mode)
    g_proj = nx.projected_graph(g,n_set)
    sims = {v:sim for (u, v, sim) in jaccard_sims(g, mode, g_proj.edges_iter(node))}
    max_len = n if len(sims) >= n else len(sims)
    return dict(sorted(sims.iteritems(), key=operator.itemgetter(1), reverse=True)[:max_len])
        

def output_sims(bipartite_mode):
    '''
    write non-zero jaccard similarity for all nodes in a particular mode to csv file specified in settings
    file format artist1,artist2,sim (or tag1,tag2,sim)
    :param bipartite_mode: which set of nodes to calculate similarity for: ARTIST_MODE or TAG_MODE
    '''
    if bipartite_mode not in [ARTIST_MODE, TAG_MODE]:
        logging.error('invalid value for bipartite mode')
        return
    f = artist_sim_filename if bipartite_mode == ARTIST_MODE else tag_sim_filename
    
    g = get_artists_tags_graph()
    n_set = set(n for n, d in g.nodes(data=True) if d['bipartite'] == bipartite_mode)
    g_proj = nx.projected_graph(g,n_set)
    sims = jaccard_sims(g, bipartite_mode, g_proj.edges_iter())  # all edges
    logging.info('calculating similarity for %d unique unorderd pairs' % g_proj.number_of_edges())
    
    with open(f, 'wb') as csvfile:
        w = csv.writer(csvfile)
        for counter,(tag1, tag2, sim) in enumerate(sims):
            row = [tag1, tag2]
            row = ([s.encode('utf-8') for s in row])
            row.append(sim)
            logging.debug(row)
            w.writerow (row)

            if (counter % 10000 == 0):
                logging.info('Wrote non-zero similarity data for pair %d, mode %d' % (counter, bipartite_mode))
