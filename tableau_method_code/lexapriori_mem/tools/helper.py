import re
from typing import Any

def chunkify(lst, N):
    return [lst[i:i+N] for i in range(0, len(lst) - (len(lst) % N), N)]
     
def combine_intervals(interval1, interval2):
    if interval1[0] <= interval2[0] and interval1[1] >= interval2[1]:
        # interval2 is completely contained within interval1
        return interval1
    elif interval2[0] <= interval1[0] and interval2[1] >= interval1[1]:
        # interval1 is completely contained within interval2
        return interval2
    else:
        if interval1[1] < interval2[0] or interval2[1] < interval1[0]:
            # Intervals do not overlap, return them separately
            return [interval1, interval2]
    
        # Combine the intervals to form a single larger interval
        start = min(interval1[0], interval2[0])
        end = max(interval1[1], interval2[1])
        return [(start, end)]
    
def reduce_intervals(intervals):
    # Sort intervals based on start values
    sorted_intervals = sorted(intervals, key=lambda x: x[0])

    merged = []
    for interval in sorted_intervals:
        if not merged:
            # First interval, add it to the merged list
            merged.append(interval)
        else:
            last_merged = merged[-1]
            combined = combine_intervals(last_merged, interval)
            if isinstance(combined, list):
                # Intervals do not overlap, add the current interval separately
                merged.append(interval)
            else:
                # Update the last merged interval with the combined interval
                merged[-1] = combined
    return merged


def register_conflicts(subjects):
    import networkx as nx

    # Crea un grafo diretto
    graph = nx.Graph()

    # Aggiungi nodi al grafo per ogni timeline
    for subject in subjects:
        for timeline in subject:
            graph.add_node(timeline)

    # Verifica i conflitti tra le timeline
    for subject in subjects:
        for timeline1 in subject:
            for timeline2 in subject:
                if timeline1 != timeline2:
                    for event1 in subject[timeline1]:
                        for event2 in subject[timeline2]:
                            if event1[2] >= event2[1] and event1[1] <= event2[2]:
                                # I due eventi si sovrappongono, c'è un conflitto
                                graph.add_edge(timeline1, timeline2)

    # Restituisci il grafo dei conflitti
    return graph

def partition_conflicts(undirected_graph):
    import networkx as nx
    partitions = []
    
    while undirected_graph and nx.graph_number_of_cliques(undirected_graph) != 0:
        max_clique = next(nx.find_cliques(undirected_graph))
                
        partition = []
        for node in undirected_graph.nodes():
            if node in max_clique:
                partition.append(node)
        
        partitions.append(partition)
        undirected_graph.remove_nodes_from(partition)
    
    return partitions

def merge_timelines(subjects, partitions):
    merged_subjects = []
    for subject in subjects:
        merged_timelines = {}

        # Init merged timelines
        for idx in range(len(partitions)):
            merged_timelines[idx] = []

        for idx, partition in enumerate(partitions):
            for timeline in partition:
                merged_timelines[idx].extend(subject[timeline])
                
        for idx in merged_timelines:
            merged_timelines[idx] = sorted(merged_timelines[idx], key=lambda x: x[1])
            
        merged_subjects.append(merged_timelines)
    return merged_subjects

def merge_dicts(d1, d2):
    merged = {}
    for key in d1:
        if key in d2:
            merged[key] = list(set(d1[key] + d2[key]))
        else:
            merged[key] = d1[key]
    for key in d2:
        if key not in merged:
            merged[key] = d2[key]
    return merged
    
    
