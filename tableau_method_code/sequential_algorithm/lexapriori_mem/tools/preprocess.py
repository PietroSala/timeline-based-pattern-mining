from . import helper as utils

def data_to_intervals(data):

    parsed_data = {}

    # Iterate through each timeline
    for timeline, events in data.items():

        parsed_data[timeline] = []

        # Keep track of time index for each timeline
        last_end = 0

        # Iterate through each event
        for event in events:
            # Ignore empty events
            if event[0] is not None:
                begin = last_end
                end = last_end + event[1]
                parsed_data[timeline].append((event[0].replace(' ', '-').replace("'", ''), begin, end))

            
            # Add duration to time index
            last_end += event[1]

    return parsed_data

def dict_to_list(data_dict):
    return [(k, v) if v else (k, []) for k, v in data_dict.items()]


def replace_current_event(current_events, parsed_data, i):

    '''
    if len(parsed_data[i]) == 0:
        current_events[i] = ()
        return
    '''
    # Update next event. If there is no next event, set null value (empty tuple)
    if len(parsed_data[i][1]) != 0:
        # Extract next event
        current_events[i] = parsed_data[i][1].pop(0)
    else:
        # Set null event
        current_events[i] = ()

def intervals_to_words(data_intervals):
    
    # Extract next events
    current_events = [()]*len(data_intervals)
    [replace_current_event(current_events, data_intervals, i) for i in range(len(data_intervals))]    
    # Keep track of which events are active (default none)
    timelines_status = {data_intervals[i][0]: '_' for i in range(len(data_intervals))}

    # Check if there are no events
    if current_events == [()]*len(data_intervals):
        return []
    
    # Prepare for the analysis starting from negative T and no instants
    lexical_representation = []
    next_t = -1

    while not all(current_events[i] == () for i in range(len(current_events))):
        
        # Define current instant as empty, editing will be put in place by each timeline
        instant_representation = ['_']*len(data_intervals)

        
        # Get next interesting instant
        next_t = min([j for j in [i[1] for i in current_events if i != ()] + [i[2] for i in current_events if i != ()] if j > next_t])

        # Cycles through each timeline
        for i in range(len(data_intervals)):

            # There is no event, '_' is fine
            if current_events[i] == ():
                continue
            
            # Check if this time instant is a start event
            elif next_t == current_events[i][1]:
                # Place S_x
                instant_representation[i] = f'S_{current_events[i][0]}'
                # Remember that event is active
                timelines_status[data_intervals[i][0]] = current_events[i][0]

                # Check if skip E_ event
                # If the end of this event is the same as the start of the next event
                # Skip this event to replace this E_x with the next S_x
                if len(data_intervals[i][1]) != 0 and current_events[i][2] == data_intervals[i][1][0][1]:
                    replace_current_event(current_events, data_intervals, i)
            
            # Check if this time instant is a start event
            elif next_t == current_events[i][2]:
                # Place E_x
                instant_representation[i] = f'E_{current_events[i][0]}'
                # Remember that event is not active
                timelines_status[data_intervals[i][0]] = '_'
                # Update current event
                replace_current_event(current_events, data_intervals, i)

            # Check if this time instant does not involve this timeline
            else:
                # If timeline has an active event, place I_x
                if timelines_status[data_intervals[i][0]] != '_':
                    instant_representation[i] = f'I_{timelines_status[data_intervals[i][0]]}'
                    
        # Add instant to representation
        lexical_representation.append(instant_representation)
    return lexical_representation

def data_to_words(data):

    # Turn data into intervals (could possibily be skipped)
    data_intervals = dict_to_list(data_to_intervals(data))
    
    return intervals_to_words(data_intervals)
    

def import_xes(path = '', file = ''):
    import pm4py
    log = pm4py.read_xes(path+'/'+file)
    return pm4py.convert_to_event_log(log)
    


def import_fluxicon(path = '', file = 'PurchasingExample.xes'):
    import networkx as nx

    if path == '':
        path = 'Datasets/Fluxicon/'
    else:
        path += 'Datasets/Fluxicon/'

    # Import XES file
    log = import_xes(path, file)

    # Convert to dictionary
    events = {}
    start_time = min([log[trace][0]['time:timestamp'] for trace in range(len(log))])
    for trace in range(len(log)):
        events[trace] = []
        events[trace].append((None, (log[trace][0]['time:timestamp'] - start_time).total_seconds()//60))
        event_index = 0
        while event_index < len(log[trace])-1:
            if log[trace][event_index]['concept:name'] == log[trace][event_index+1]['concept:name']:
                events[trace].append((log[trace][event_index]['concept:name'], (log[trace][event_index+1]['time:timestamp'] - log[trace][event_index]['time:timestamp']).total_seconds()//60))
                event_index += 1
                events[trace].append((None, (log[trace][event_index+1]['time:timestamp'] - log[trace][event_index]['time:timestamp']).total_seconds()//60))
                event_index += 1
            else:
                events[trace].append((log[trace][event_index]['concept:name'], 1))
                events[trace].append((None, (log[trace][event_index+1]['time:timestamp'] - log[trace][event_index]['time:timestamp']).total_seconds()//60))
                event_index += 1

    events = data_to_intervals(events)
    # Calculate how many timelines are needed
    event_names = list(set([event[0] for trace in events.values() for event in trace]))
    
    # Distribute all events to each timeline
    partitioned_events = []
    for subject in events:
        partition = {}
        for event in range(len(event_names)):
            partition[event] = []

        for event in range(len(events[subject])):
            partition[event_names.index(events[subject][event][0])].append(events[subject][event])
        partitioned_events.append(partition)

    conflicts_graph = utils.register_conflicts(partitioned_events)
    mergeability_graph = nx.complement(conflicts_graph)
    partitions = utils.partition_conflicts(mergeability_graph)
    
    merged_subjects = utils.merge_timelines(partitioned_events, partitions)

    return merged_subjects

            
            