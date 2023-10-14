
import random

def generate_data(num_tables, num_cols, col_values, seed):
    random.seed(seed)
    data = {}
    for i in range(num_tables):
        cols = []
        for j in range(num_cols):
            col_value = random.choice(col_values)
            col_count = random.randint(1, 10)
            cols.append((col_value, col_count))
        data[i] = cols
    return data

# Returns (timeline, eventname, start, end)
def generate_event(timeline, seed, events):
    random.seed(seed)
    eventname = random.choice(events)
    start = random.randint(0, 10)
    end = random.randint(start+1, 20)
    return (timeline, eventname, (start, end))