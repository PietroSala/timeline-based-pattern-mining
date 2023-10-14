from lexapriori_mem.lexical_apriori.lexApriori import apriori
from lexapriori_mem.lex.lex_mem import memLexRepr
from lexapriori_mem.tools.random_data_generator import generate_data, generate_event
from lexapriori_mem.tools import preprocess as preprocess
import pytest

import os

tables = 3
rows = 2
events = ['a', 'b', 'c']
seed = 40

def generate_test_data(seed = seed, n_tables=tables, n_rows=rows, events=events):
    return preprocess.data_to_words(generate_data(n_tables, n_rows, events, seed))

def generate_test_event(timeline, seed = seed, events = events):
    return generate_event(timeline, seed, events)


sample_dataset = [memLexRepr([['S_a'], ['S_b'], ['S_c'], ['E_c']]),
                  memLexRepr([['S_a'], ['S_b'], ['S_c'], ['E_c']]),
                  memLexRepr([['S_a'], ['S_b'], ['S_c'], ['E_c']])]

sample_result = {1: [memLexRepr([['S_a'], ['E_a']]), 
                     memLexRepr([['S_b'], ['E_b']]), 
                     memLexRepr([['S_c'], ['E_c']])], 
                 2: [memLexRepr([['S_a'], ['S_b'], ['E_b']]), 
                     memLexRepr([['S_a'], ['E_a'], ['S_c'], ['E_c']]), 
                     memLexRepr([['S_b'], ['S_c'], ['E_c']]),], 
                 3: [memLexRepr([['S_a'], ['S_b'], ['S_c'], ['E_c']])]}

sample_cutout = []
for itemset in [j for i in sample_result for j in sample_result[i]]:
    eventlist = {}
    for timeline in range(len(itemset[0])):
        eventlist[timeline] = []
    for event in itemset.events_list:
        eventlist[event.timeline].append(
            (event.event, event.start, event.end))
    sample_cutout.append(eventlist)

# Check for apriori class creation check successes
@pytest.mark.parametrize("dataset, epsilon, expected", [
    (sample_dataset, 0.5, sample_result),
    (sample_dataset, 0.6, sample_result),
    (sample_dataset, 0.7, sample_result)
])
def test_apriori_initialization(dataset, epsilon, expected):
    a = apriori(dataset, epsilon)
    assert a.dataset == dataset
    assert a.epsilon == epsilon
    assert a.frequent_itemsets == {}
    assert a.candidate_next == {}
    assert a.singlets == []
    assert a.size == 0
    assert a.frequent_itemsets == {}

    result = a.apriori()
    assert set(result[1]) == set(expected[1])
    assert set(result[2]) == set(expected[2])
    assert set(result[3]) == set(expected[3])

# Test database connection and creation
def test_connection():
    filename = './test.db'

    a = apriori([], 0, filename)

    with a._database_connection() as conn:
        cur = conn.cursor()
        res = cur.execute("SELECT name FROM sqlite_master")
        assert res.fetchone() == (a.tablename,)

    conn.close()
    os.remove(filename)

def test_insertion():

    filename = './test.db'
    a = apriori([], 0, filename)

    test = memLexRepr(generate_test_data(seed, 1, 2, ['a', 'b']))
    a.insert(test, 1)

    with a._database_connection() as conn:
        cur = conn.cursor()
        res = cur.execute(f"SELECT * FROM {a.tablename}")
        assert len(res.fetchall()) == 1, f'Found more than one entry in the database, expected 1'

    conn.close()
    os.remove(filename)
    
@pytest.mark.parametrize('number_of_insertions', [2, 5, 10, 15, 20, 50, 100])
def test_multiple_insertion(number_of_insertions):

    filename = './test.db'
    a = apriori([], 0, filename)

    for _ in range(number_of_insertions):
        test = memLexRepr(generate_test_data(seed, 3, 3, ['a', 'b', 'c', 'd', 'e', 'f']))
        a.insert(test, 1)

    with a._database_connection() as conn:
        cur = conn.cursor()
        res = cur.execute(f"SELECT * FROM {a.tablename}")
        assert len(res.fetchall()) == number_of_insertions, f'Found wrong number of entries in the database, expected {number_of_insertions}'

    conn.close()
    os.remove(filename)

def test_apriori_execution():
    sample_dataset = [memLexRepr([['S_a'], ['S_b'], ['S_c'], ['E_c']]),
                  memLexRepr([['S_a'], ['S_b'], ['S_c'], ['E_c']]),
                  memLexRepr([['S_a'], ['S_b'], ['S_c'], ['E_c']])]
    epsilon = .5

    filename = './test.db'
    a = apriori(sample_dataset, epsilon, database = filename)

    result = a.apriori()    
    
    with a._database_connection() as conn:
        cur = conn.cursor()
        res = cur.execute(f"SELECT * FROM {a.tablename}")
        assert len(res.fetchall()) == sum([len(result[i]) for i in result]), f'Found wrong number of entries in the database, expected {sum([len(result[i]) for i in result])}, got {len(res.fetchall())}'

    conn.close()
    os.remove(filename)


# Test solutions cutting
@pytest.mark.parametrize('dataset, cutout', [
    (sample_dataset, sample_cutout),
])
def test_solutions_cut(dataset, cutout):
    a = apriori(dataset, 0.5, cut_solutions=cutout)
    frequent_itemsets = a.apriori()

    assert {k:v for k,v in frequent_itemsets.items() if v} == {}
