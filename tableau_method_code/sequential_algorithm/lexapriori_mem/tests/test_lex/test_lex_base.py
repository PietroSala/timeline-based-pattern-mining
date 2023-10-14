from lexapriori_mem.lex.lex_mem import memLexRepr
from lexapriori_mem.lex.lex_base import baseLexRepr
from lexapriori_mem.tools.random_data_generator import generate_data, generate_event
from lexapriori_mem import preprocess as preprocess
from lexapriori_mem.tools import helper as utils
from lexapriori_mem.lib.event import eventClass

import pytest
import re
import copy


tables = 3
rows = 2
events = ['a', 'b', 'c']
seed = 40

def generate_test_data(seed = seed, n_tables=tables, n_rows=rows, events=events):
    return preprocess.data_to_words(generate_data(n_tables, n_rows, events, seed))


# Check for baseLexRepr class creation check successes
@pytest.mark.parametrize("data", (generate_test_data(i) for i in range(10)))
def test_baseLexRepr_initialization(data):
    b = baseLexRepr(data)
    assert b.data == data


malformed_data = [generate_test_data() for _ in range(5)]

malformed_data[0][0][0] = 'a'
malformed_data[1][0][0] = ''
del malformed_data[2][0][1]
del malformed_data[3][0]
malformed_data[4][3][1] = '_'

# Check for baseLexRepr class creation check failures
@pytest.mark.parametrize("data", malformed_data)
def test_baseLexRepr_initialization(data):
    with pytest.raises(ValueError):
        baseLexRepr(data)

# Check single event creation method
@pytest.mark.parametrize("data", [
    eventClass(1, 'a', (2, 4)),
    eventClass(2, 'b', (3, 5)),
    eventClass(3, 'c', (4, 6)),
    eventClass(4, 'a', (5, 7)),
])
def test_baseLexRepr_single_event_creation(data):
    b = baseLexRepr.from_event(data, 10)
    assert len(b) == 2, f"Generated lexical representation should have 2 instants, but it has {len(b)}"
    assert len(b[0]) == 10, f"Generated lexical representation should have 10 timelines, but it has {len(b[0])}"
    assert b[0][data.timeline] == 'S_' + data.event, f"Generated lexical representation should have event S_{data.event} in instant 0, but it has {b[0][data.timeline]}"
    assert b[1][data.timeline] == 'E_' + data.event, f"Generated lexical representation should have event E_{data.event} in instant 1, but it has {b[1][data.timeline]}"




# Test as_searchable_string method
@pytest.mark.parametrize("data, correct", [
    (generate_test_data(0),
     '[S_b,S_c,S_b][I_b,I_c,S_c][S_a,I_c,I_c][I_a,S_b,I_c][I_a,I_b,E_c][E_a,I_b,_][_,E_b,_]'),
    (generate_test_data(5),
     '[S_c,S_c,S_a][I_c,I_c,S_a][I_c,I_c,E_a][S_c,I_c,_][I_c,S_a,_][E_c,I_a,_][_,E_a,_]'),
    (generate_test_data(10),
     '[S_c,S_c,S_b][S_b,S_a,I_b][I_b,I_a,S_c][I_b,I_a,E_c][E_b,E_a,_]'),
    (generate_test_data(15),
     '[S_a,S_a,S_c][S_c,I_a,I_c][E_c,I_a,I_c][_,I_a,S_c][_,S_a,I_c][_,E_a,I_c][_,_,E_c]'),
    (generate_test_data(100), 
     '[S_a,S_c,S_b][I_a,S_c,I_b][S_b,I_c,I_b][I_b,I_c,S_a][E_b,I_c,I_a][_,E_c,I_a][_,_,E_a]'),
    (generate_test_data(1000), 
     '[S_b,S_a,S_b][S_b,I_a,I_b][I_b,I_a,S_a][I_b,I_a,E_a][E_b,S_a,_][_,E_a,_]'),
])
def test_baseLexRepr_as_searchable_string(data, correct):
    b = baseLexRepr(data)
    assert b.as_searchable_string == correct

# Check as_regex method
@pytest.mark.parametrize("data", [
    generate_test_data(0),
    generate_test_data(5),
    generate_test_data(10),
    generate_test_data(15),
    generate_test_data(100),
    generate_test_data(1000),
])
def test_baseLexRepr_as_regex(data):
    b = baseLexRepr(data)
    assert bool(re.match(b.as_regex, b.as_searchable_string)), "Regex does not match searchable string"

# Check events method
@pytest.mark.parametrize("data", [
    generate_test_data(0),
    generate_test_data(5),
    generate_test_data(10),
    generate_test_data(15),
    generate_test_data(100),
    generate_test_data(1000),
])
def test_baseLexRepr_events(data):
    b = baseLexRepr(data)
    assert all([isinstance(event, eventClass) for event in b.events_list])
    assert all([event.event in events for event in b.events_list])
    assert len(b.events_list) == tables * rows


# Check timelines compatibility method
@pytest.mark.parametrize("data1, data2", [
    (generate_test_data(0), generate_test_data(5, 3, 4)),
    (generate_test_data(10, 4, 3), generate_test_data(15, 4, 2)),
    (generate_test_data(100, 12, 3), generate_test_data(1000, 12, 2)),
])
def test_baseLexRepr_timelines_compatibility(data1, data2):
    b1 = baseLexRepr(data1)
    b2 = baseLexRepr(data2)
    assert b2.check_compatibility(b1), f"Timelines are compatible, but the method returned False.\ndata1: {data1}\ndata2:{data2}"
    assert b1.check_compatibility(b2), f"Timelines are compatible, but the method returned False.\ndata1: {data1}\ndata2:{data2}"

@pytest.mark.parametrize("data1, data2", [
    (generate_test_data(0), generate_test_data(5, 2, 4)),
    (generate_test_data(10, 4, 3), generate_test_data(15, 5, 2)),
    (generate_test_data(100, 12, 3), generate_test_data(1000, 10, 2)),
])
def test_baseLexRepr_timelines_compatibility(data1, data2):
    b1 = baseLexRepr(data1)
    b2 = baseLexRepr(data2)
    assert not b2.check_compatibility(b1), f"Timelines are compatible, but the method returned False.\ndata1: {data1}\ndata2:{data2}"
    assert not b1.check_compatibility(b2), f"Timelines are compatible, but the method returned False.\ndata1: {data1}\ndata2:{data2}"

# Check len method
@pytest.mark.parametrize("data", [
    generate_test_data(0),
    generate_test_data(5),
    generate_test_data(10),
    generate_test_data(15),
    generate_test_data(100),
    generate_test_data(1000),
])
def test_baseLexRepr_len(data):
    b = baseLexRepr(data)
    assert len(b) == len(data)

# Check size method
@pytest.mark.parametrize("data", [
    generate_test_data(0),
    generate_test_data(5),
    generate_test_data(10),
    generate_test_data(15),
    generate_test_data(100),
    generate_test_data(1000),
])
def test_baseLexRepr_size(data):
    b = baseLexRepr(data)
    assert b.size == tables * rows


# check delete_event method
@pytest.mark.parametrize("data", [
    generate_test_data(0),
    generate_test_data(5),
    generate_test_data(10),
    generate_test_data(15),
    generate_test_data(100),
    generate_test_data(1000),
])
def test_baseLexRepr_delete_event(data):
    b = baseLexRepr(data)
    for event in b.events_list:
        c = b.delete_event(event)
        assert event not in c.events_list
        assert c.size == b.size - 1

test_baseLexRepr_delete_event(generate_test_data(0))

@pytest.mark.parametrize("data", [
    generate_test_data(0, 1, 1),
    generate_test_data(5, 1, 1),
    generate_test_data(10, 1, 1),
    generate_test_data(15, 1, 1),
    generate_test_data(100, 1, 1),
    generate_test_data(1000, 1, 1),
])
def test_baseLexRepr_clean_text(data):
    b = baseLexRepr(data)
    with pytest.raises(ValueError):
        for event in b.events_list:
            b = b.delete_event(event)

# Test clean_text
@pytest.mark.parametrize("data", [
    generate_test_data(0),
    generate_test_data(5),
    generate_test_data(10),
    generate_test_data(15),
    generate_test_data(100),
    generate_test_data(1000),
])
def test_baseLexRepr_clean_text(data):
    b = baseLexRepr(data)

    assert b.data == data, f"Data should not be changed here. {b.data} != {data}"
    b.del_null()
    assert b.data == data, f"Null values deletion should not affect the data here. {b.data} != {data}"

    for _ in range(10):
        b.data.insert(0, [''*tables]*rows)
        b.data.insert(len(b)//2, [''*tables]*rows)
    b.del_null()
    assert b.data == data, f"Null values deletion should return to original state. {b.data} != {data}"
    

# Test gen_null
@pytest.mark.parametrize("data, insertion0, insertion_half, insertion_end", [
    (generate_test_data(0), ['_', '_', '_'], ['I_a', 'I_c', 'I_c'], ['_', '_', '_']),
    (generate_test_data(5), ['_', '_', '_'], ['I_c', 'I_c', '_'], ['_', '_', '_']),
    (generate_test_data(10), ['_', '_', '_'], ['I_b', 'I_a', 'I_b'], ['_', '_', '_'])
])
def test_baseLexRepr_gen_null(data, insertion0, insertion_half, insertion_end):
    b = baseLexRepr(data)

    null_event = b.gen_null(0)
    assert null_event == insertion0, f"Null event should be {insertion0}, but it is {null_event}"

    null_event = b.gen_null(len(b)//2)
    assert null_event == insertion_half, f"Null event should be {insertion_half}, but it is {null_event}"

    null_event = b.gen_null(len(b))
    assert null_event == insertion_end, f"Null event should be {insertion_end}, but it is {null_event}"

# Test __contains__ per event e lex_repr
@pytest.mark.parametrize("data", [
    generate_test_data(0),
    generate_test_data(5),
    generate_test_data(10),
    generate_test_data(15),
    generate_test_data(100),
    generate_test_data(1000),
])
def test_baseLexRepr_contains(data):
    b = baseLexRepr(data)
    for event in b.events_list:
        assert event in b, f"Event {event} should be in {b}"
    assert b in b, f"Lex_repr {b} should be in itself"
