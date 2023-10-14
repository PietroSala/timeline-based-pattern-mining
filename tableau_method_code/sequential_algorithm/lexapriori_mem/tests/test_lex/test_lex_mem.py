from lexapriori_mem.lex.lex_mem import memLexRepr
from lexapriori_mem.lex.lex_base import baseLexRepr
from lexapriori_mem.tools.random_data_generator import generate_data, generate_event
from lexapriori_mem import preprocess as preprocess
from lexapriori_mem.tools import helper as utils

from lexapriori_mem.lib import event, intervals
import pytest

tables = 3
rows = 2
events = ['a', 'b', 'c']
seed = 40

def generate_test_data(seed = seed, n_tables=tables, n_rows=rows, events=events):
    return preprocess.data_to_words(generate_data(n_tables, n_rows, events, seed))

def generate_test_event(timeline, seed = seed, events = events):
    return generate_event(timeline, seed, events)

# Check for memLexRepr class creation check successes
@pytest.mark.parametrize("data", (generate_test_data(i) for i in range(10)))
def test_memLexRepr_initialization(data):
    b = memLexRepr(data)
    assert b.data == data
    assert b.forbidden == {}
    assert b.history == []
    assert b.instants == []

@pytest.mark.parametrize("data, instants", [
    (baseLexRepr.from_event(generate_test_event(0), tables), ['1', '2']),
    (baseLexRepr.from_event(generate_test_event(1), tables), ['1', '2']),
    (baseLexRepr.from_event(generate_test_event(2), tables), ['1', '2'])
])
def test_memLexRepr_initialization(data, instants):
    b = memLexRepr(data, instants)
    assert b.data == data
    assert b.forbidden == {}
    assert b.history == []
    assert b.instants == instants

malformed_data = [generate_test_data() for _ in range(5)]

malformed_data[0][0][0] = 'a'
malformed_data[1][0][0] = ''
del malformed_data[2][0][1]
del malformed_data[3][0]
malformed_data[4][3][1] = '_'

# Check for memLexRepr class creation check failures
@pytest.mark.parametrize("data", malformed_data)
def test_memLexRepr_initialization(data):
    with pytest.raises(ValueError):
        memLexRepr(data)


# Test equality and hash
@pytest.mark.parametrize("data", (generate_test_data(i) for i in range(10)))
def test_memLexRepr_equality(data):
    b = memLexRepr(data)
    assert b == b
    assert hash(b) == hash(b)
    assert len(set([b]*200)) == 1
    assert set([b]*200) == set([b])

# Test copy
@pytest.mark.parametrize("data", (generate_test_data(i*10) for i in range(10)))
def test_memLexRepr_copy(data):
    b = memLexRepr(data)
    c = b.copy()
    assert b == c
    assert hash(b) == hash(c)
    assert len(set([b]*200)) == 1
    assert set([b]*200) == set([b])
    assert b.instants == c.instants
    assert b.forbidden == c.forbidden
    assert b.history == c.history
    assert b.data == c.data

# Test del_null
@pytest.mark.parametrize("data", [
    generate_test_data(0),
    generate_test_data(5),
    generate_test_data(10),
    generate_test_data(15),
    generate_test_data(100),
    generate_test_data(1000),
])
def test_baseLexRepr_clean_text(data):
    original_instants = '0'*len(data)
    b = memLexRepr(data, original_instants)

    assert b.data == data, f"Data should not be changed here. {b.data} != {data}"
    assert b.instants == original_instants, f"Data should not be changed here. {b.instants} != {original_instants}"
    b.del_null()
    assert b.data == data, f"Null values deletion should not affect the data here. {b.data} != {data}"
    assert b.instants == original_instants, f"Null values deletion should not affect the data here. {b.instants} != {original_instants}"

    for _ in range(10):
        b.data.insert(0, [''*tables]*rows)
        b.data.insert(len(b)//2, [''*tables]*rows)
    b.del_null()
    assert b.data == data, f"Null values deletion should return to original state. {b.data} != {data}"
    assert b.instants == original_instants, f"Null values deletion should return to original state. {b.instants} != {original_instants}"
    
# Test forbidden property management
@pytest.mark.parametrize("forbidden", [
    {'a': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]},
    {'a': [intervals.forbidden_interval(('1', '2'), ('3', '4'))], 'b': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]},
    {'a': [intervals.forbidden_interval(('1', '2'), ('3', '4')), intervals.forbidden_interval(('1', '2'), ('3', '4'))]}
])
def test_memLexRepr_forbidden(forbidden):
    b = memLexRepr(generate_test_data())
    assert b.forbidden == {}
    b.forbidden = forbidden
    assert all([set(b.forbidden[item]) == set(forbidden[item]) for item in forbidden])
    del b.forbidden
    assert b.forbidden == {}

@pytest.mark.parametrize("forbidden, result", [
    ([{'a': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]}, 
      {}
      ], 
     {'a': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]}
     ),

    ([{'a': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]}, 
      {'b': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]}
      ], 
      {'a': [intervals.forbidden_interval(('1', '2'), ('3', '4'))], 
       'b': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]}
       ),

    ([{'a': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]}, 
      {'b': [intervals.forbidden_interval(('1', '2'), ('3', '4'))], 
      'a': [intervals.forbidden_interval(('3', '5'), ('5', '6'))]}
      ], 
      {'a': [intervals.forbidden_interval(('3', '5'), ('5', '6')), 
             intervals.forbidden_interval(('1', '2'), ('3', '4'))], 
       'b': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]}
       ),

    ([{'a': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]},      
      {'a': [intervals.forbidden_interval(('3', '4'), ('4', '5'))]}
      ],
        {'a': [intervals.forbidden_interval(('3', '4'), ('4', '5')),
                intervals.forbidden_interval(('1', '2'), ('3', '4'))]}
    ),

    ([{},
       [{'a': [intervals.forbidden_interval(('1', '2'), ('3', '4'))]},      
      {'a': [intervals.forbidden_interval(('3', '4'), ('4', '5'))]}],
      ],
        {'a': [intervals.forbidden_interval(('3', '4'), ('4', '5')),
                intervals.forbidden_interval(('1', '2'), ('3', '4'))]}
    )

])
def test_memLexRepr_forbidden(forbidden, result):
    b = memLexRepr(generate_test_data())
    assert b.forbidden == {}
    b.forbidden = forbidden[0]
    assert b.forbidden == forbidden[0]
    b.forbidden = forbidden[1]
    assert all([set(b.forbidden[item]) == set(result[item]) for item in b.forbidden])
    del b.forbidden
    assert b.forbidden == {}

# Test history property management
@pytest.mark.parametrize("history", [
    ('event', ('0.4', '0.6')), 
    ('event', ('0.4', '0.6'), 'event', ('0.4', '0.6')),
    ('a', ('0.4', '0.6'), 'b', ('0.4', '0.6'), 'c', ('0.4', '0.6')),
])
def test_memLexRepr_history(history):
    b = memLexRepr(generate_test_data())
    assert b.history == []
    b.history = history
    assert b.history == [history]
    del b.history
    assert b.history == []

# Test conversion to forbidden
@pytest.mark.parametrize("instants, history, forbidden", [
    (['04', '06', '10', '20'], ['event', ('04', '06')], {'event': [intervals.forbidden_interval(('00', '10'), ('00', '10'))]}),
    (['04', '10', '20', '26'], ['event', ('10', '26')], {'event': [intervals.forbidden_interval(('10',), ('20', '30'))]}),
    (['06', '10', '20', '26'], ['a', ('20', '26')], {'a': [intervals.forbidden_interval(('20',), ('20', '30'))]}),
    (['04', '06', '10', '20'], ['b', ('10', '20')], {'b': [intervals.forbidden_interval(('10', ), ('20', ))]})
])
def test_memLexRepr_as_forbidden(instants, history, forbidden):
    b = memLexRepr(memLexRepr.from_event(generate_test_event(1, 5), tables) + memLexRepr.from_event(generate_test_event(0, 10), tables), instants)
    b.history = history
    assert b.as_forbidden() == forbidden


# Test merge functionality
@pytest.mark.parametrize("singlet1, singlet2", 
    ((memLexRepr(memLexRepr.from_event(generate_test_event(0, 5*i), tables), ['1', '2']),
        memLexRepr(memLexRepr.from_event(generate_test_event(0, 10*i), tables), ['1', '2']),
    ) for i in range(10))
)
def test_memLexRepr_merge(singlet1, singlet2):
    c = singlet1.merge(singlet2)
    assert len(c) == 4
    assert [c[0].delete_event(event) in [singlet1, singlet2] for event in c[0].events_list]
    assert len(c[0].instants) == 4
    assert [c[1].delete_event(event) in [singlet1, singlet2] for event in c[1].events_list]
    assert len(c[1].instants) == 3
    assert [c[2].delete_event(event) in [singlet1, singlet2] for event in c[2].events_list]
    assert len(c[2].instants) == 3
    assert [c[3].delete_event(event) in [singlet1, singlet2] for event in c[3].events_list]
    assert len(c[3].instants) == 4

for i in range(10):
    test_memLexRepr_merge(memLexRepr(memLexRepr.from_event(generate_test_event(0, 5*i), tables), ['1', '2']),
        memLexRepr(memLexRepr.from_event(generate_test_event(0, 10*i), tables), ['1', '2']),
    )

# TODO: More extensive testing on _generate_insertion_points, _prune_from_memory, _generate_combinations