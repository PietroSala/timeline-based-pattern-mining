from lexapriori_mem.lib.event import eventClass
import pytest

# Check for event class creation check successes
@pytest.mark.parametrize("timeline, event, start, end", [
    (1, 'a', 0, 1),
    (2, 'b', 1, 2),
    [3, 'x', 0, 3],
])
def test_event_initialization(timeline, event, start, end):
    e = eventClass(timeline, event, (start, end))
    assert e.timeline == timeline
    assert e.event == event
    assert e.start == start
    assert e.end == end

# Check for event class creation check failures
@pytest.mark.parametrize("timeline, event, start, end", [
    (1, 0, 0, 1),
    (2, 'b', 1, None),
    ['3', 'x', 0, 3],
    (1, 'a', 2, 1)
])
def test_event_initialization(timeline, event, start, end):
    with pytest.raises(ValueError):
        e = eventClass(timeline, event, (start, end))

# Check for string representation of event class
@pytest.mark.parametrize("timeline, event, start, end, expected", [
    (1, 'a', 0, 1, "(1, a, (0, 1))"),
    (2, 'b', 1, 2, "(2, b, (1, 2))"),
    [3, 'x', 0, 3, "(3, x, (0, 3))"],
])
def test_event_string(timeline, event, start, end, expected):
    e = eventClass(timeline, event, (start, end))
    assert str(e) == expected

# Check for equality of event class
@pytest.mark.parametrize("timeline, event, start, end, timeline2, event2, start2, end2, expected", [
    (1, 'a', 0, 1, 1, 'a', 0, 1, True),
    (2, 'b', 1, 2, 2, 'b', 1, 2, True),
    [3, 'x', 0, 3, 3, 'x', 0, 3, True],
    (1, 'a', 0, 1, 1, 'a', 0, 2, False),
    (2, 'b', 1, 2, 2, 'b', 1, 3, False),
    [3, 'x', 0, 3, 3, 'x', 0, 4, False],
])
def test_event_equality(timeline, event, start, end, timeline2, event2, start2, end2, expected):
    e = eventClass(timeline, event, (start, end))
    e2 = eventClass(timeline2, event2, (start2, end2))
    assert (e == e2) == expected

# Check for hash of event class
@pytest.mark.parametrize("timeline, event, start, end, timeline2, event2, start2, end2, expected", [
    (1, 'a', 0, 1, 1, 'a', 0, 1, True),
    (2, 'b', 1, 2, 2, 'b', 1, 2, True),
    [3, 'x', 0, 3, 3, 'x', 0, 3, True],
    (1, 'a', 0, 1, 1, 'a', 0, 2, False),
    (2, 'b', 1, 2, 2, 'b', 1, 3, False),
    [3, 'x', 0, 3, 3, 'x', 0, 4, False],
])
def test_event_hash(timeline, event, start, end, timeline2, event2, start2, end2, expected):
    e = eventClass(timeline, event, (start, end))
    e2 = eventClass(timeline2, event2, (start2, end2))
    assert (hash(e) == hash(e2)) == expected
    

# Test list-like interface
@pytest.mark.parametrize("timeline, event, start, end, index, expected", [
    (1, 'a', 0, 1, 0, 1),
    (2, 'b', 1, 2, 1, 'b'),
    [3, 'x', 0, 3, 2, (0, 3)],
    (2, 'c', 1, 2, -1, 2),
    (2, 'c', 1, 2, -2, 'c'),
    [3, 'x', 0, 3, -3, (0, 3)],
])
def test_event_getitem(timeline, event, start, end, index, expected):
    e = eventClass(timeline, event, (start, end))
    assert e[index] == expected
