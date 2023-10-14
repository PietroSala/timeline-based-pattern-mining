"""Class for lexicon representation with memoization

This module contains the class for lexicon representation with memoization.

Example:
    The following example shows how to use the mem class to remember not to try 
    the same possibilities twice:

        >>> from lex_base import memLexRepr
        >>> data = [['a', 'b', 'c'], ['d', 'e', 'f']]
        >>> lex = memLexRepr(data)
        >>> print(lex)
        a b c
        d e f

        
"""

from __future__ import annotations
import copy
import itertools

from .lex_base import baseLexRepr
from ..lib import intervals
from ..tools import helper as utils


class memLexRepr(baseLexRepr):
    """Class for lexicon representation with memoization

    Inherits interface and everything from base class, adding memory and specific methods
    to exploit memoization.
    Defines a merge method to merge two lexicon representations and cut possibilities using
    memory.


    Attributes:
        data: The data to be wrapped into a lexical representation.
        instants: The instants corresponding to the data.

    Raises:
        ValueError: If the data is not in the correct format.
        ValueError: If the size of input instants is not equal to the number of instants in the data.
    """
    
    def __init__(self, input: list[list[str]], instants: list[str] = None):
        if not super().check_format(input):
            raise ValueError("Wrong format for input data")
        super().__init__(input)

        # Save instants values
        if instants is None:
            self.instants = []
        else:
            if len(instants) != len(input):
                raise ValueError(f"Wrong number of instants. Expected {len(input)} got {len(instants)}")
            self.instants = instants

        # Forbidden insertions
        self._forbidden = {}
        # Story of insertions
        self._history = []

    @property
    def forbidden(self) -> dict:    
        """dict: The forbidden rules for the lexical representation.
        
        The forbidden rules are stored as a dictionary, where the keys are the item names
        and the values are lists of forbidden intervals.
        """    

        return self._forbidden
    
    @forbidden.setter
    def forbidden(self, forbidden_rules: dict) -> None:

        # If the input is a list of dictionaries, merge them
        if isinstance(forbidden_rules, list) and all([isinstance(i, dict) for i in forbidden_rules]):
            merged = {}
            for i in forbidden_rules:
                merged = utils.merge_dicts(merged, i)

            forbidden_rules = merged

        # If the input is a dictionary, check if it is valid
        if isinstance(forbidden_rules, dict):
            if not all([isinstance(j, intervals.forbidden_interval) for i in forbidden_rules for j in forbidden_rules[i]]):
                raise TypeError(
                    'Input must be a dictionary of forbidden rules')

            # If the input is valid, merge it with the existing one
            for i in forbidden_rules:
                if i in self._forbidden:
                    self._forbidden[i] = list(
                        set(self._forbidden[i] + forbidden_rules[i]))
                else:
                    self._forbidden[i] = forbidden_rules[i]

        # If the input is not a dictionary nor a list, raise an error
        else:
            raise TypeError(
                f'Input must be a dict, or a list of dicts, instead got {type(forbidden_rules)}')

    @forbidden.deleter
    def forbidden(self) -> None:
        self._forbidden = {}

    @property
    def history(self) -> list:    
        """list: The history of insertions for the lexical representation.

        The history of insertions is stored as a list of tuples, where the first element
        is the item name and the second element is the interval added.
        This is useful to keep track of the insertions and to be able to turn them into rules 
        for fobidding the same intervals in the future.
        """

        return self._history
    
    @history.setter
    def history(self, value: list) -> None:
            self._history.append(value)

    @history.deleter
    def history(self) -> None:
        self._history = []

    def as_forbidden(self) -> dict[str, list[intervals.forbidden_interval]]:
        """Turn this lexical representation into a forbidden rule.

        This method turns the current lexical representation into a forbidden rule
        by extracting the last addition from the history and turning it into a forbidden
        interval object.

        """

        # Get last modification
        addition = self.history[-1]

        # Get the start and end instants
        s = addition[1][0]
        e = addition[1][1]

        # Get the next instant
        next_index = self.instants.index(e)+1
        # If there is no next instant, set it to 30*
        if next_index < len(self.instants):
            next_instant = self.instants[next_index]
        else:
            next_instant = '3' + '0'*(len(self.instants[0])-1)

        # If the start instant is a middle instant, set the lower bound to 0 for start
        if s[-1] != '0':
            s = (s[:-1] + '0', next_instant)
        else:
            s = (s,)

        # If the end instant is a middle instant, set the lower bound to 0 for end
        if e[-1] != '0':
            e = (e[:-1] + '0', next_instant)
        else:
            e = (e,)

        # Return a dictionary with the item name and the forbidden interval object
        return {addition[0]: [intervals.forbidden_interval(s, e)]}
    
    def __delitem__(self, index: int) -> None:
        super().__delitem__(index)
        del self.instants[index]
    
    def copy(self) -> baseLexRepr:
        # Check if instants are present
        instants = copy.deepcopy(self.instants)
        if instants == []:
            # If not pass None as parameter
            instants = None
        
        # Copy over the data
        temp = memLexRepr([copy.deepcopy(i) for i in self.data], instants)
        temp._forbidden = copy.deepcopy(self.forbidden)
        temp._history = copy.deepcopy(self.history)

        return temp

    def merge(self, other: memLexRepr) -> list[memLexRepr]:
        """Merge two lexical representations.
        
        This method merges two lexical representations, where one is a singlet,
        cutting the possibilities using the memory of the current lexical representation.
        
        Args:
            other: The other lexical representation to merge with.
            
        Raises:
            TypeError: If the input is not a memLexRepr object.
            ValueError: If the timelines of the two lexical representations do not match.
            
        Returns:
            A list of all the possible combinations of the two lexical representations.
                    
        """

        if not isinstance(other, memLexRepr):
            raise TypeError('Input must be a memLexRepr object')

        # Check if the timelines match
        if not self.check_compatibility(other):
            raise ValueError(
                f'Input timelines do not match, self got {len(self.data[0])} timelines, input got {len(other.data[0])} timelines')

        # Determine largest itemset
        if other.size > self.size:
            base = other
            add = self
        else:
            base = self
            add = other

        # Saving which timeline we are merging in and has conflicts
        timeline = add.events_list[0].timeline

        # Generate all accepted insertion points
        combinations_graph = memLexRepr._generate_insertion_points(
            base, timeline)

        # Extract item name we are merging
        item = other.events_list[0].event

        # Prune insertion points based on forbidden
        self._prune_from_memory(item, combinations_graph)

        # Prune empty insertion points
        for i in [i for i in combinations_graph]:
            if combinations_graph[i] == []:
                del combinations_graph[i]

        # Generate the actual combinations
        combinations_list = memLexRepr._generate_combinations(
            base, add, timeline, combinations_graph)

        # Delete duplicates
        return combinations_list

    def _generate_insertion_points(base, timeline) -> dict:
        base_data = base.data
        points = base.instants

        candidate_points = []

        # Generation of all possible insertion points
        updated_points = [i+'0' for i in points]
        # Add 0 and 3 to the list of points
        starting_point = '0'*(len(updated_points[0])-1) + '5'

        middle_points = []
        for point in range(len(updated_points)):
            middle_points.append(updated_points[point][:-1] + '5')

        # Merge into a single list
        candidate_points = list(itertools.chain.from_iterable(
            zip(updated_points, middle_points)))
        # Add the 0.5 point at the beginning
        candidate_points.insert(0, starting_point)

        # Extracting events from the timeline
        s, e = None, None
        # Create a fake starting event
        events_instants = [('0', '0'*(len(updated_points[0])))]
        for i in range(len(points)):
            if base_data[i][timeline].startswith('S'):
                if s is None:
                    s = updated_points[i]
            elif base_data[i][timeline].startswith('E'):
                e = updated_points[i]
                # Save event
                events_instants.append((s, e))
                s = None
        # Create a fake ending event
        events_instants.append(('3'+'0'*(len(updated_points[0])-1), 3))

        combinations_graph = {}
        for event in range(1, len(events_instants)):
            starting_points = []
            ending_points = []

            # Find all the points between the previous event and the current one suitable for starting
            for i in candidate_points:
                if i >= events_instants[event-1][1] and i < events_instants[event][0]:
                    starting_points.append(i)

            # Find all the points between the previous event and the current one suitable for ending
            for i in candidate_points:
                if i > events_instants[event-1][1] and i <= events_instants[event][0]:
                    ending_points.append(i)

            # For every starting point, add all the ending points that are after it or coincide with it when the starting point is a middle point
            for starting_point in starting_points:
                ending_points_list = [i for i in ending_points if (
                    starting_point[-1] == '5' and i >= starting_point) or i > starting_point]
                if ending_points_list != []:
                    combinations_graph[starting_point] = ending_points_list

        return combinations_graph

    def _prune_from_memory(self, item, combinations_graph):

        # If the item is registered as forbidden
        if item in self.forbidden:
            # For every forbidden insertion
            for forbidden in self.forbidden[item]:
                # For every starting point
                for start_point in [i for i in combinations_graph]:
                    # If matched inside a forbidden range
                    if forbidden.contains_start(start_point):
                        # Inspect and remove endpoints that are in forbidden range
                        for end_point in [i for i in combinations_graph[start_point]]:
                            if forbidden.contains_end(end_point):
                                combinations_graph[start_point].remove(
                                    end_point)

    def _generate_combinations(base, add, timeline, combinations_graph) -> list:
        if not (isinstance(base, memLexRepr) and isinstance(add, memLexRepr)):
            raise TypeError('Input must be a memLexRepr object')

        positions = (['0'*(len(base.instants[0]))] +
                     [i for i in base.instants] + ['3' + '0'*(len(base.instants[0])-1)])
        base_data = base.data
        add_data = add.data

        # Now we generate the actual combinations
        combinations_list = []
        for i in combinations_graph:
            # Get the position of the i value in the list
            i_position = positions.index(i[:-1])
            if i_position != 0 and not i[-1] == '5':
                i_position -= 1

            for j in combinations_graph[i]:
                j_position = positions.index(j[:-1])
                if j_position != 0 and not j[-1] == '5':
                    j_position -= 1

                combination = copy.deepcopy(base_data)

                # If the number is between 2 integers, insert a blank event in the middle
                if i[-1] != '0':

                    combination.insert(
                        i_position, base.gen_null(i_position))

                    # Register something has been added
                    offset = 1
                else:
                    offset = 0

                # Place the Start event
                combination[i_position][timeline] = add_data[0][timeline]

                # If the number is between 2 integers, insert the end event in the middle
                if j[-1] != '0':
                    combination.insert(j_position+offset,
                                       base.gen_null(j_position))

                # Place the end event if another Start event is not already present
                if not combination[j_position+offset][timeline].startswith('S'):
                    combination[j_position +
                                offset][timeline] = add_data[1][timeline]

                # Fill in the Intermediate events between the start and the end that has just been added
                for k in range(i_position+1, j_position+offset):
                    combination[k][timeline] = 'I_' + \
                        add_data[0][timeline].split('_')[1]

                # Get a copy of previous instants
                temp_instants = [i+'0' for i in positions[1:-1]]
                # If it was intermediate, add Start with a final 4
                if i[-1] != '0':
                    i = i[:-1] + '4'
                    temp_instants.insert(i_position, i)

                # If it was intermediate add End with a final 6
                if j[-1] != '0':
                    j = j[:-1] + '6'
                    temp_instants.insert(j_position+offset, j)

                combinations_list.append(
                    memLexRepr(combination, temp_instants))
                combinations_list[-1].history.append(
                    (add.events_list[0].event, (i, j)))

        return combinations_list

    def del_null(self) -> None:
        """Removes all the null events

        This function removes all the null events from the dataset.
        Null events are those that consist entirely of '_' and Intermediate
        events.

        """

        changed = False
        j = 0
        # Parses all the events and removes those that are null
        while j < len(self):
            if all([i == "_" or i.startswith("I") for i in self[j]]):
                del self[j]
                changed = True
            else:
                j += 1

        # Data have changed, invalidate cached values
        if changed:
            self._as_regex = None
            self._as_searchable_string = None
            self._event_list = None


    # Representation
    def __repr__(self) -> str:
        return f"memLexRepr({self.data})"
