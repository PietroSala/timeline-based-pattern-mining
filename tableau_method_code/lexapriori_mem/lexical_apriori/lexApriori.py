import re
import copy
from ..lex.lex_mem import memLexRepr
from ..lib import intervals
from ..tools import preprocess
from tqdm import tqdm

import sqlite3


class apriori:
    """Apriori algorithm implementation

    Apriori algorithm implementation.
    It generates the next size of itemsets from the previous one
    and checks if they are supported by the dataset.
    If they are, it adds them to the next size.
    It stops when there are no more itemsets to generate.
    This implementation uses memoization to speed up the process and
    avoid generating the same itemsets multiple times.

    Attributes:
        dataset: The dataset to use for the algorithm
        epsilon: The minimum support threshold

    """

    def __init__(self, dataset, epsilon, database=None, save_all = False, cut_solutions=None):
        self.dataset = dataset
        self.epsilon = epsilon

        # Structure to save the itemsets during execution
        self.frequent_itemsets = {}
        self.frequent_itemsets_set = {}

        # Structure to save the candidates generated during execution
        self.candidate_next = {}

        # Structure to save the singlets extracted from the dataset
        self.singlets = []

        # Size of the itemsets at current iteration
        self.size = 0

        # Optional database connection
        self.database = database

        # Setup database connection
        if database is not None:
            self.frequent_tablename = 'frequent_itemsets'
            self.unfrequent_tablename = 'unfrequent_itemsets'
            self.save_all = save_all

            self._create_database()

        if cut_solutions is not None:
            
            new_cut_solutions = []
            for itemset in cut_solutions:
                new_cut_solutions.append(memLexRepr(preprocess.intervals_to_words(preprocess.dict_to_list(itemset))))

            self.cut_solutions = set(new_cut_solutions)
        else:
            self.cut_solutions = cut_solutions

    def _create_database(self) -> None:
        """ Create an SQLite database """

        conn = None
        
        # Check if database already exists. If so raise exception
        import os
        if os.path.exists(self.database):
            raise Exception('Database already exists')        

        # Create file if it doesn't exist and connect to it
        try:
            conn = sqlite3.connect(self.database)
        except sqlite3.Error as e:
            print(e)            

        cursor = conn.cursor()

        # Create table
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self.frequent_tablename}(itemset, support, timestamp)")
        
        if self.save_all:
            # Create table
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {self.unfrequent_tablename}(itemset, support, timestamp)")

        return conn


    def _database_connection(self) -> None:
        """ create a database connection to the SQLite database

            Address is specified by self.database passed during the creation of Apriori object
        """
        if self.database == None:
            raise Exception('No database connection provided')

        conn = None
        try:
            conn = sqlite3.connect(self.database)
        except sqlite3.Error as e:
            print(e)

        return conn

    def insert(self, itemset, support, tablename) -> None:
        """ Insert an itemset, support couple into the database

        """

        import datetime

        with self._database_connection() as conn:

            sql = f''' INSERT INTO {tablename}(itemset, support, timestamp)
                    VALUES(?,?,?) '''

            cur = conn.cursor()

            eventlist = {}
            for timeline in range(len(itemset[0])):
                eventlist[timeline] = []
            for event in itemset.events_list:
                eventlist[event.timeline].append(
                    (event.event, event.start, event.end))

            cur.execute(sql, (str(eventlist), support, datetime.datetime.now()))
            conn.commit()

    def _extract_items(self) -> None:
        """Extract all singlets from the dataset

        Parses the dataset and extracts all singlets from it,
        creating a list of memLexRepr objects with a single event each.
        Timelines positions are respected and the number of timelines
        is coherent.
        """
        temp = list()

        # In the whole dataset
        for data in self.dataset:
            text_events = data.events_list
            # For every timeline
            for event in text_events:
                # Generate new event
                new_event = memLexRepr(memLexRepr.from_event(
                    event, total_timelines=len(data[0])), ['1', '2'])
                if not (self.cut_solutions is not None and 
                                 new_event in self.cut_solutions):
                    temp.append(new_event)

        self.singlets = list(set(temp))

    def _generate_next(self) -> list[memLexRepr]:
        """Generate the next size of itemsets

        Generates the next size of itemsets from the previous one.
        It uses the previous size to check if the new candidates are
        backed by the previous size, and if they are, it adds them to
        the next size.
        If they are, it also adds the forbidden rules of the previous
        size to the current candidate.

        Returns:
            A list of memLexRepr objects with the next size of itemsets

        """

        next_size = []

        print(f'generating {self.size}:')

        for i in tqdm(self.frequent_itemsets[self.size-1]):
            for j in self.frequent_itemsets[1]:

                # Merge itemsets
                candidates = i.merge(j)

                # Check if candidate is backed by previous size
                # Remove all events one by one and check if the remaining is in the previous size
                # If it can always be found, then it is backed by the previous size and can be measured
                known_candidates = []
                for candidate in [i for i in candidates]:
                    if candidate not in known_candidates:
                        if (not self._check_reasonable(candidate) or
                                (self.cut_solutions is not None and 
                                 candidate in self.cut_solutions)):
                            candidates.remove(candidate)
                        else:
                            known_candidates.append(candidate)
                    else:
                        candidates.remove(candidate)

                # If there are some candidates left, add them to the next size
                if candidates != []:
                    next_size.append(candidates)

        return next_size

    def _check_reasonable(self, candidate: memLexRepr) -> bool:
        """Check if a candidate is backed by the previous size

        Check if a candidate is backed by the previous size.
        It does so by removing all events one by one and checking
        if the remaining is in the previous size.
        If it can always be found, then it is backed by the previous
        size and can be measured.
        In the meantime it also adds the forbidden rules of the
        previous size to the current candidate.

        Args:
            candidate: The candidate to check
            current_set: The previous size as set for faster search

        Returns:
            True if the candidate is backed by the previous size,
            False otherwise

        """
        found = True

        # Check if candidate is backed by previous size
        for event in candidate.events_list:

            # Try to extract a candidate of previous size equal to the current one minus an event
            candidate_previous = candidate.delete_event(event)

            # Try to get a match
            # match_candidate = self.frequent_itemsets_set[self.size-1] & {candidate_previous}
            match_candidate = []
            for i in self.frequent_itemsets[self.size-1]:
                if i == candidate_previous:
                    match_candidate.append(i)

            # If there is no match, then the candidate is not backed by the previous size and can be removed
            if len(match_candidate) == 0:
                found = False
            else:
                assert len(
                    match_candidate) == 1, f'Found more than one match for {candidate_previous}'

                shifted_rule = {}
                match_candidate = match_candidate.pop()
                # Forward pass
                for event_name in match_candidate.forbidden:
                    shifted_rule[event_name] = []
                for event_name in match_candidate.forbidden:

                    for rule in match_candidate.forbidden[event_name]:

                        # Create new shifted rule
                        temp_start = []
                        temp_end = []
                        for i in rule.start:
                            if bool(re.match('^0+$', i)):
                                temp_start.append(
                                    '0'*len(candidate.instants[0]))
                            elif bool(re.match('^30*$', i)):
                                temp_start.append(
                                    '3' + '0'*(len(candidate.instants[0])-1))
                            else:
                                temp_start.append(
                                    candidate_previous.instants[match_candidate.instants.index(i)])
                        for i in rule.end:
                            if bool(re.match('^0+$', i)):
                                temp_end.append('0'*len(candidate.instants[0]))
                            elif bool(re.match('^30*$', i)):
                                temp_end.append(
                                    '3' + '0'*(len(candidate.instants[0])-1))
                            else:
                                temp_end.append(
                                    candidate_previous.instants[match_candidate.instants.index(i)])

                        # Add rule to candidate forbidden
                        shifted_rule[event_name].append(
                            intervals.forbidden_interval(tuple(temp_start), tuple(temp_end)))
                    candidate.forbidden = shifted_rule

                # Backward pass
                # Find start interval
                temp_start = []
                try:
                    # If the instant actually coincides
                    temp_start.append(match_candidate.instants[candidate_previous.instants.index(
                        candidate.instants[event.start])])
                except ValueError:
                    # Start event is in the first instant
                    if event.start == 0:

                        # start of interval is 0, end is the first event
                        temp_start.append(
                            '0'*(len(match_candidate.instants[0])))
                        temp_start.append(
                            match_candidate.instants[event.start])

                    else:

                        # Get the previous instant's index
                        previous_instant = candidate.instants[event.start-1]
                        previous_instant_index = candidate_previous.instants.index(
                            previous_instant)

                        # Add this instant as start
                        temp_start.append(
                            match_candidate.instants[previous_instant_index])

                        # If it was the last instant, add 3 as end
                        if previous_instant_index == len(candidate_previous.instants)-1:
                            temp_start.append(
                                '3' + '0'*(len(match_candidate.instants[0])-1))
                        else:
                            # Otherwise add the next instant as end
                            temp_start.append(
                                match_candidate.instants[previous_instant_index+1])

                temp_end = []
                try:
                    # If the instant actually coincides
                    temp_end.append(match_candidate.instants[candidate_previous.instants.index(
                        candidate.instants[event.end])])
                except ValueError:
                    # If the end event is the last instant
                    if event.end == len(candidate.instants)-1:
                        # The interval is the last instant and 3
                        temp_end.append(match_candidate.instants[-1])
                        temp_end.append(
                            '3' + '0'*(len(match_candidate.instants[0])-1))
                    else:

                        # Otherwise get the next instant's index
                        next_instant = candidate.instants[event.end+1]
                        next_instant_index = candidate_previous.instants.index(
                            next_instant)

                        # If the next instant is the first one, add 0 as start
                        if next_instant_index == 0:
                            temp_end.append(
                                '0'*(len(match_candidate.instants[0])))
                        else:
                            # Otherwise add the previous instant as start
                            temp_end.append(
                                match_candidate.instants[next_instant_index-1])

                        # Add this instant as end
                        temp_end.append(
                            match_candidate.instants[next_instant_index])

                shifted_rule = intervals.forbidden_interval(
                    tuple(temp_start), tuple(temp_end))
                match_candidate.forbidden = {event.event: [shifted_rule]}

        return found

    def _check_group_support(self) -> None:
        """Check support for every generated group and remove unsupported ones

        Check support for every generated group and remove unsupported ones,
        propagating them as forbidden rules to siblings.
        Finally flatten the list of groups into a list of itemsets.
        """

        # Check support for every generated group and remove unsupported ones, saving them into forbidden rules
        temp = copy.deepcopy(self.candidate_next[self.size])
        for group in temp:
            for candidate in [i for i in group]:
                if self.support(candidate) < self.epsilon:
                    group.remove(candidate)
                    if self.database is not None and self.save_all:
                        self.insert(candidate, self.support(candidate), self.unfrequent_tablename)
                else:
                    if self.database is not None:
                        self.insert(candidate, self.support(candidate), self.frequent_tablename)

        # Extract supported ones from nonempty groups
        return [j for i in temp for j in i if len(i) != 0]

    def support(self, itemset: memLexRepr) -> float:
        """Calculate support for an itemset

        Calculate support for an itemset, given a dataset.
        It does so by calculating the number of times the itemset
        is found in a subject in the dataset and dividing it by the number of
        subjects in the dataset.
        """
        return sum([itemset in data for data in self.dataset])/len(self.dataset)

    def apriori(self) -> dict[int, list[memLexRepr]]:
        """Apriori algorithm

        Apriori algorithm implementation.
        It generates the next size of itemsets from the previous one
        and checks if they are supported by the dataset.
        If they are, it adds them to the next size.
        It stops when there are no more itemsets to generate.
        This implementation uses memoization to speed up the process and
        avoid generating the same itemsets multiple times.

        Returns:
            A dictionary of itemsets, where the key is the size of the itemsets
            and the value is a list of memLexRepr objects with that size.

        """

        # Generate first size
        if self.singlets == []:
            self._extract_items()
        self.size = 1

        # Save first size
        self.candidate_next[self.size] = [self.singlets]

        self.frequent_itemsets[self.size] = []

        # Filter out unsupported ones
        for itemset in self.singlets:
            supp = self.support(itemset)
            if supp >= self.epsilon:
                self.frequent_itemsets[self.size].append(itemset)
                if self.database is not None:
                    self.insert(itemset, supp, self.frequent_tablename)
            else:
                if self.database is not None and self.save_all:
                    self.insert(itemset, supp, self.unfrequent_tablename)

        while self.frequent_itemsets[self.size] != []:
            self.size += 1

            # Generate next batch of candidates
            self.candidate_next[self.size] = self._generate_next()

            # Filter out unsupported ones
            self.frequent_itemsets[self.size] = self._check_group_support()

        return self.frequent_itemsets

    def print_statistics(self) -> None:

        output = f'Apriori memoization algorithm statistics\n'

        output += f'Number of singlets: {len(self.singlets)}\n'

        for size in self.candidate_next:
            output += f'Itemsets of size {size}: {sum([len(i) for i in self.candidate_next[size]])}\n'

        for size in self.frequent_itemsets:
            output += f'Frequent itemsets of size {size}: {len(self.frequent_itemsets[size])}\n'

        output += f'\n'
        return output
