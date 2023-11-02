from bisect import bisect_left
from decimal import InvalidOperation
from pydoc import doc
from typing import Iterable
from .postings import Posting
from .index import Index


class PositionalInvertedIndexSqlite(Index):
    def __init__(self):
            self.index = {}  # Initialize the inverted index as an empty dictionary.

    def add_term(self, term, doc_id, positions):
        """
        Add a term to the index with its associated document ID and positions.
        Args:
            term (str): The term to add to the index.
            doc_id (int): The ID of the document containing the term.
            positions (list): A list of positions where the term occurs in the document.
        """
        if term not in self.index:
            self.index[term] = []
        self.index[term].append((doc_id, positions))  # Append (doc_id, positions) to the list.

    def get_postings(self, term):
        """
        Retrieve the postings list for a given term.
        Args:
            term (str): The term for which to retrieve the postings list.
        Returns:
            list: A list of (doc_id, positions) pairs.
        """
        return self.index.get(term, [])  # Return the postings list for the term or an empty list if not found.

    def search(self, query):
        """
        Perform a positional search query.
        Args:
            query (str): The search query, e.g., "apple AND car".
        Returns:
            list: A list of document IDs that match the query.
        """
        query_terms = query.split()  # Split the query into individual terms

        # Initialize the result set with the first term's postings
        result_set = set(doc_id for doc_id, _ in self.get_postings(query_terms[0]))

        # Iterate through the query terms
        for term in query_terms[1:]:
            if term.upper() == "AND":
                continue  # Skip the logical operator
            term_postings = self.get_postings(term)
            term_set = set(doc_id for doc_id, _ in term_postings)

            # Apply positional intersection logic
            result_set = result_set.intersection(term_set)

        return list(result_set)