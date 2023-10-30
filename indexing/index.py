from abc import ABC, abstractmethod
from typing import Iterable

from .postings import Posting

class Index(ABC):
    """An Index can retrieve postings for a term from a data structure associating terms and the documents
    that contain them."""
    def __init__(self):
        # Initialize an empty dictionary to store the inverted index.
        self.index = {}

    def get_postings(self, term : str) -> Iterable[Posting]:
        """Retrieves a sequence of Postings of documents that contain the given term."""
        # Check if the term exists in the index.
        if term in self.index:
            return self.index[term]
        else:
            return []

    def vocabulary(self) -> list[str]:
        """A (sorted) list of all terms in the index vocabulary."""
        return sorted(self.index.keys())