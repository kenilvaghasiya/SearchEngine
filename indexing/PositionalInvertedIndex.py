from bisect import bisect_left
from decimal import InvalidOperation
from pydoc import doc
from typing import Iterable
from .postings import Posting
from .index import Index


class PositionalInvertedIndex(Index):
    """Implements an Index using a term-document matrix. Requires knowing the full corpus
    vocabulary and number of documents prior to construction."""

    def __init__(self):
        self.vocabulary=set()
        self.InvertedIndex={}
        self.IndexFindnumber={}

    def add_term(self, term : str, doc_id : int):
        """Records that the given term occurred in the given document ID."""
        self.vocabulary.add(term)
        if term in self.InvertedIndex:
            if doc_id!=self.InvertedIndex[term][-1]:
                self.InvertedIndex[term].append(doc_id)
        else:
            self.InvertedIndex[term]=[doc_id]
            
    def add_termIndex(self, dataIndex,doc_id):
        """Records that the given term occurred in the given document ID."""
        if dataIndex["data"] in self.IndexFindnumber:
            self.IndexFindnumber[dataIndex["data"]].append({"filename":doc_id,"index":dataIndex["position"]})
        else:
            self.IndexFindnumber[dataIndex["data"]]=[{"filename":doc_id,"index":dataIndex["position"]}]
            
            
    def get_postings(self, term : str) -> Iterable[Posting]:
        """Returns a list of Postings for all documents that contain the given term."""
        if term in self.InvertedIndex:
            return self.InvertedIndex[term]
        else:
            return []
    
    def vocabulary(self) -> Iterable[str]:
        return sorted(list(self.vocabulary))
    
    def get_all_doc_ids(self):
        """Returns a list of all document IDs that appear in the inverted index."""
        all_doc_ids = set()
        for term, postings in self.InvertedIndex.items():
            all_doc_ids.update(postings)
        return sorted(list(all_doc_ids))

    def get_phrase_postings(self, phrase: str) -> Iterable[Posting]:
        if phrase in self.InvertedIndex:
            return self.IndexFindnumber[phrase]
        else:
            return []
