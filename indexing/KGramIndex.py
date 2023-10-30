class KGramIndex:
    def __init__(self, k):
        self.k = k  # The length of k-grams
        self.index = {}  # The k-gram index data structure

    def build_index(self, text):
        """
        Build the k-gram index for the given text.
        """
        text = text.lower()  # Convert text to lowercase (optional)
        for i in range(len(text) - self.k + 1):
            kgram = text[i:i + self.k]
            if kgram in self.index:
                self.index[kgram].append(i)
            else:
                self.index[kgram] = [i]

    def search(self, query):
        """
        Search for a query in the k-gram index and return positions.
        """
        query = query.lower()  # Convert query to lowercase (optional)
        positions = []
        for i in range(len(query) - self.k + 1):
            kgram = query[i:i + self.k]
            if kgram in self.index:
                positions.extend([pos - i for pos in self.index[kgram]])
        return positions