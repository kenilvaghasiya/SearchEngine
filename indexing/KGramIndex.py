from collections import defaultdict
import re

class KGramIndex:
    def __init__(self, k):
        self.k = k
        self.index = defaultdict(set)
        self.reverse_index = defaultdict(set)

    def add_object(self, obj):
        filename = obj['filename']
        words = obj['words']

        for word in words:
            # Convert the word to lowercase and modify it by adding $ to the beginning and end
            word = word.lower()
            modified_word = '$' + word + '$'
            
            for i in range(len(modified_word) - self.k + 1):
                kgram = modified_word[i:i + self.k]
                self.index[kgram].add(filename)

                # Build a reverse index for leading wildcard queries
                if i == 0:
                    self.reverse_index[kgram[::-1]].add(filename)
        print(self.index) 

    def search_trailing_wildcard(self, query):
        query = query.lower()  # Convert query to lowercase
        regex = re.compile(query.replace('*', '.*'))
        result = set()
        for kgram in self.index.keys():
            if regex.match(kgram):
                result.update(self.index[kgram])
        return result
    
    def search_leading_wildcard(self, query):
        query = query.lower()  # Convert query to lowercase
        regex_pattern = query.replace('*', '.*')
        regex = re.compile(f'^{regex_pattern}')
        result = set()

        for word in self.index.keys():
            if regex.match(word):
                result.update(self.index[word])

        return result

    def search_single_wildcard(self, query):
        query = query.lower()  # Convert query to lowercase
        result = set()

        # Split the query into segments separated by asterisks
        segments = query.split('*')

        # Construct a regex pattern to match the query segments
        regex_pattern = '.*'.join(map(re.escape, segments))
        regex = re.compile(regex_pattern)

        # Iterate through all words in the index
        for word in self.index.keys():
            if regex.match(word):
                result.update(self.index[word])

        return result





    def search_general_wildcard(self, query):
        query = query.lower()  # Convert query to lowercase
        regex = re.compile(query.replace('*', '.*'))
        result = set()
        for kgram in self.index.keys():
            if regex.match(kgram):
                result.update(self.index[kgram])
        return result


    def search_wildcard(self, query):
        if '*' in query:
            if query.startswith('*') and query.endswith('*'):
                # General wildcard query with wildcards at both ends
                return self.search_general_wildcard(query)
            elif query.startswith('*'):
                # Leading wildcard query
                return self.search_leading_wildcard(query)
            elif query.endswith('*'):
                # Trailing wildcard query
                return self.search_trailing_wildcard(query)
            else:
                # Single wildcard query
                return self.search_single_wildcard(query)
        else:
            # No wildcard, treat it as a regular query
            return self.search_trailing_wildcard(query)

# Example usage:
if __name__ == "__main__":
    objects = [
        {
            'filename': 'document1.txt',
            'words': ['apple', 'banana', 'cherry', 'date', 'elderberry']
        },
        {
            'filename': 'document2.txt',
            'words': ['fig', 'grape', 'honeydew', 'kiwy', 'lemon']
        },
        {
            'filename': 'document3.txt',
            'words': ["apple", 'mango', 'orange', 'pear', 'quince', 'raspberry']
        }
    ]

    kgram_index = KGramIndex(k=2)

    for obj in objects:
        kgram_index.add_object(obj)

    query = "ap*l"
    search_result = kgram_index.search_wildcard(query)
    print("Wildcard query:", query)
    print("Matching filenames:", search_result)
