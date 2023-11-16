import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from porter2stemmer import Porter2Stemmer
from nltk.stem import SnowballStemmer

class TokenProcessor:
    def __init__(self):
        self.stemmer = Porter2Stemmer()  # English stemmer
        self.non_alphanumeric_pattern = re.compile(r'^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$', re.UNICODE)
        self.stop_words = set(stopwords.words('english'))

    def process_token(self, token):
        # Step 1: Handle hyphens
    
        tokens = token.split()
        result_tokens = []
        for token in tokens:
            sub_tokens = token.split('-')
            result_tokens.extend(sub_tokens)
            combined_token = ''.join(sub_tokens)
            result_tokens.append(combined_token)
        hyphen_tokens =result_tokens

        # Step 2: Remove non-alphanumeric characters from the beginning and end
        cleaned_tokens = set([re.sub(self.non_alphanumeric_pattern, '', t) for t in hyphen_tokens])

        # Step 3: Remove apostrophes or quotation marks
        cleaned_tokens = set([t.replace("'", "").replace('"', '') for t in cleaned_tokens])

        # Step 4: Convert to lowercase
        cleaned_tokens = set([t.lower() for t in cleaned_tokens])

        # Return a list of resulting types
        return list(cleaned_tokens)

    def normalize_type(self, type):
        # Step 5: Stem using Porter2 Stemmer
        return self.stemmer.stem(type)


    def cleantoken(self,word):
        result_tokens = []
        sub_tokens = word.split('-')
        result_tokens.extend(sub_tokens)
        combined_token = ''.join(sub_tokens)
        result_tokens.append(combined_token)
        hyphen_tokens = result_tokens
        # Step 2: Remove non-alphanumeric characters from the beginning and end
        cleaned_tokens = re.sub(self.non_alphanumeric_pattern, '', hyphen_tokens[0])
        # Step 3: Remove apostrophes or quotation marks
        cleaned_tokens = cleaned_tokens.replace("'", "").replace('"', '') 
        # Step 4: Convert to lowercase
        cleaned_tokens = cleaned_tokens.lower()
        # Return a list of resulting types
        return cleaned_tokens



    def get_tokens_and_positions(self, text):
        words =  text.split()
        tokens_with_positions = []
        position = 0
        for word in words:
            tokens = self.process_token(word)
            for token in tokens:
                # Append token and its position
                tokens_with_positions.append({"data":self.normalize_type(self.cleantoken(token)),"position": position})
                position += 1  # Increment position for the next token
        return tokens_with_positions
    
    
    def get_tokens_and_sqlite(self, text):
        if len(text)==0 or text==None:
            return []
        words =  text.split()
        tokens_with_positions = []
        position = 0
        for word in words:
            tokens = self.process_token(word)
            for token in tokens:
                # Append token and its position
                tokens_with_positions.append(self.normalize_type(self.cleantoken(token)))
                position += 1  # Increment position for the next token
        return tokens_with_positions