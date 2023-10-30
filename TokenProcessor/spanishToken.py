import nltk
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer

class spanishToken:
    def __init__(self):
        nltk.download('punkt')
        nltk.download('stopwords')
        self.stemmer = SnowballStemmer('spanish')

    def process_text(self, text):
        spanish_tokens = word_tokenize(text, language='spanish')
        spanish_terms = [self.stemmer.stem(token.lower()) for token in spanish_tokens]
        return spanish_tokens, spanish_terms


