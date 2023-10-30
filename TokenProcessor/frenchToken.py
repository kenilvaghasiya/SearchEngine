import nltk
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer

class frenchToken:
    def __init__(self):
        nltk.download('punkt')
        nltk.download('stopwords')
        self.stemmer = SnowballStemmer('french')

    def process_text(self, text):
        french_tokens = word_tokenize(text, language='french')
        french_terms = [self.stemmer.stem(token.lower()) for token in french_tokens]
        return french_tokens, french_terms

