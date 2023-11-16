import re
from porter2stemmer import Porter2Stemmer
from nltk.corpus import stopwords


class BooleanQueryParser:
    def __init__(self, index):
        self.index = index

    def parse_query(self, query):
        # Tokenize the query into components
        result2 = re.findall(r'\w+|AND|OR|NOT|"[^"]+"|[+-]', query)
        stemmer = Porter2Stemmer()
        result=[]
        for i in range(len(result2)):
            if result2[i] == "+":
                result.append("or")
            elif result2[i] == "-":
                result.append("and")
                result.append("not")
            else:
                result.append(stemmer.stem(result2[i]))
      
        tokens = []
        i = 0
        if(len(result)==1):
            tokens.append(result[i])
        while i+1 < len(result):
            element1 = result[i]
            element2 = result[i+1]
            if element1.lower()  not in {'and', 'or', 'not'}:
                if element2.lower() not in  {'and', 'or', 'not'}:
                    tokens.append(element1)
                    tokens.append('and')
                
            if element1.lower() in {'and', 'or', 'not'}:
                if element2.lower()not in  {'and', 'or', 'not'}:
                    tokens.append(element1)
                
            if element1.lower() not in {'and', 'or', 'not'}:
                if element2.lower() in  {'and', 'or', 'not'}:
                    tokens.append(element1)
            
            if element1.lower() in {'and'}:
                if element2.lower() in  {'not'}:
                    tokens.append('and')
                
            if(len(result)-2==i):
                tokens.append(element2)
        

            i=i+1
        components = []
        print(tokens)
        for i in range(len(tokens)):
            token = tokens[i]

            if (token == 'AND' or token == 'and'):
                components.append('AND')
            elif (token == 'OR' or token == 'or'):
                components.append('OR')
            elif (token == 'not'):
                components.append('NOT')
            elif token.startswith('"') and token.endswith('"'):
                components.append(PhraseLiteral(token.strip('"')))
            else:
                # Check for "AND NOT" query
                if i + 2 < len(tokens) and (tokens[i + 1] == 'NOT' or tokens[i + 1] == 'not'):
                    and_not_query = AndNotQuery(TermLiteral(token), TermLiteral(tokens[i + 2]))
                    components.append(and_not_query)
                    i += 2
                else:
                    components.append(TermLiteral(token))
                    

        # Build the query tree using components
        return self.build_query_tree(components)
    
    
    def and_not_merge(self, postings1, postings2):
        result = []
        i, j = 0, 0

        while i < len(postings1) and j < len(postings2):
            if postings1[i] == postings2[j]:
                i += 1
                j += 1
            elif postings1[i] < postings2[j]:
                result.append(postings1[i])
                i += 1
            else:
                j += 1

        # Add remaining postings from postings1
        while i < len(postings1):
            result.append(postings1[i])
            i += 1

        return result

    def build_query_tree(self, components):
        stack = []
        output = []

        for component in components:
            if isinstance(component, TermLiteral) or isinstance(component, PhraseLiteral):
                output.append(component)
            elif component == 'NOT':
                while stack and stack[-1] == 'NOT':
                    output.append(stack.pop())
                stack.append('NOT')
            elif component in ('AND', 'OR'):
                while stack and stack[-1] in ('AND', 'OR'):
                    output.append(stack.pop())
                stack.append(component)

        while stack:
            output.append(stack.pop())

        query_stack = []

        for component in output:
            if isinstance(component, TermLiteral) or isinstance(component, PhraseLiteral):
                query_stack.append(component)
            elif component == 'NOT':
                operand = query_stack.pop()
                query_stack.append(NotQuery(operand))
            elif component == 'AND':
                right_operand = query_stack.pop()
                left_operand = query_stack.pop()
                query_stack.append(AndQuery(left_operand, right_operand))
            elif component == 'OR':
                right_operand = query_stack.pop()
                left_operand = query_stack.pop()
                query_stack.append(OrQuery(left_operand, right_operand))

        return query_stack[0]

    def get_postings(self, query_component):
        if isinstance(query_component, TermLiteral):
            return self.index.get_postings(query_component.term)
        elif isinstance(query_component, PhraseLiteral):
            print("PhraseLiteral OPT")
            return self.phrase_query(query_component)
        elif isinstance(query_component, AndQuery):
            print("AND OPT")
            postings = self.get_postings(query_component.operands[0])
            for operand in query_component.operands[1:]:
              
                postings = self.and_merge(postings, self.get_postings(operand))
            return postings
        elif isinstance(query_component, OrQuery):
            print("OR OPT")
            postings = self.get_postings(query_component.operands[0])
            for operand in query_component.operands[1:]:
                print(query_component.operands[0],self.get_postings(operand))
                
                postings = self.or_merge(postings, self.get_postings(operand))
            return postings
        
        elif isinstance(query_component, NotQuery):
            print("NOT OPT")
            return self.not_query(query_component)
        
        elif isinstance(query_component, AndNotQuery):
            print("AND NOT OPT")
            postings1 = self.get_postings(query_component.operands[0])
            postings2 = self.get_postings(query_component.operands[1])
            return self.and_not_merge(postings1, postings2)
        # Add handling for wildcard queries if desired

    def phrase_query(self, phrase_literal):
        term=str(phrase_literal.terms[0]).split(" ")
        stemmer = Porter2Stemmer()
        postings = self.index.get_phrase_postings(stemmer.stem(term[0]))
        
        # Iterate through the rest of the terms in the phrase
        for i in range(1, len(term)):
            term_postings = self.index.get_phrase_postings(stemmer.stem(term[i]))
            # Merge the current postings with the term's postings
            postings = self.phrase_merge(postings, term_postings)
            
        unique_filenames = set()

        for item in postings:
            filename = item.get('filename')
            if filename is not None:  # Check if 'filename' key exists in the dictionary
                unique_filenames.add(filename)

        # Convert the set of unique filenames back to a list if needed
        unique_filenames_list = list(unique_filenames)


        return unique_filenames_list

    def not_query(self, not_query):
        operand_postings = self.get_postings(not_query.operand)
        all_doc_ids = set(self.index.get_all_doc_ids())
        not_doc_ids = all_doc_ids - set(operand_postings)
        return list(not_doc_ids)

    # Add methods for wildcard queries if desired

    # Add your existing methods for "AND" and "OR" merges

    def and_merge(self, postings1, postings2):
        result = []
        i, j = 0, 0
        duplicates = [value for value in postings1 if value in postings2]
        return duplicates
    
    def or_merge(self, postings1, postings2):
        # Implement "OR" merge logic

        result =list(set(postings1 + postings2))
        return result
    

    def phrase_merge(self, postings1, postings2):
        if postings1 is None or postings2 is None:
            return []
        postings2_index = {(obj['filename'], obj['index']): obj for obj in postings2}
        result = []
        for obj1 in postings1:
            filename = obj1['filename']
            index = obj1['index']

            # Check if (filename, index-1) exists in postings2_index
            if (filename, index +  1) in postings2_index:
                result.append(postings2_index[(filename, index + 1)])

        return result

class TermLiteral:
    def __init__(self, term):
        self.term = term

class AndQuery:
    def __init__(self, operand1, operand2):
        self.operands = [operand1, operand2]

class OrQuery:
    def __init__(self, operand1, operand2):
        self.operands = [operand1, operand2]

class PhraseLiteral:
    def __init__(self, phrase):
        self.terms = [phrase]
        self.spt = phrase.split()

class NotQuery:
    def __init__(self, operand):
        self.operand = operand

class AndNotQuery:
    def __init__(self, operand1, operand2):
        self.operands = [operand1, operand2]