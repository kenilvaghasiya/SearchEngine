import struct
import sqlite3
from indexing import PositionalInvertedIndexSqlite
from porter2stemmer import Porter2Stemmer

def encode_number(number):
    if number < 0:
        raise ValueError("Negative numbers cannot be encoded using this method.")

    bytes_list = []
    while True:
        byte = number & 0x7F  # Get the last 7 bits
        number >>= 7
        if number == 0:
            bytes_list.append(byte)  # Last byte, do not set the high bit
            break
        else:
            bytes_list.append(byte | 0x80)  # Set the high bit
    return bytes_list 

def decode_bytes(byte_stream):
    number = 0
    shift = 0
    for byte in byte_stream:
        number |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return number

class DiskIndexWriter:
    def __init__(self, index: PositionalInvertedIndexSqlite, db_path: str, postings_file: str):
        self.index = index
        self.conn = sqlite3.connect(db_path)
        self.postings_file = postings_file
        # self.conn.execute("DROP TABLE IF EXISTS vocab_term_mapping")
        # self.conn.execute("""
        #     CREATE TABLE vocab_term_mapping (
        #         term TEXT PRIMARY KEY,
        #         byte_position INTEGER
        #     )
        # """)

        self.conn.commit()
    
    def write_index(self):
        with open(self.postings_file, 'wb') as file:
            for term, postings in self.index.index.items():
                byte_position = file.tell()

                # Variable byte encode the document frequency
                file.write(bytes(encode_number(len(postings))))

                last_doc_id = 0
                for doc_id, positions in postings:
                    # Encode and write the gap between document IDs
                    file.write(bytes(encode_number(doc_id)))
                    last_doc_id = doc_id

                    # Encode and write the term frequency
                    file.write(bytes(encode_number(len(positions))))

                    last_position = 0
                    for pos in positions:
                        # Encode and write the gap between positions
                        file.write(bytes(encode_number(pos - last_position)))
                        last_position = pos

                self.conn.execute("INSERT OR REPLACE INTO vocab_term_mapping (term, byte_position) VALUES (?, ?)", (term, byte_position))
                self.conn.commit()
    
    def close(self):
        self.conn.close()

class DiskPositionalIndex():
    def __init__(self, db_path: str, postings_file: str):
        self.conn = sqlite3.connect(db_path)
        self.postings_file = postings_file

    def get_postings(self, term):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM vocab_term_mapping WHERE term=?", (term,))
        result = cur.fetchone()
        if not result:
            return []

        byte_position = result[1]
        postings = []

        with open(self.postings_file, 'rb') as file:
            file.seek(byte_position)

            # Decode the document frequency
            dft = decode_bytes(iter(lambda: ord(file.read(1)), 0))

            last_doc_id = 0
            for _ in range(dft):
                # Decode the document ID gap
                doc_id_gap = decode_bytes(iter(lambda: ord(file.read(1)), 0))
                doc_id =  doc_id_gap
                last_doc_id = doc_id

                # Decode the term frequency
                tftd = decode_bytes(iter(lambda: ord(file.read(1)), 0))

                positions = []
                last_position = 0
                for _ in range(tftd):
                    # Decode the position gap
                    pos_gap = decode_bytes(iter(lambda: ord(file.read(1)), 0))
                    position = last_position + pos_gap
                    positions.append(position)
                    last_position = position

                postings.append((doc_id, positions))

        return postings
    def phrase_intersect(self, postings1, postings2, distance=1):

        results = []
        index=[]
        postings2_dict = {}
        for doc_id, positions in postings2:
            postings2_dict[doc_id] = positions
        
        for doc_id, positions in postings1:
            if doc_id in postings2_dict:
                # For each position in postings1, check if position+1 exists in postings2
                
                for pos in positions:
                    if pos + 1 in postings2_dict[doc_id]:
                        results.append((doc_id, [pos+1]))
                        # break  # To ensure unique doc_ids in the result, without repetition
        print(results)
        return results
        


    def merge_postings(self, postings1, postings2, operation):

        if operation == "AND":
            ids1 = {item[0] for item in postings1}
            common_tuples = [item for item in postings2 if item[0] in ids1]
            return common_tuples

        elif operation == "OR":
            dict2 = {item[0]: item[1] for item in postings2}
    
            combined_tuples = []
            
            for item in postings1:
                id1, positions1 = item
                if id1 in dict2:
                    # If id1 exists in arr2, use the positions from arr2
                    combined_tuples.append((id1, dict2[id1]))
                else:
                    # Otherwise, combine the positions
                    combined_tuples.append((id1, positions1 + dict2.get(id1, [])))
            
            # Now add the tuples from arr2 that are not in arr1
            ids1 = {item[0] for item in postings1}
            for id2, positions2 in dict2.items():
                if id2 not in ids1:
                    combined_tuples.append((id2, positions2))
            
            return combined_tuples

        elif operation == "AND NOT":
            ids1 = {item[0] for item in postings2}
            filtered_tuples = []
            for item in postings1:
                id2, positions12 = item
                if id2 not in ids1:
                    # If id2 does not exist in arr1, append the item to the result
                    filtered_tuples.append((id2, positions12))
            
            return filtered_tuples

    def query(self, terms, operations):
        if not terms:
            return []

        stopwords = Porter2Stemmer()

        def get_phrase_postings(phrase):
            words = phrase.split()
            if len(words) == 1:
                print("11111")
                return self.get_postings(stopwords.stem(words[0]))
            else:
                # Start with postings of the first word
                
                current_postings = self.get_postings(stopwords.stem(words[0].replace("\"","")))
                # Iterate over the rest of the words in the phrase
                print("22222",stopwords.stem(words[0].replace("\"","")))
                
                for i in range(1, len(words)):
                    next_postings = self.get_postings(stopwords.stem(words[i].replace("\"","")))
                    current_postings = self.phrase_intersect(current_postings, next_postings, i)
                    print("22222",stopwords.stem(words[i].replace("\"","")))
                    
                return current_postings

        postings = get_phrase_postings(terms[0])

        # Process the rest of the terms and operations
        for i in range(1, len(terms)):
            next_postings = get_phrase_postings(terms[i])
            postings = self.merge_postings(postings, next_postings, operations[i-1])

        return postings


    
    
    def close(self):
        self.conn.close()
