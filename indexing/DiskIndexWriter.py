import struct
import sqlite3
from indexing import PositionalInvertedIndexSqlite
from porter2stemmer import Porter2Stemmer

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
                # Get the byte position for this term
                byte_position = file.tell()

                # Write the document frequency (number of documents where the term appears)
                packed_data = struct.pack("I", len(postings))
                file.write(packed_data)

                last_doc_id = 0
                print(postings)
                for doc_id, positions in postings:
                    print(term,doc_id,positions)
                    # Write the docID gap
                    packed_data = struct.pack("I", doc_id)
                    file.write(packed_data)
                    last_doc_id= doc_id+last_doc_id

                    # Write the term frequency for this document (number of times the term appears in the document)
                    packed_data = struct.pack("I", len(positions))
                    file.write(packed_data)

                    # Write the positions as gaps
                    last_position = 0
                    for pos in positions:
                        packed_data = struct.pack("I", pos - last_position)
                        file.write(packed_data)
                        last_position = pos

                # Save term and its byte position to the database
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

            dft_data = file.read(4)
            dft = struct.unpack("I", dft_data)[0]

            last_doc_id = 0
            for _ in range(dft):
                doc_id_gap_data = file.read(4)
                doc_id_gap = struct.unpack("I", doc_id_gap_data)[0]
                doc_id =  doc_id_gap
                last_doc_id = doc_id

                tftd_data = file.read(4)
                tftd = struct.unpack("I", tftd_data)[0]

                positions = []
                last_position = 0
                for _ in range(tftd):
                    pos_gap_data = file.read(4)
                    pos_gap = struct.unpack("I", pos_gap_data)[0]
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
