import sqlite3
import struct
import os
from math import sqrt

class DiskPositionalInvertedIndex:
    def __init__(self, db_path, postings_path, doc_weights_path):
        self.db_connection = sqlite3.connect(db_path)
        self.postings_file = open(postings_path, 'wb')
        self.doc_weights_file = open(doc_weights_path, 'wb')
        self.db_connection.execute('''
            CREATE TABLE IF NOT EXISTS vocabulary (
                term TEXT PRIMARY KEY,
                postings_start INTEGER
            )
        ''')
        self.doc_id_mapping = {}
        self.current_doc_id = 0
    
    def add_document(self, document):
        doc_id = self.current_doc_id
        self.current_doc_id += 1
        doc_name = document['fileName']
        tokenData = []

        for position, term in enumerate(document['tokenData']):
            tokenData.append((term, [position]))  # Use a tuple to store term and positions

        doc_length = 0

        for term, positions in tokenData:
            term_frequency = len(positions)
            doc_length += term_frequency

            # Check if the term already exists in the vocabulary
            cursor = self.db_connection.execute("SELECT postings_start FROM vocabulary WHERE term=?", (term,))
            existing_postings_start = cursor.fetchone()
            
            if existing_postings_start:
                # Term already exists, update the postings_start
                postings_start = existing_postings_start[0]
            else:
                # Term doesn't exist, insert a new record
                postings_start = self.postings_file.tell()
                self.db_connection.execute("INSERT INTO vocabulary VALUES (?, ?)", (term, postings_start))

            # Save postings to the postings file
            postings_data = struct.pack('I', doc_id)
            postings_data += struct.pack('s', doc_name.encode('utf-8'))  # Gap-encoded doc_id
            postings_data += struct.pack('I', term_frequency)
            postings_data += struct.pack(f'{term_frequency}H', *positions)
            self.postings_file.write(postings_data)

        # Calculate and save document length (L_d) to doc_weights file
        doc_length = sqrt(doc_length)
        self.doc_weights_file.write(struct.pack('d', doc_length))

        self.doc_id_mapping[doc_id] = doc_length     


    def get_postings(self, term):
        if term:
            cursor = self.db_connection.execute("SELECT postings_start FROM vocabulary WHERE term=?", (term,))
            result = cursor.fetchone()
            if result:
                postings_start = result[0]
                with open("your_postings_path.bin", 'rb') as file:
                    file.seek(postings_start)
                    doc_ids = []
                    positions = []

                    while True:
                        try:
                            doc_id = struct.unpack('I', file.read(4))[0]
                            doc_name_length = struct.unpack('I', file.read(4))[0]
                            doc_name = struct.unpack(f'{doc_name_length}s', file.read(doc_name_length))[0].decode('utf-8')
                            term_frequency = struct.unpack('I', file.read(4))[0]

                            # Read positions as a list of integers
                            term_positions = struct.unpack(f'{term_frequency}H', file.read(2 * term_frequency))

                            doc_ids.append(doc_id)
                            positions.append((doc_name, term_frequency, term_positions))
                        except struct.error:
                            break
                    file.close()
                return doc_ids, positions
        return [], []



  
        
        
        
    def close(self):
        self.db_connection.commit()
        self.db_connection.close()
        self.postings_file.close()
        self.doc_weights_file.close()

    def get_doc_length(self, doc_id):
        return self.doc_id_mapping.get(doc_id, 0)

