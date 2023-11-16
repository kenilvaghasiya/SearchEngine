import struct
import sqlite3
from indexing import PositionalInvertedIndexSqlite
from porter2stemmer import Porter2Stemmer
from JsonFileDocument import JsonFileDocument
import math
import os
import numpy as np
import json

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
        self.total_length={}
        self.total_length_LD={}
        
        # self.conn.execute("DROP TABLE IF EXISTS vocab_term_mapping")
        # self.conn.execute("""
        #     CREATE TABLE vocab_term_mapping (
        #         term TEXT PRIMARY KEY,
        #         byte_position INTEGER
        #     )
        # """)
        # self.conn.execute("DROP TABLE IF EXISTS All_length")
        # self.conn.execute("""
        #     CREATE TABLE All_length (
        #         id TEXT PRIMARY KEY,
        #         All_length INTEGER
        #     )
        # """)
        # self.conn.execute("DROP TABLE IF EXISTS total_length")
        # self.conn.execute("""
        #     CREATE TABLE total_length (
        #         id TEXT PRIMARY KEY,
        #         total_position INTEGER,
        #         ld REAL
        #     )
        # """)

        self.conn.commit()
    
    def write_index(self):
        with open(self.postings_file, 'wb') as file:
            All_length=0
            for term, postings in self.index.index.items():
                byte_position = file.tell()

                # Variable byte encode the document frequency
                file.write(bytes(encode_number(len(postings))))

                last_doc_id = 0
                for doc_id, positions in postings:
                    # Encode and write the gap between document IDs
                    file.write(bytes(encode_number(doc_id)))
                    last_doc_id = doc_id
                    if doc_id in self.total_length:
                        self.total_length[doc_id]+=len(positions)
                    else:
                        self.total_length[doc_id]=len(positions)
                    if doc_id in self.total_length_LD:
                        self.total_length_LD[doc_id]+=(1+math.log(len(positions)))*(1+math.log(len(positions)))
                    else:
                        self.total_length_LD[doc_id]=(1+math.log(len(positions)))*(1+math.log(len(positions)))

                    # Encode and write the term frequency
                    file.write(bytes(encode_number(len(positions))))

                    last_position = 0
                    for pos in positions:
                        # Encode and write the gap between positions
                        file.write(bytes(encode_number(pos - last_position)))
                        last_position = pos

                self.conn.execute("INSERT OR REPLACE INTO vocab_term_mapping (term, byte_position) VALUES (?, ?)", (term, byte_position))
                self.conn.commit()
            for id, total_position in  self.total_length.items():
                All_length += total_position
                self.conn.execute("INSERT OR REPLACE INTO total_length (id, total_position,ld) VALUES (?, ?,?)", (id, total_position,np.sqrt(self.total_length_LD[id])))
                self.conn.commit()
            self.conn.execute("INSERT OR REPLACE INTO All_length (id, All_length) VALUES (?, ?)", (1, All_length))
            self.conn.commit()
                
            
    
    def close(self):
        self.conn.close()

