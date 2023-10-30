class Posting:
    def __init__(self, doc_id):
        self.doc_id = doc_id
        self.positions = []

    def add_position(self, position):
        self.positions.append(position)