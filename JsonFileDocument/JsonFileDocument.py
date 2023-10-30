import json

class JsonFileDocument:
    def __init__(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
            self.title = data.get('title', '')
            self.body = data.get('title', '')+data.get('body', '')
            self.url = data.get('url', '')
            
    def get_title(self):
        return self.title

    def get_body(self):
        if self.body is None:
            return ""
        else:
            return self.body
    
    def get_url(self):
        return self.url
