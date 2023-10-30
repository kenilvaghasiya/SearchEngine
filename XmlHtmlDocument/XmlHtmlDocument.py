from bs4 import BeautifulSoup
import os

class XmlHtmlDocument:
    def __init__(self, file_path):
        self.file_path = file_path
        self.title = os.path.splitext(os.path.basename(file_path))[0]
        self.body = self.load_document()
        self.filetype = os.path.splitext(self.file_path)[1].lower()
       
        
    def load_document(self):
        with open(self.file_path, 'r', encoding='utf-8') as file:
            if os.path.splitext(self.file_path)[1].lower() == ".html":
                soup = BeautifulSoup(file, 'html.parser')
            elif os.path.splitext(self.file_path)[1].lower() == ".xml":
                soup = BeautifulSoup(file, 'lxml-xml')  # Use lxml parser for XML
            else:
                return ""  # Unknown document type
            text_content = soup.get_text(separator=' ')
            return text_content
        
    def dataSend(self):
        return   {"title":self.title, "body":self.body, "filetype":self.filetype}
        