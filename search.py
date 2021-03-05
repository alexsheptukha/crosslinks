from itertools import permutations
import os
import json
from nltk import wordpunct_tokenize
from string import punctuation


class Search:
    def __init__(self, text_fields, url_field, punct, ext=".json"):
        self.punct = punct
        self.text_fields = text_fields
        self.ext = ext
        self.url_field = url_field

    def check_dir(self, name: str, files_arr: list):
        if os.path.isfile(name):
            files_arr.append(name)
        else:
            for file in os.listdir(name):
                self.check_dir(os.path.join(name, file), files_arr)

    def load_files(self, path):
        files = list()
        self.check_dir(path, files)
        valid_ext_files = [f for f in files if os.path.splitext(f)[1] == self.ext]
        return valid_ext_files

    def file_parser(self, filename, queries):
        with open(filename, "rb") as f:
            content = json.load(f)
        url = None
        for field in self.text_fields:
            text = content.get(field)
            if text is None:
                raise Exception(f"No field called {field}")
            text = text.lower()
            words = wordpunct_tokenize(text)
            words = [word for word in words if word not in self.punct]
            text = " ".join(words)

            if any(q in text for q in queries):
                found_url = content.get(self.url_field)
                del text
                if found_url is None:
                    raise Exception(f"No field called {self.url_field}")
                return found_url
        return url

    def dir_parser(self, path, queries):
        files = self.load_files(path)
        resp_urls = []
        for file in files:
            try:
                found_url = self.file_parser(file, queries)
            except Exception as e:
                print(e)
                continue
            if found_url is not None:
                resp_urls.append(found_url)
        return resp_urls

    def search(self, path, query):
        query = query.lower()
        query_words = wordpunct_tokenize(query)
        query_words = [word for word in query_words if word not in self.punct]
        possible_queries = permutations(query_words)
        possible_queries = [" ".join(words) for words in possible_queries]
        urls = self.dir_parser(path, possible_queries)
        return urls


if __name__ == "__main__":
    search = Search(text_fields=["article"], url_field="url", punct=punctuation)
    urls = search.search("/root/nlp/crosslinks/data/pages", "Quasi als Zweitausgabe")
    print(urls)