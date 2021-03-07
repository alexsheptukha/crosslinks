from itertools import permutations
import os
import ast
import sys
import json
from nltk import wordpunct_tokenize
from string import punctuation
import pandas as pd


class Search:
    def __init__(self, text_fields, url_field, punct):
        self.punct = punct
        self.text_fields = text_fields
        self.url_field = url_field

    def check_dir(self, name: str, files_arr: list):
        if os.path.isfile(name):
            files_arr.append(name)
        else:
            for file in os.listdir(name):
                self.check_dir(os.path.join(name, file), files_arr)

    def load_files(self, path, ext=".json"):
        files = list()
        self.check_dir(path, files)
        valid_ext_files = [f for f in files if os.path.splitext(f)[1] == ext]
        return valid_ext_files

    def file_parser(self, filename, queries):
        """
        Search for urls in a file
        :param filename:
        :param queries:
        :return:
        """
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
        """
        Search for urls in a directory
        :param path:
        :param queries:
        :return:
        """
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

    def search(self, query, path):
        query = query.lower()
        query_words = wordpunct_tokenize(query)
        query_words = [word for word in query_words if word not in self.punct]
        possible_queries = permutations(query_words)
        possible_queries = [" ".join(words) for words in possible_queries]
        urls = self.dir_parser(path, possible_queries)
        return urls

    def predict_from_csv(self, search_path, csv_file, field, out_csv):
        df = pd.read_csv(csv_file)
        columns = list(df.columns)
        df["URLS"] = df[field].apply(self.search, args=(search_path, ))
        df["Count of pages"] = df["URLS"].apply(len)
        columns.extend(["Count of pages", "URLS"])
        df = df.reindex(columns=columns)
        df.to_csv(out_csv, index=False)

    def predict_from_dir(self, input_dir, search_path, text_field, ext):
        files = self.load_files(input_dir, ext=ext)
        if ext == ".csv":
            for file in files:
                if "_edited" not in file:
                    out_filename = os.path.splitext(file)[0] + "_edited" + ".csv"
                    self.predict_from_csv(search_path, file, text_field, out_filename)


if __name__ == "__main__":
    # parameters for search
    search = Search(text_fields=["article"], url_field="url", punct=punctuation)
    # urls = search.search("/root/nlp/crosslinks/data/pages", "Quasi als Zweitausgabe")
    args = sys.argv
    if len(args[:-1]) != 2:
        exit("usage: python3 search.py csv_dir search_path")
    in_dir = args[1]
    s_path = args[2]
    search.predict_from_dir(input_dir=in_dir, search_path=s_path, text_field="Word", ext=".csv")





