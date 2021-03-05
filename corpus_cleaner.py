import json
import os
import threading
import pandas as pd
from string import punctuation
import uuid
import concurrent.futures
from readability.readability import Document as Paper
from bs4 import BeautifulSoup
import spacy
from nltk import wordpunct_tokenize
from nltk.corpus import stopwords
import nltk

INPUT_DIR = "/home/alexey/nlp/crosslinks/data/pages/pages"
CLASS_DIR = "/home/alexey/nlp/crosslinks/data/clean_galileo"
TEXT_FIELDS = ["article"]


class JsonCorpusCleaner:
    def __init__(self, input_dir, class_dir, text_fields, ext=".json"):
        self.input_dir = input_dir
        self.text_fields = text_fields
        self.class_dir = class_dir
        self.ext = ext
        if not os.path.exists(input_dir):
            os.mkdir(input_dir)
        if not os.path.exists(class_dir):
            os.mkdir(class_dir)
        try:
            self.nlp = spacy.load("de_core_news_lg")
        except:
            os.system("python -m spacy download de_core_news_lg")
            self.nlp = spacy.load("de_core_news_lg")
        try:
            self.stopwords = stopwords.words("german")
        except:
            nltk.download("words")
            nltk.download("stopwords")
            nltk.download("punkt")
            self.stopwords = stopwords.words("german")

    def transform(self, text):
        bad_tags = ["-PRON-"]
        # readable = Paper(text).summary()
        # parser = BeautifulSoup(readable, "lxml")
        # text = parser.text
        # parser.decompose()
        text = text.lower()
        words = wordpunct_tokenize(text)
        words = [word for word in words if word not in punctuation and word not in self.stopwords]
        # remove digits and short words
        words = [word for word in words if len(word) > 2 and not any(s.isdigit() for s in word)]
        text = " ".join(words)
        doc = self.nlp(text)
        lemmas = [word.lemma_ for word in doc]
        lemmas = [lemma for lemma in lemmas if lemma not in bad_tags]
        out = " ".join(lemmas)
        return out

    def check_dir(self, name: str, files_arr: list):
        if os.path.isfile(name):
            files_arr.append(name)
        else:
            for file in os.listdir(name):
                self.check_dir(os.path.join(name, file), files_arr)

    def load_files(self):
        files = list()
        self.check_dir(self.input_dir, files)
        valid_ext_files = [f for f in files if os.path.splitext(f)[1] == self.ext]
        return valid_ext_files

    def file_parser(self, input_file):
        out_ext = ".txt"
        with open(input_file, "rb") as f:
            content = json.load(f)
            out_text = ""
            out_dir = self.class_dir
            for field in self.text_fields:
                text = content.get(field)
                if text is not None:
                    try:
                        text = self.transform(text)
                        out_text += text
                        out_text += "\n"
                        del text
                    except Exception as e:
                        print(e)
                        del text
                        continue
            if len(out_text) == 0:
                return "no text"
            fname = os.path.join(out_dir, str(uuid.uuid4()) + out_ext)
            with open(fname, "w+") as out_f:
                out_f.write(out_text)
            return fname + " " + threading.current_thread().getName()

    def dir_parser(self, chunk_size, threads, limit):
        all_files = self.load_files()[:limit]
        for i in range(0, len(all_files)-chunk_size):
            files_batch = all_files[i:i+chunk_size+1]
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                futures = []
                for file in files_batch:
                    futures.append(executor.submit(self.file_parser, input_file=file))
                for future in concurrent.futures.as_completed(futures):
                    try:
                        print(future.result())
                    except Exception as e:
                        print(e)


if __name__ == "__main__":
    cleaner_args = {
        "input_dir": INPUT_DIR,
        "class_dir": CLASS_DIR,
        "text_fields": TEXT_FIELDS
    }
    corpus_cleaner = JsonCorpusCleaner(**cleaner_args)
    corpus_cleaner.dir_parser(chunk_size=5, threads=5, limit=100000)





