# coding=utf-8
"""
Contains all the methods relevant to creating and accessing subreddit-level languages models.
"""
from nltk import word_tokenize, sent_tokenize
import os
import json
import csv
import extract_pairs
import numpy as np
from collections import defaultdict
import kenlm
import subprocess
import re

def get_relevant_text_bodies(subreddit_list, start_year, start_month, end_month, base_path):
    """
    Given a list of subreddits, returns the appropriate text for those subreddits
    :param subreddit_list:
    :param start_year:
    :param start_month:
    :param end_month:
    :param base_path:
    :return:
    """

    text_bodies = defaultdict(lambda: [])

    #this really should be done using bash brace expansion...

    base_path_full = base_path + "non_top_level_comments/"
    valid_file_paths = extract_pairs.list_file_appropriate_data_range(start_year,
                                                        start_month, end_month,
                                                        base_path_full)

    for file_path in valid_file_paths:
        with open(file_path, 'rb') as fop:
            # because that's what this file and others had for output
            csvreadr = csv.reader(fop, delimiter=',', quotechar='|')
            csvreadr.next()
            for line in csvreadr:
                try:
                    comm = json.loads(line[0])
                except ValueError:
                    print "BADLY FORMATTED", line
                    continue
                if any (subreddit in comm["subreddit"] for subreddit in subreddit_list):
                    #body = re.search('\"body\":\"(.+?)\"(,\")|(})', line).group(1)
                    body = comm["body"] #these two options appear to the be the same speed.
                    if body == "[deleted]" or body == "[removed]":
                        continue

                    subreddit = comm["subreddit"]

                    text_bodies[subreddit].append(body)
    return text_bodies


def create_language_model(joined_text, file_path, ngrams=3):

    with open("temp.txt", "w") as text_file:
        text_file.write(joined_text)

    print len(joined_text)
    subprocess.call('cat temp.txt | ~/kenlm/build/bin/lmplz --skip_symbols --discount_fallback -o {} -S 5G -T /tmp  > {}'.format(ngrams, file_path), shell=True)

    file_path_binary = file_path[:-4]+"klm"
    subprocess.call("~/kenlm/build/bin/build_binary {} {}".format(file_path, file_path_binary), shell=True)


def create_subreddit_language_models(subreddit_list, start_year, start_month, end_month, ngrams, text_min, text_max, base_path):
    """
    Search to see if you have a cached language model for the params given.
    If not, create it.

    :param subreddit_list: list of subreddits
    :param start_year: year
    :param start_month: month
    :param end_month: month
    :param ngrams: the n of ngrams
    :param text_min: minimum text length to warrant inclusion
    :param text_max: maximum text length
    :param base_path: the project directory. always the same.
    :return: None
    """
    language_model_base_path  = base_path + "/language_models/"

    new_subreddit_list = []
    for subreddit in subreddit_list:
        file_name = "{}_{}_{}_{}_{}_{}_{}.arpa".format(subreddit, start_year, start_month, end_month, ngrams, text_min, text_max)
        file_path = language_model_base_path + file_name
        if not os.path.isfile(file_path):
            print "looks like it doesn't exist, creating {} language model from scratch".format(subreddit)
            new_subreddit_list.append(subreddit)
        else:
            print "looks like {} language model already created".format(subreddit)
            continue

    if not new_subreddit_list:
        return

    comment_list = get_relevant_text_bodies(new_subreddit_list, start_year, start_month, end_month, base_path)

    for subreddit in new_subreddit_list:

        text_list = comment_list[subreddit]
        text = []
        for comm_text in text_list:
            res = preprocess_text(comm_text, text_min, text_max)
            if res:
                text.extend(res)

        joined_text = "\n".join(text)
        file_name = "{}_{}_{}_{}_{}_{}_{}.arpa".format(subreddit, start_year,
                                                      start_month, end_month,
                                                      ngrams, text_min,
                                                      text_max)
        file_path = language_model_base_path + file_name

        create_language_model(joined_text, file_path, ngrams=ngrams)

def preprocess_text(text_body, min_length, max_length):
    """
    Given a piece of text, lower it, remove punctuation and remove unicode
    If there are multiple sentences, mark boundaries and then flatten into a single list.
    :param text_body: the text body as a string
    :return: a list sentences, each with tokens.
    """

    if len(text_body) < min_length:
        return None
    #print text_body
    whitespace = "\r\n\t"
    text_body = text_body.strip(whitespace).lower().encode('ascii', 'ignore') #fix this
    text_body = re.sub(r'[^a-zA-Z0-9.,\s]', '', text_body)
    if len(text_body) > max_length:
        text_body = text_body[:max_length]
    sents = [' '.join(word_tokenize(sent)) for sent in sent_tokenize(text_body)] #now tokenize those sentences
    return sents

def load_language_model(subreddit, start_year, start_month, end_month, ngrams, text_min, text_max, base_path):
    """
    Loads and returns the language model that
    :param subreddit_list: list of subreddits
    :param start_year: year
    :param start_month: month
    :param end_month: month
    :param ngrams: the n of ngrams
    :param text_min: minimum text length to warrant inclusion
    :param text_max: maximum text length
    :param base_path: the project directory. always the same.
    :return:
    """
    language_model_base_path  = base_path + "language_models/"

    # TODO: make this global
    file_name = "{}_{}_{}_{}_{}_{}_{}.klm".format(subreddit, start_year,
                                                  start_month, end_month,
                                                  ngrams, text_min, text_max)

    file_path = language_model_base_path + file_name
    print file_path

    if not os.path.isfile(file_path):
        raise ValueError("the language model has not been created")
    file_path = language_model_base_path + file_name
    model = kenlm.LanguageModel(file_path)
    return model

# TODO: is inverse of entropy a thing??
def text_scores(texts, lm, text_min, text_max):
    """
    Given a list of texts return a list of their inverse entropy
    :param texts: a list of strings, preprocessed as they were for the language model
    :param lm: the language model
    :return: a list of  entropys for each text, plus the number of words in the text
    """
    res = []
    lengths = []
    for text in texts:
        text_length = 0
        text = preprocess_text(text, text_min, text_max)
        text_scores = []
        if not text:
            res.append(None)
            continue
        for sent in text:
            text_length += len(sent.split(" "))
            text_scores.append(lm.score(sent))
        res.append(np.mean(text_scores))
        lengths.append(text_length)

    return res, lengths