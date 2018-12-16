# coding=utf-8
"""
Contains all the methods relevant to creating and accessing subreddit-level languages models.
"""
from nltk.lm.preprocessing import padded_everygram_pipeline
from nltk.lm import KneserNeyInterpolated, WittenBellInterpolated
from nltk.util import everygrams
from nltk.lm.preprocessing import pad_both_ends
from nltk.lm.preprocessing import flatten
from nltk.tokenize import sent_tokenize
from nltk import word_tokenize
import pickle
import os
import json
import gzip
import re
import csv

from collections import defaultdict


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
    paths = []
    for month in range(start_month, end_month+1):
        month = '{:02d}'.format(month)
        paths.append("RC_{}-{}-01".format(start_year, month))
    base_path_text = base_path + "non_top_level_comments/"
    subreddit_keys = ['subreddit":"{}'.format(subreddit) for subreddit in subreddit_list]
    print subreddit_keys[0]
    for f in os.listdir(base_path_text):
        if any(ext in f for ext in paths):
            file_path = base_path_text + f
            print file_path
            with open(file_path, 'rb') as fop:
                # because that's what this file and others had for output
                csvreadr = csv.reader(fop, delimiter=',', quotechar='|')
                csvreadr.next()
                for line in csvreadr:
                    comm = json.loads(line[0])
                    if any (subreddit in comm["subreddit"] for subreddit in subreddit_list):
                        #body = re.search('\"body\":\"(.+?)\"(,\")|(})', line).group(1)
                        body = comm["body"] #these two options appear to the be the same speed.
                        if body == "[deleted]" or body == "[removed]":
                            continue

                        subreddit = comm["subreddit"]

                        text_bodies[subreddit].append(body)
    return text_bodies


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
    file_name = "{}_{}_{}_{}_{}_{}_{}.pkl".format(subreddit, start_year,
                                                  start_month, end_month,
                                                  ngrams, text_min, text_max)
    file_path = language_model_base_path + file_name
    print file_path

    if not os.path.isfile(file_path):
        raise ValueError("the language model has not been created")
    file_path = language_model_base_path + file_name
    lm = pickle.load(open(file_path, "rb"))
    return lm

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
    # TODO: should you be setting these in the function???
    language_model_base_path  = base_path + "/language_models/"

    new_subreddit_list = []
    for subreddit in subreddit_list:
        file_name = "{}_{}_{}_{}_{}_{}_{}.pkl".format(subreddit, start_year, start_month, end_month, ngrams, text_min, text_max)
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
                text.append(res)

        #bad to have this twice
        file_name = "{}_{}_{}_{}_{}_{}_{}.pkl".format(subreddit, start_year, start_month, end_month, ngrams, text_min, text_max)
        file_path = language_model_base_path + file_name
        all_sents = [item for sublist in text for item in sublist]
        lm = create_language_model_nltk_everygrams(all_sents, ngrams=2)
        pickle.dump(lm, open(file_path, "wb"), protocol=pickle.HIGHEST_PROTOCOL)

# TODO: make this consistent over the whole project..?
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
    if len(text_body) > max_length:
        text_body = text_body[:max_length]
    text = sent_tokenize(text_body) #now split into sentences
    sents = [word_tokenize(sent) for sent in text] #now tokenize those sentences
    return sents


def create_language_model_nltk_everygrams(text, ngrams=2):
    """
    A first attempt at creating a language model.
    Likely very memory heavy and inefficient.
    :param subreddit:
    :param time_period:
    :return:
    """
    # see http://www.nltk.org/api/nltk.lm.html#module-nltk.lm.models
    train, vocab = padded_everygram_pipeline(ngrams, text)
    lm = WittenBellInterpolated(ngrams)
    lm.fit(train, vocab)
    print lm.generate(1, random_seed=3)
    return lm

# TODO: is inverse of entropy a thing??
def text_similarity_nltk_everygrams(texts, lm, ngrams, text_min, text_max):
    """
    Given a list of texts return a list of their inverse entropy
    :param texts: a list of strings, preprocessed as they were for the language model
    :param lm: the language model
    :return: a list of inverse entropys for each text
    """
    print lm.generate(6, random_seed=2)
    res = []
    for text in texts:
        text = preprocess_text(text, text_min, text_max)
        print text
        train, _ = padded_everygram_pipeline(ngrams, text)
        test = [('and'), ('when')]
        res.append(lm.entropy(
            test
        ))

    return res
