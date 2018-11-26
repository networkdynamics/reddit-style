# coding=utf-8
"""
Contains all the methods relevant to creating and accessing subreddit-level languages models.
"""
from nltk.lm.preprocessing import padded_everygram_pipeline
from nltk.lm import KneserNeyInterpolated
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
from collections import defaultdict

def get_relevant_text_bodies(subreddit_list, start_year, start_month, end_month):
    """
    Currently slower than it needs to be. Finds the relevant text.
    :param subreddit:
    :param start_year:
    :param start_month:
    :param end_month:
    :return:
    """

    text_bodies = defaultdict(lambda: [])

    #this really should be done using bash brace expansion...
    paths = []
    for month in range(start_month, end_month+1):
        month = '{:02d}'.format(month)
        paths.append("RC_{}-{}-12-01".format(start_year, month)) #TODO: you edited this
    base_path = '/home/ndg/arc/reddit/{}/'.format(start_year)
    subreddit_keys = ['subreddit":"{}'.format(subreddit) for subreddit in subreddit_list]

    for f in os.listdir(base_path):
        if any(ext in f for ext in paths):
            file_path = base_path + f
            fop = gzip.open(file_path, 'rb')
            for line in fop:
                if any(subreddit_key in line for subreddit_key in subreddit_keys):  # aka is a top_level comment
                    #body = re.search('\"body\":\"(.+?)\"(,\")|(})', line).group(1)
                    comm = json.loads(line)
                    body = comm["body"] #these two options appear to the be the same speed.
                    subreddit = comm["subreddit"]
                    text_bodies[subreddit].append(body)

    return text_bodies

def load_language_model(subreddit, start_year, start_month, end_month, ngrams, text_min, text_max, base_path = "../data/subreddit_language_models/"):
    #TODO fix with args and such
    verify_subreddit_language_model([subreddit], start_year, start_month, end_month, ngrams, text_min, text_max, base_path = "../data/subreddit_language_models/" )
    # TODO: this file name thing is dumb
    file_name = "{}_{}_{}_{}_{}_{}_{}.pkl".format(subreddit, start_year, start_month, end_month, ngrams, text_min,
                                                  text_max)
    file_path = base_path + file_name
    lm = pickle.load(open(file_path, "rb"))
    return lm

def verify_subreddit_language_model(subreddit_list, start_year, start_month, end_month, ngrams, text_min, text_max, base_path = "../data/subreddit_language_models/"):
    """
    Search to see if you have a cached language model. If so, load into memory and return.
    If not, create the language model, cache it and return.
    :param subreddit: the subreddit
    :param date_range: the date range, as a datetime tuple (start, end)
    :return: None
    """
    new_subreddit_list = []
    for subreddit in subreddit_list:
        file_name = "{}_{}_{}_{}_{}_{}_{}.pkl".format(subreddit, start_year, start_month, end_month, ngrams, text_min, text_max)
        file_path = base_path + file_name

        if not os.path.isfile(file_path):
            print "looks like it doesn't exist, creating {} language model from scratch".format(subreddit)
            new_subreddit_list.append(subreddit)

    comment_list = get_relevant_text_bodies(new_subreddit_list, start_year, start_month, end_month)

    for subreddit in new_subreddit_list:
        #bad to have this twice
        file_name = "{}_{}_{}_{}_{}_{}_{}.pkl".format(subreddit, start_year, start_month, end_month, ngrams, text_min, text_max)
        file_path = base_path + file_name

        text_list = comment_list[subreddit]
        text = []
        for comm_text in text_list:
            res = preprocess_text(comm_text, text_max, text_min)
            if res:
                text.append(res)
        all_sents = [item for sublist in text for item in sublist]
        lm = create_language_model_nltk_everygrams(all_sents, ngrams=2)
        pickle.dump(lm, open(file_path, "wb"), protocol=pickle.HIGHEST_PROTOCOL)


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
    A first attempt at creating a language model. Likely very memory heavy and inefficient.
    :param subreddit:
    :param time_period:
    :return:
    """
    # see http://www.nltk.org/api/nltk.lm.html#module-nltk.lm.models
    train, vocab = padded_everygram_pipeline(ngrams, text)
    lm = KneserNeyInterpolated(ngrams)
    lm.fit(train, vocab)
    return lm

# TODO: is inverse of entropy a thing??
def text_similarity_nltk_everygrams(texts, lm):
    """
    Given a list of texts return a list of their inverse entropy
    :param texts: a list of strings, preprocessed as they were for the language model
    :param lm: the language model
    :return: a list of inverse entropys for each text
    """
    res = []
    for text in texts:
        tokens = list(flatten(pad_both_ends(sent, n=2) for sent in text))  # now get ngrams and bigrams, padded
        res.append(1/lm.entropy(tokens))

    return res

