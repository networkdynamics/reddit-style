# coding=utf-8
"""
Describes everything that is done to process data. No modifications should be made to any methods.
"""
import os
import language_model
import get_features

#THE FUNCTION HAS SINCE BEEN CHANGED
def get_2015_pairs_duplicates_included():

    data_dir = "/home/ndg/users/carmst16/reddit-style/data/2015_duplicates"
    include_duplicates = True

    #I ran the following

    # ls - d - 1 / home / ndg / arc / reddit / 2015 / *.gz | parallel - j20 - -pipe
    # parallel - j100 - -no - notice
    # python
    # extract_pairs.py > ../data/2015_duplicates/all_pairs_2015.txt

#RAN ON COMMAND LINE
#dec 14 2018
def get_all_jan_2016_data():
    pass

    #I ran the following

    #"ls -d -1 /home/ndg/arc/reddit/2016/RC_2016-01-*.gz | parallel -j20 --pipe parallel -j100 --no-notice python extract_pairs.py"


def testing_language_model_match():
    #subreddit_language_model(["AskReddit", "McGill"], 2015, 1, 2, 2, 0, 50)

    #lm = language_model.load_language_model(subreddit, start_year, start_month, end_month, ngrams, text_min, text_max)
    d = get_features.load_pairs("../data/2015_duplicates/all_pairs_2015.txt")

def test_get_prior_interaction():

    prior_interactions = get_features.get_prior_interactions("AJinxyCat", "edcsociety")
    print prior_interactions
if __name__ == "__main__":
    test_get_prior_interaction()
