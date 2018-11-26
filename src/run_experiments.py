# coding=utf-8
"""
Describes everything that is done to process data. No modifications should be made to any methods.
"""
import os

def get_2015_pairs_duplicates_included():

    data_dir = "/home/ndg/users/carmst16/reddit-style/data/2015_duplicates"
    include_duplicates = True

    #I ran the following

    # ls - d - 1 / home / ndg / arc / reddit / 2015 / *.gz | parallel - j20 - -pipe
    # parallel - j100 - -no - notice
    # python
    # extract_pairs.py > ../data/2015_duplicates/all_pairs_2015.txt


if __name__ == "__main__":
    from language_model import subreddit_language_model

    lm = subreddit_language_model(["AskReddit", "McGill"], 2015, 1, 2, 2, 0, 50)
