# coding=utf-8
"""
Describes everything that is done to process data. No modifications should be made to any methods.
"""
import os
import language_model
import get_features
import csv

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

# dec 15 test
def test_create_language_model_new_scheme():
    subreddits = ["mcgill"]
    year = 2016
    start_month = 1
    end_month = 1
    ngrams = 4
    text_min = 10
    text_max = 1000
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"
    language_model.create_subreddit_language_models(subreddits, year,
                                                    start_month, end_month,
                                                    ngrams, text_min, text_max,
                                                    base_path)

    lm = language_model.load_language_model(subreddits[0], year,
                                                    start_month, end_month,
                                                    ngrams, text_min, text_max,
                                                    base_path)

    print(lm.generate(num_words=100, text_seed="mcgill"))

    texts = ["what is life", "As if anyone claimed it would end the occupation. It was always intended as a symbolic gesture: to not stand in support of oppression.", "this is me at a bank"]
    res = language_model.text_similarity_nltk_everygrams(texts, lm, ngrams, text_min, text_max)
    print res

def test_load_dataframe():
    year = 2016
    start_month = 1
    end_month = 2
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"

    comment_df = get_features.load_dataframe(year, start_month, end_month, base_path)
    comment_df.info(verbose=True, memory_usage=True)
    comment_df = get_features.clean_dataframe(comment_df)
    comment_df.info(verbose=True, memory_usage=True)
    num_prior_interactions = get_features.get_user_interactions("AristocratPhoto", "aresef", comment_df)
    print num_prior_interactions
    post_df = get_features.load_dataframe(year, start_month, end_month, base_path, contribtype="post")

    post_df.info(verbose=True, memory_usage=True)
    karma = get_features.get_user_karma("aresef", comment_df, post_df)
    print karma, "KARMA"
    prolif = get_features.get_user_prolificness("aresef", comment_df, post_df)
    print prolif, "PROLIF"


# TODO: need to decide on all features from LIWC 2015
# TODO: need to make a file name creation function
def get_all_values_jan_4():
    """Defines all parameters for the entire experiment"""
    subreddits = ["mcgill"]

    year = 2016
    start_month = 1
    end_month = 2

    ngrams = 4
    text_min = 10 # TODO: make sure this is being used in all the right places
    text_max = 1000

    # TODO: are these truly the values that you want?
    # values that define if you restrict value calculation to just a certain
    # subreddit
    prior_interaction_subreddit = None
    user_prolificness_subreddit = None
    user_karma_subreddit = None

    relevant_categories = [] # TODO: fill this out
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"

    out_file = "/home/ndg/projects/shared_datasets/reddit-style/data/get_all_values_jan_4.csv"

    get_features.write_to_csv(subreddits, year, start_month, end_month, ngrams, text_min,
                              text_max, base_path, relevant_categories, out_file,
                              user_prolificness_subreddit, user_karma_subreddit,
                              prior_interaction_subreddit)


if __name__ == "__main__":
    test_create_language_model_new_scheme()
