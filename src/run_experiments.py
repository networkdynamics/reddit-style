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

    #"ls -d -1 /home/ndg/arc/reddit/2016/RC_2016-0*.gz | parallel -j20 --pipe parallel -j100 --no-notice python extract_pairs.py"


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
    text_min = 0
    text_max = 100000
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

def test_create_language_model_kenlm():




    subreddits = ["mcgill"]
    year = 2016
    start_month = 1
    end_month = 1
    ngrams = 5
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


    texts = ["what is life", "As if anyone claimed it would end the occupation. It was always intended as a symbolic gesture: to not stand in support of oppression.", "this is me at a bank"]
    res = language_model.text_scores(texts, lm, text_min, text_max)
    print res


def create_many_language_models():

    with open("../data/cohesion_subs.txt") as f:
        content = f.readlines()
    content = [x.strip() for x in content]

    subreddits = content
    year = 2016
    start_month = 1
    end_month = 4
    ngrams = 5
    text_min = 0
    text_max = 10000
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"
    language_model.create_subreddit_language_models(subreddits, year,
                                                    start_month, end_month,
                                                    ngrams, text_min, text_max,
                                                    base_path)

# TODO: need to decide on all features from LIWC 2015
# TODO: need to make a file name creation function
def get_all_values_jan_22():
    """Defines all parameters for the entire experiment"""
    with open("../data/large_subs.txt") as f:
        content = f.readlines()
    content = [x.strip() for x in content]

    subreddits = content[:100]

    year = 2016

    start_month_pairs = 4
    end_month_pairs = 4

    start_month_metadata = 1
    end_month_metadata = 4

    ngrams = 5
    text_min = 0 # TODO: make sure this is being used in all the right places
    text_max = 10000

    num_pairs_cap = 10000
    num_pairs_min = 1000

    # TODO: are these truly the values that you want?
    # values that define if you restrict value calculation to just a certain
    # subreddit

    restrict_to_subreddit_only = False

    relevant_categories = ["ppron", "i", "we", "you", "shehe", "they" "ipron",
                           "article", "prep", "auxverb",
                           "conj", "negate", "verb", "adj", "compare",
                           "interrog", "number", "quant", "posemo", "negemo",
                           "anx", "anger", "sad"]
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"

    out_file = "/home/ndg/projects/shared_datasets/reddit-style/output_data/large_subs_get_all_values_jan_23_{}_{}_{}_{}_{}_{}.csv".format(year, start_month_pairs, end_month_pairs, ngrams, text_min, text_max)

    # language_model.create_subreddit_language_models(subreddits, year,
    #                                                 start_month_pairs, end_month_pairs,
    #                                                 ngrams, text_min, text_max,
    #                                                 base_path)
    get_features.write_to_csv(subreddits, year, start_month_pairs, end_month_pairs, start_month_metadata, end_month_metadata, ngrams, text_min,
                              text_max, base_path, relevant_categories, out_file, restrict_to_subreddit_only, num_pairs_cap, num_pairs_min)

import random
def partition (list_in, n):
    random.shuffle(list_in)
    return [list_in[i * n:(i + 1) * n] for i in
             range((len(list_in) + n - 1) // n)]

def get_all_values_test_subs_jan_27():
    """Defines all parameters for the entire experiment"""
    with open("../data/hostile_subs.txt") as f:
        content = f.readlines()
    content = [x.strip() for x in content]

#    subreddits = content[:3]

    partitioned_subreddits = [["penguins"]]

    year = 2016

    start_month_pairs = 4
    end_month_pairs = 4

    num_months_back = 1

    ngrams = 5
    text_min = 5 # TODO: make sure this is being used in all the right places
    text_max = 10000

    num_pairs_cap = 5000
    num_pairs_min = 5

    # TODO: are these truly the values that you want?
    # values that define if you restrict value calculation to just a certain
    # subreddit

    restrict_to_subreddit_only = False

    relevant_categories = ["ppron", "i", "we", "you", "shehe", "they" "ipron",
                           "article", "prep", "auxverb",
                           "conj", "negate", "verb", "adj", "compare",
                           "interrog", "number", "quant", "posemo", "negemo",
                           "anx", "anger", "sad"]
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"

    out_file = "/home/ndg/projects/shared_datasets/reddit-style/output_data/TEST_penguin_subs_get_all_values_jan_27_{}_{}_{}_{}_{}_{}.csv".format(year, start_month_pairs, end_month_pairs - num_months_back, ngrams, text_min, text_max)

    # language_model.create_subreddit_language_models(subreddits, year,
    #                                                 start_month_pairs, end_month_pairs,
    #                                                 ngrams, text_min, text_max,
    #                                                 base_path)
    for subreddits in partitioned_subreddits:
        print subreddits
        get_features.write_to_csv(subreddits, year, start_month_pairs, end_month_pairs, ngrams, text_min,
                              text_max, base_path, relevant_categories, out_file, restrict_to_subreddit_only, num_pairs_cap, num_pairs_min, num_months_back)


def get_all_values_hostile_subs_jan_27():
    """Defines all parameters for the entire experiment"""
    with open("../data/hostile_subs.txt") as f:
        content = f.readlines()
    content = [x.strip() for x in content]

#    subreddits = content[:3]

    partitioned_subreddits = partition(content, 10)

    year = 2016

    start_month_pairs = 4
    end_month_pairs = 4

    num_months_back = 1

    ngrams = 5
    text_min = 5 # TODO: make sure this is being used in all the right places
    text_max = 10000

    num_pairs_cap = 5000
    num_pairs_min = 100

    # TODO: are these truly the values that you want?
    # values that define if you restrict value calculation to just a certain
    # subreddit

    restrict_to_subreddit_only = False

    relevant_categories = ["ppron", "i", "we", "you", "shehe", "they" "ipron",
                           "article", "prep", "auxverb",
                           "conj", "negate", "verb", "adj", "compare",
                           "interrog", "number", "quant", "posemo", "negemo",
                           "anx", "anger", "sad"]
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"

    for i in range(len(partitioned_subreddits)):
        subreddits = partitioned_subreddits[i]
        print subreddits
        out_file = "/home/ndg/projects/shared_datasets/reddit-style" \
                   "/output_data/hostile_subs_{}_get_all_values_jan_27_{}_{" \
                   "}_{" \
                   "}_{}_{}_{}.csv".format(i,
                                              year, start_month_pairs,
                                              end_month_pairs -
                                              num_months_back,
                                              ngrams,
                                              text_min, text_max)

        get_features.write_to_csv(subreddits, year, start_month_pairs,
                                  end_month_pairs, ngrams, text_min,
                                  text_max, base_path, relevant_categories,
                                  out_file, restrict_to_subreddit_only,
                                  num_pairs_cap, num_pairs_min,
                                  num_months_back)


def get_all_values_cohesion_subs_27():
    """Defines all parameters for the entire experiment"""
    with open("../data/cohesion_subs.txt") as f:
        content = f.readlines()
    content = [x.strip() for x in content]

#    subreddits = content[:3]

    partitioned_subreddits = partition(content, 10)

    year = 2016

    start_month_pairs = 4
    end_month_pairs = 4

    num_months_back = 1

    ngrams = 5
    text_min = 5 # TODO: make sure this is being used in all the right places
    text_max = 10000

    num_pairs_cap = 5000
    num_pairs_min = 100

    # TODO: are these truly the values that you want?
    # values that define if you restrict value calculation to just a certain
    # subreddit

    restrict_to_subreddit_only = False

    relevant_categories = ["ppron", "i", "we", "you", "shehe", "they" "ipron",
                           "article", "prep", "auxverb",
                           "conj", "negate", "verb", "adj", "compare",
                           "interrog", "number", "quant", "posemo", "negemo",
                           "anx", "anger", "sad"]
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"

    for i in range(len(partitioned_subreddits)):
        subreddits = partitioned_subreddits[i]
        print subreddits
        out_file = "/home/ndg/projects/shared_datasets/reddit-style" \
                   "/output_data/cohesion_subs_{}_get_all_values_jan_27_{}_{" \
                   "}_{" \
                   "}_{}_{}_{}.csv".format(i,
                                              year, start_month_pairs,
                                              end_month_pairs -
                                              num_months_back,
                                              ngrams,
                                              text_min, text_max)

        get_features.write_to_csv(subreddits, year, start_month_pairs,
                                  end_month_pairs, ngrams, text_min,
                                  text_max, base_path, relevant_categories,
                                  out_file, restrict_to_subreddit_only,
                                  num_pairs_cap, num_pairs_min,
                                  num_months_back)


def get_all_values_large_subs_27():
    """Defines all parameters for the entire experiment"""
    with open("../data/large_subs.txt") as f:
        content = f.readlines()
    content = [x.strip() for x in content]

#    subreddits = content[:3]

    partitioned_subreddits = partition(content, 10)

    year = 2016

    start_month_pairs = 4
    end_month_pairs = 4

    num_months_back = 1

    ngrams = 5
    text_min = 5 # TODO: make sure this is being used in all the right places
    text_max = 10000

    num_pairs_cap = 5000
    num_pairs_min = 100

    # TODO: are these truly the values that you want?
    # values that define if you restrict value calculation to just a certain
    # subreddit

    restrict_to_subreddit_only = False

    relevant_categories = ["ppron", "i", "we", "you", "shehe", "they" "ipron",
                           "article", "prep", "auxverb",
                           "conj", "negate", "verb", "adj", "compare",
                           "interrog", "number", "quant", "posemo", "negemo",
                           "anx", "anger", "sad"]
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"

    for i in range(len(partitioned_subreddits)):
        subreddits = partitioned_subreddits[i]
        print subreddits
        out_file = "/home/ndg/projects/shared_datasets/reddit-style" \
                   "/output_data/large_subs_{}_get_all_values_jan_27_{}_{" \
                   "}_{" \
                   "}_{}_{}_{}.csv".format(i,
                                              year, start_month_pairs,
                                              end_month_pairs -
                                              num_months_back,
                                              ngrams,
                                              text_min, text_max)

        get_features.write_to_csv(subreddits, year, start_month_pairs,
                                  end_month_pairs, ngrams, text_min,
                                  text_max, base_path, relevant_categories,
                                  out_file, restrict_to_subreddit_only,
                                  num_pairs_cap, num_pairs_min,
                                  num_months_back)


def get_all_values_support_subs_27():
    """Defines all parameters for the entire experiment"""
    with open("../data/support_subs.txt") as f:
        content = f.readlines()
    content = [x.strip() for x in content]

#    subreddits = content[:3]

    partitioned_subreddits = partition(content, 10)

    year = 2016

    start_month_pairs = 4
    end_month_pairs = 4

    num_months_back = 1

    ngrams = 5
    text_min = 5 # TODO: make sure this is being used in all the right places
    text_max = 10000

    num_pairs_cap = 5000
    num_pairs_min = 100

    # TODO: are these truly the values that you want?
    # values that define if you restrict value calculation to just a certain
    # subreddit

    restrict_to_subreddit_only = False

    relevant_categories = ["ppron", "i", "we", "you", "shehe", "they" "ipron",
                           "article", "prep", "auxverb",
                           "conj", "negate", "verb", "adj", "compare",
                           "interrog", "number", "quant", "posemo", "negemo",
                           "anx", "anger", "sad"]
    base_path = "/home/ndg/projects/shared_datasets/reddit-style/"


    # language_model.create_subreddit_language_models(subreddits, year,
    #                                                 start_month_pairs, end_month_pairs,
    #                                                 ngrams, text_min, text_max,
    #                                                 base_path)
    for i in range(len(partitioned_subreddits)):
        subreddits = partitioned_subreddits[i]
        print subreddits
        out_file = "/home/ndg/projects/shared_datasets/reddit-style" \
                   "/output_data/support_subs_{}_get_all_values_jan_27_{}_{}_{" \
                   "}_{}_{}_{}.csv".format(i,
            year, start_month_pairs, end_month_pairs - num_months_back, ngrams,
            text_min, text_max)

        get_features.write_to_csv(subreddits, year, start_month_pairs, end_month_pairs, ngrams, text_min,
                              text_max, base_path, relevant_categories, out_file, restrict_to_subreddit_only, num_pairs_cap, num_pairs_min, num_months_back)

if __name__ == "__main__":
    get_all_values_hostile_subs_jan_27()
