"""
This file is where we pull in all the various features, essentially creating the values we will use for stats
"""
import sklearn
import language_model
from collections import defaultdict
import json
from sshtunnel import SSHTunnelForwarder
import pymongo
import atexit
import datetime
import pandas as pd
import numpy as np
import extract_pairs
import get_token_categories
from collections import Counter, defaultdict
import csv

def start_server():
    """
    Starts the mongo server and will shut it on exit
    :return:
    The comment and post cursor objects, can use to run queries.
    """

    MONGO_HOST = "132.206.3.193"
    MONGO_DB = "reddit"
    MONGO_USER = "carmst16"

    server = SSHTunnelForwarder(
        MONGO_HOST,
        ssh_pkey="/home/ndg/users/carmst16/.ssh/id_rsa",
        ssh_username=MONGO_USER,
        remote_bind_address = ("127.0.0.1", 27017)
    )

    server.start()

    client = pymongo.MongoClient("127.0.0.1", server.local_bind_port)
    db = client[MONGO_DB]

    dbcomments = db.comms
    dbposts = db.posts

    def exit_handler():
        server.stop()

    atexit.register(exit_handler)

    return dbcomments, dbposts

# TODO: store with HDFStore?  https://realpython.com/fast-flexible-pandas/
def load_dataframe(year, start_month, end_month, base_path, contribtype="comment"):
    """
    Loads the appropriate data for the given dates. Base path is where your data is stored
    Contribtype is either comments or posts.
    :param year:
    :param start_month:
    :param end_month:
    :param base_path:
    :param contribtype: Either string "comment" or string "post", returns appropriate data
    :return:
    """

    if "comment" in contribtype:
        base_path_full = base_path + "comment_metadata/"

        valid_file_paths = extract_pairs.list_file_appropriate_data_range(year,
                                                                  start_month,
                                                                  end_month,
                                                                  base_path_full,
                                                                )
    elif "post" in contribtype:
        base_path_full = base_path + "post_metadata/"

        valid_file_paths = extract_pairs.list_file_appropriate_data_range(year,
                                                                  start_month,
                                                                  end_month,
                                                                  base_path_full,
                                                                  posts=True)
    else:
        raise ValueError("Not a valid contribtype")

    print valid_file_paths

    if not valid_file_paths:
        raise ValueError("You haven't processed date ranges yet")

    big_frame = pd.concat(
        [pd.read_csv(f, sep=',', index_col=0, header=0,
                     dtype={"karma":np.int32}) for f in valid_file_paths])

    big_frame = clean_dataframe(big_frame)
    return big_frame

def clean_dataframe(df):
    """
    Cleans the dataframe.
    :param df:
    :return: the dataframe. done in place however.
    """
    #remove all columns with user="[deleted]"
    # in place maybe because I don't want to create another
    df = df[df.author != "[deleted]"]

    return df

def get_category_counts(pairs, relevant_categories):
    """
    Gets the relevant category counts for the given texts. Is reasonably efficient.
    :param texts: the texts we want to get category counts for
    :param relevant_categories: list of all the categories we care about
    :return: a list of 2 item lists of dictionaries, in the same order as the input texts,
    where the dictionaries contain the counts of each of the relevant categories
    """
    # TODO: deal with lemmatization??
    parse, category_names = get_token_categories.load_token_parser("../data/categories.dic")

    all_text_counts = []
    for pair in pairs:

        text1 = pair[0]["body"]
        text2 = pair[1]["body"]

        pair_category_counts = []

        for text in [text1, text2]:
            sents = language_model.preprocess_text(text, 0, 1000)
            tokens = [item for sublist in sents for item in sublist]
            counts = Counter(category for token in tokens for category in
                parse(token))
            category_counts = {cat: counts[cat] for cat in
                               relevant_categories}

            pair_category_counts.append(category_counts)

        all_text_counts.append(pair_category_counts)

    return all_text_counts

def get_pairs_user_interactions(pairs, df, subreddit):
    """
    Gets a list of all the interactions in the same order as the pairs passed
    :param pairs:
    :param df:
    :param subreddit:
    :return:
    """

    user_interactions = []
    for pair in pairs:

        user1 = pair[0]["author"]
        user2 = pair[1]["author"]

        res = get_user_interactions(user1, user2, df, subreddit)

        user_interactions.append(res)

    return user_interactions

# TODO: refactor below to use apply ??
# TODO: include replies to top-level posts?
def get_user_interactions(user1, user2, df, subreddit=None):
    """
    Gets the number of times two users have interacted in
    :param user1:
    :param user2:
    :param df:
    :param subreddit: If value, then interactions only on that subreddit will be included.
    Otherwise will integrate all data from that date range.
    :return:
    """

    if not subreddit:
        res = df.loc[((df['author'] == user1) & (df['parent_author'] == user2)) |
           ((df['author'] == user2) & (df['parent_author'] == user1))]

    if subreddit:
        res = df.loc[((df['author'] == user1) & (df['parent_author'] == user2) & (df['subreddit'] == subreddit)) |
               ((df['author'] == user2) & (df['parent_author'] == user1) & (df['subreddit'] == subreddit))]

    return res.shape[0]


def get_pairs_user_prolificness(pairs, comment_df, post_df, subreddit):
    """
    Get user prolificness for the pairs list in the same order
    :param pairs:
    :param comment_df:
    :param post_df:
    :param subreddit:
    :return:
    """

    user_prolificness = []
    for pair in pairs:

        user1 = pair[0]["author"]
        user2 = pair[1]["author"]

        pair_prolificness = []
        for user in [user1, user2]:

            res = get_user_prolificness(user, comment_df, post_df, subreddit)

            pair_prolificness.append(res)

        user_prolificness.append(pair_prolificness)

    return user_prolificness


def get_user_prolificness(user, comment_df, post_df, subreddit):
    """
    Get user prolificness for the data given. Sum of number of comments and posts.
    If subreddit given will only include data for that subreddit.
    :param user:
    :param comment_df:
    :param post_df:
    :param subreddit:
    :return:
    """
    if not subreddit:
        res_comm = comment_df.loc[(comment_df['author'] == user)]
        res_post = post_df.loc[(post_df['author'] == user)]

    if subreddit:
        res_comm = comment_df.loc[(comment_df['author'] == user) & (comment_df['subreddit'] == subreddit)]
        res_post = post_df.loc[(post_df['author'] == user) & (post_df['subreddit'] == subreddit)]

    return res_comm.shape[0] + res_post.shape[0]


def get_pairs_user_karma(pairs, comment_df, post_df, subreddit):
    """

    :param pairs:
    :param comment_df:
    :param post_df:
    :param subreddit:
    :return:
    """


    user_karma = []
    for pair in pairs:

        user1 = pair[0]["author"]
        user2 = pair[1]["author"]

        pair_karma = []
        for user in [user1, user2]:
            res = get_user_prolificness(user, comment_df, post_df, subreddit)

            pair_karma.append(res)

        user_karma.append(pair_karma)

    return user_karma

# TODO: double check logic here.
def get_user_karma(user, comment_df, post_df, subreddit):
    """
    Get user karma for the given data. If subreddit given get data only for that subreddit
    Sum of comment and post karma
    :param user:
    :param comment_df:
    :param post_df:
    :param subreddit:
    :return:
    """

    if not subreddit:
        comment_karma = comment_df.loc[(comment_df['author'] == user)]['karma'].sum()
        post_karma = post_df.loc[(post_df['author'] == user)]['karma'].sum()
    else:
        comment_karma = comment_df.loc[(comment_df['author'] == user) & (comment_df['subreddit'] == subreddit)]['karma'].sum()
        post_karma = post_df.loc[(post_df['author'] == user) & (post_df['subreddit'] == subreddit) ]['karma'].sum()

    return comment_karma, post_karma

# TODO: test this.
def load_pairs(base_path, year, start_month, end_month, subreddits):
    """
    Loads the parent child pairs found in the given file
    :param file_path:
    :return: The pairs organized by subreddit
    """

    base_path_full = base_path + "non_top_level_comments/"
    valid_file_paths = extract_pairs.list_file_appropriate_data_range(year,
                                                        start_month, end_month,
                                                        base_path_full)

    pairs = defaultdict(lambda: [])

    for file_path in valid_file_paths:
        with open(file_path, "rb") as f:
            for line in f:
                line = line.split("} {") #TODO: fix. saved file wrong
                parent = json.loads(line[0]+ "}")
                child = json.loads("{" + line[1])

                subreddit = parent["subreddit"]
                if subreddit not in subreddits:
                    continue

                pairs[subreddit].append((parent, child))

    return pairs



def get_language_model_match(pairs_list, lm):
    """
    Given a list of pairs, get the language model similarity score for the parent in the pair. Parent always comes first.
    :param pairs_list: List of tuples of json objects
    :return: A list of len(pairs_list)
    """

    all_text = []
    for pair in pairs_list:
        p = pair[0] # TODO: why aren't we doing this for both??
        all_text.append(p["body"])

    res = language_model.text_similarity_nltk_everygrams(all_text, lm)

    return res

def create_query(author, time_period, subreddit):
    """
    Creates a mongo query given the params
    :param author:
    :param time_period:
    :param subreddit:
    :return:
    """

    query_dict = {}
    query_dict['author'] = author

    if time_period:
        start_year, start_month, end_month = time_period
        start = datetime.datetime(start_year, start_month, 1)
        end = datetime.datetime(start_year, end_month, 1)

        query_dict['created_time'] = {'$gte': start, '$lt': end}

    if subreddit:
        query_dict['subreddit'] = subreddit

    return query_dict

#deprecated
def get_prior_interactions(user1, user2, time_period=None, subreddit=None):
    """
    Get the number of times the given users have interacted. If given a time period and/or subreddit, restrict
    the search to those parameters
    :param user1: user1
    :param user2: user2
    :param time_period: tuple of start year, start month, end month
    :param subreddit: the subreddit
    :return: a number
    """
    dbcomments, _ = start_server()
    prior_interactions = 0
    user_list = [user1, user2]
    for i in range(len(user_list)):
        if i == 1:
            i_r = 0  #this needs help..
        else:
            i_r = 1
        query = create_query(user_list[i], time_period, subreddit)
        return_fields = {"parent_id":1}
        poss = dbcomments.find(query, return_fields)
        for comm in poss:
            parent_id = comm["parent_id"]
            if "t1_" in parent_id:
                comm = dbcomments.find_one({"_id": parent_id[3:]}, {"author": 1})
                if comm["author"] in user_list[i_r]:
                    prior_interactions += 1
    return prior_interactions



def write_to_csv(subreddits, year, start_month, end_month, ngrams, text_min,
                 text_max, base_path, relevant_categories, out_file,
                 user_prolificness_subreddit, user_karma_subreddit,
                 prior_interaction_subreddit):

    comment_df = load_dataframe(year, start_month, end_month,
                                             base_path)
    post_df = load_dataframe(year, start_month, end_month,
                                          base_path, contribtype="post")

    pairs = load_pairs(base_path, year, start_month, end_month,
                                    subreddits)


    language_model.create_subreddit_language_models(subreddits, year,
                                                    start_month, end_month,
                                                    ngrams, text_min, text_max,
                                                    base_path)

    # TODO: check that parent is always in the right pace
    categories_per_comment_header = []

    # so at the end we have all of your
    for category in relevant_categories:
        categories_per_comment_header.append(category + "_parent")
        categories_per_comment_header.append(category + "_child")

    csv_header_values = ["subreddit",
                         "parent_id",
                         "child_id",
                         "parent_lm",
                         "child_lm",
                         "parent_user_proflificness",
                         "child_user_prolificness",
                         "parent_user_karma",
                         "child_user_karma",
                         "pair_prior_interactions"
                         ]

    csv_header_values = csv_header_values + categories_per_comment_header

    with open(out_file, "wb") as csvfile:

        cwriter = csv.writer(csvfile)

        cwriter.write(csv_header_values)

        for subreddit in subreddits:

            lm = language_model.load_language_model(subreddit, year,
                                                    start_month, end_month,
                                                    ngrams, text_min,
                                                    text_max, base_path)

            subreddit_pairs = pairs[subreddit]

            # next 3 below values are a list of tuples

            pairs_entropy_values = get_language_model_match(subreddit_pairs, lm)

            pairs_user_prolificness = get_pairs_user_prolificness(
                subreddit_pairs, comment_df, post_df,
                user_prolificness_subreddit
            )

            pairs_user_karma = get_pairs_user_karma(
                subreddit_pairs, comment_df, post_df, user_karma_subreddit
            )

            # just a simple list
            pairs_prior_interactions = get_pairs_user_interactions(subreddit_pairs,
                                            comment_df,
                                            prior_interaction_subreddit)

            # list of lists of dictionaries
            pairs_category_counts = get_category_counts(
                subreddit_pairs, relevant_categories)

            # TODO: this could be more efficiently done in the function

            for i in range(len(subreddit_pairs)):
                pair = subreddit_pairs[i]  # TODO: verify or index better
                category_values = []
                for category in relevant_categories:
                    category_values.append(
                        pairs_category_counts[i][0][category])
                    category_values.append(
                        pairs_category_counts[i][1][category])

                # TODO: this is error city
                values = [subreddit,
                          pair[0]["id"],
                          pair[1]["id"],
                          pairs_entropy_values[i][0],
                          pairs_entropy_values[i][1],
                          pairs_user_prolificness[i][0],
                          pairs_user_prolificness[i][1],
                          pairs_user_karma[i][0],
                          pairs_user_karma[i][0],
                          pairs_prior_interactions[i]
                          ]

                values = values + category_values

                cwriter.write([values])