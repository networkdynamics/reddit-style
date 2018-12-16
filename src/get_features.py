"""
This file is where we pull in all the various features, essentially creating the values we will use for stats
"""
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


def get_user_prolificness(user, comment_df, post_df, subreddit=None):
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

# TODO: double check logic here.
def get_user_karma(user, comment_df, post_df, subreddit=None):
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


def load_pairs(file_path):
    """
    Loads the parent child pairs found in the given file
    :param file_path:
    :return: The pairs organized by subreddit
    """
    pairs = defaultdict(lambda: [])
    with open(file_path, "rb") as f:
        for line in f:
            line = line.split("} {") #TODO: fix. saved file wrong
            parent = json.loads(line[0]+ "}")
            child = json.loads("{" + line[1])

            subreddit = parent["subreddit"]

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
        p = pair[0]
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