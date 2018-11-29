"""
This file is where we pull in all the various features, essentially creating the values we will use for stats
"""
import language_model
import csv
from collections import defaultdict
import json
from sshtunnel import SSHTunnelForwarder
import pymongo
import pprint
import atexit
import datetime

def start_server():

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