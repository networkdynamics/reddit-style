# coding=utf-8
"""
This file defines all methods used to process data before we pass it through the methods.
"""
from recordclass import recordclass
import numpy as np
import pickle
import os
from pymongo import MongoClient
import re
# returns list of tuples, with restrictions
'''
    The output is a dictionary. Key is the subreddit-name; Value is a dictionary itself.
    That inner dictionary's Key is a ('user1', 'user2') tuple; Value is a list lists (basically a list of threads in that subreddit.) The inner list always has two elements/strings in it.
'''

# TODO: not great to have dict as default value but like whatever
def get_data_mongo(subreddit, daterange, cached_data_path,
                   fields={"created_time": 1, "body": 1, "parent_id": 1, "link_id": 1, "author": 1, "score": 1}):
    start = daterange[0]
    end = daterange[1]
    client = MongoClient(serverSelectionTimeoutMS=30, connectTimeoutMS=20000)
    db = client.reddit
    comments = db.comms

    all_data_file_path = cached_data_path + subreddit + "_" + start.strftime("%B%d_%Y") + "_" + \
                         end.strftime("%B%d_%Y") + ".pickle"

    # TODO: This could be really big
    if os.path.exists(all_data_file_path):
        print "Yay, we have it."
        indexed_subreddit_data = pickle.load(open(all_data_file_path, "r"))

    else:
        print "Didn't already have it :("
        subreddit_data = list(comments.find({"subreddit": subreddit, 'created_time': {'$gte': start, '$lt': end}},
                                                 fields))
        indexed_subreddit_data = dict()
        for comment in subreddit_data:
            indexed_subreddit_data[comment["_id"]] = comment

        pickle.dump(indexed_subreddit_data, open(all_data_file_path, "w"))
    return indexed_subreddit_data


def get_all_data_mongo(subreddit_list, daterange, cached_data_path,
                       fields={"created_time": 1, "body": 1, "parent_id": 1, "link_id": 1, "author": 1, "score": 1}):
    for subreddit in subreddit_list:
        get_data_mongo(subreddit, daterange, cached_data_path, fields)


def create_userpair_tuples(indexed_data, max_pairs, min_convo_length=2, min_string_length=None, remove_deleted_users=True):

    all_basic_comment_tuples = {}

    total_so_far = 0

    all_link_ids = dict()

    for comment in indexed_data:

        parent_id = indexed_data[comment]["parent_id"][3:]
        link_id = indexed_data[comment]["link_id"][3:]
        id = indexed_data[comment]["_id"]

        if link_id not in all_link_ids:
            all_link_ids[link_id] = list()

        all_link_ids[link_id].append((id, parent_id))

    comment_tuples = dict()
    for link in all_link_ids:
        if total_so_far > max_pairs:
            break
        for paren_child in all_link_ids[link]:
            if total_so_far > max_pairs:
                break
            id, parent_id = paren_child
            if (id in indexed_data) and (parent_id in indexed_data):

                if min_string_length:
                    if len(indexed_data[id]["body"]) < min_string_length:
                        continue
                    if len(indexed_data[parent_id]["body"]) < min_string_length:
                        continue

                par = indexed_data[parent_id]["author"]
                chil = indexed_data[id]["author"]
                par = par.encode('ascii', 'ignore')
                chil = chil.encode('ascii', 'ignore')

                # Deleted users filter:
                if remove_deleted_users:
                    if ("deleted" in par) or ("deleted" in chil):
                        continue

                    if par in chil:
                        continue

                if (par, chil) not in comment_tuples:
                    comment_tuples[(par, chil)] = list()

                parent_body = indexed_data[parent_id]["body"]
                child_body = indexed_data[id]["body"]

                regex = re.compile('[^a-zA-Z]')
                if parent_body is not "" and child_body is not "":
                    parent_body = parent_body.encode('utf-8').strip()
                    child_body = child_body.encode('utf-8').strip()
                    parent_body = regex.sub(' ', parent_body)
                    child_body = regex.sub(' ', child_body)
                    comment_tuples[(par, chil)].append([str(parent_body), str(child_body)])
                    total_so_far += 1

    for interac in comment_tuples:
        if len(comment_tuples[interac]) > min_convo_length:
            all_basic_comment_tuples[interac] = comment_tuples[interac]

    return all_basic_comment_tuples


def all_create_userpair_tuples(all_raw_data):
    for s in all_raw_data:
        create_userpair_tuples(all_raw_data[s])

def write_txt_accommodation(interactions, subreddit, file_path):
    """
    For Accommodation: Writes the pre-LIWC txt files for ONE subreddit to the directory specified in 'file_path'
    Naming format: subredditName_ConversationIndex_userID_IndexWithinThatConversation_.txt
        userID is to identify a user
        ConversationIndex is to indentify a thread
        Index is to identify the comment within a thread
        subredditName is to identify the subreddit
            
    Parameters
    ----------
        interactions: dictionary mapping userpair to list of interactions for a subreddit
        file_path: directory path where to write the txt(s). This should include the 'name' of the experiment
        subreddit: name of the subreddit
    """
    
    conv_index = 0
    
    for user_pair, conversation in interactions.items():
        person1 = user_pair[0]
        person2 = user_pair[1]
        
        if person1 in person2:
            continue
    
        for tup in conversation:
            index = conversation.index(tup)
            with open(file_path + str(subreddit) + '_' + str(conv_index) + '_' + str(person1) + '_' + str(index) + '_.txt', 'wb') as f1:
                f1.write(tup[0])
            with open(file_path + str(subreddit) + '_' + str(conv_index) + '_' + str(person2) + '_' + str(index) + '_.txt', 'wb') as f2:
                f2.write(tup[1])
        
        conv_index += 1




def write_all_accommodation(all_interactions, subreddit_list, file_path):
    """
    For Accommodation: Writes the pre-LIWC txt files for ALL subreddits to the directory specified in 'file_path'
        
    Parameters
    ----------
    all_interactions: dictionary mapping subreddits to {userpair: [list of interactions]}
    file_path: directory path where to write the txt(s). This should include the 'name' of the experiment
    subreddit_list: list of all subreddits
    """
    
    for subreddit in subreddit_list:
        print "Writing accommodation TXTs for: ", subreddit
        write_to_txt_accommodation(all_interactions[subreddit], file_path, subreddit)




def write_txt_cohesion(random_interactions, subreddit, file_path):
    """
    For Cohesion: Writes the pre-LIWC txt files for ONE subreddit to the directory specified in 'file_path'
    
    Parameters
    ----------
    random_interactions: list of interactions
    file_path: directory path where to write the txt(s). This should include the 'name' of the experiment
    subreddit: subreddit name
    """
    for tup in random_interactions:
        conv_index = turns.index(tup)
        
        with open(filepath+str(subreddit)+str(conv_index)+'_person_1.txt', 'wb') as f1:
            f1.write(tup[0])
        
        with open(filepath+str(subreddit)+str(conv_index)+'_person_2.txt', 'wb') as f2:
            f2.write(tup[1])




def write_all_cohesion(all_random_interactions, subreddit_list, file_path):
    """
    For Cohesion: Writes the pre-LIWC txt files for ALL subreddits to the directory specified in 'file_path'
    
    Parameters
    ----------
    all_random_interactions: dictionary mapping subreddits to [list of interactions]
    file_path: directory path where to write the txt(s). This should include the 'name' of the experiment
    subreddit_list: list of all subreddits
    """
    
    for subreddit in subreddit_list:
        print "Writing cohesion TXTs for: ", subreddit
        write_txt_cohesion(all_random_interactions[subreddit], file_path, subreddit)


# thanks to https://stackoverflow.com/questions/34964878/python-generate-a-dictionarytree-from-a-list-of-tuples
def order_comment_threads(list_of_parent_child_tuples):
    """Given a list of (id, parentid) tuples, generate a nested dictionary tree of form parent: children
    :param: list_of_parent_child_tuples a list of child, parent tuples in the form of IDs
    :return: a dictionary of nested parent child relationships
    """

    a = list_of_parent_child_tuples
    print a
    nodes = {}
    for i in a:
        child_id, parent_id = i
        nodes[child_id] = {'id': child_id}

    # pass 2: create trees and parent-child relations
    forest = []
    for i in a:
        print "hi"
        print nodes
        child_id, parent_id = i
        node = nodes[child_id]

        # either make the node a new tree or link it to its parent
        if child_id == parent_id:
            # start a new tree in the forest
            forest.append(node)
        else:
            # add new_node as child to parent
            try:
                parent = nodes[parent_id]
            except:
                continue
            if not 'children' in parent:
                # ensure parent has a 'children' field
                parent['children'] = []
            children = parent['children']
            children.append(node)
    return nodes


def get_user_karma(user, cur_date):
    """
    Gets the user's comment karma up to the given date.
    :param user: username
    :param cur_date: the given date
    :return: the karma as an integer
    """
    pass


# TODO: get rid of threads??
# TODO: need to add average karma per bin
def bin_data_on_karma(indexed_subreddit_data):
    """
    Given a dictionary of subreddit data indexed on comment id, bin comment-reply pairs based on their comment and reply scores
    :param indexed_subreddit_data:
    :return: a dictionary of key (comment_karma, reply_karma) and value list of (comment_text, reply) tuples
    """

    ids_touched = []  # this keeps track of all ids used in creating our binned data. we don't want to use somethign twice

    # creating a recordclass object - a mutable named tuple
    comment_reply = recordclass("CommentReply", ["comment_text", "reply_text", "comment_karma", "reply_karma"])
    unbinned_data = []

    for reply_id in indexed_subreddit_data:
        if reply_id in ids_touched:
            continue
        comment_id = indexed_subreddit_data[reply_id]["parent_id"]
        if comment_id not in indexed_subreddit_data:
            continue

        reply = indexed_subreddit_data[reply_id]
        comment = indexed_subreddit_data[comment_id]

        reply_karma = get_user_karma(reply["username"], reply["created_time"])
        comment_karma = get_user_karma(comment["username"], comment["created_time"])

        reply_text = reply["body"]
        comment_text = comment["body"]

        unbinned_data.append(comment_reply(comment_text, reply_text, comment_karma, reply_karma))

    all_comment_karma = [x.comment_karma for x in unbinned_data]
    all_reply_karma = [x.reply_karma for x in unbinned_data]

    max_comment_karma = max(all_comment_karma)
    min_comment_karma = min(all_comment_karma)

    max_reply_karma = max(all_comment_karma)
    min_reply_karma = min(all_reply_karma)

    bins = np.linspace(0.0, 1.0, num=20)

    # normalize both comment_karma and reply_karma and replace with the bin #

    for t in unbinned_data:
        normed_comm = (float(t.comment_karma - min_comment_karma)) / (float(max_comment_karma - min_comment_karma))
        t.comment_karma = np.digitize([normed_comm], bins)[0]
        normed_reply = (float(t.reply_karma - min_reply_karma)) / (float(max_reply_karma - min_reply_karma))
        t.reply_karma = np.digitize([normed_reply], bins)[0]

    binned_data = {}

    for t in unbinned_data:
        if (t.comment_karma, t.reply_karma) not in binned_data:
            binned_data[(t.comment_karma, t.reply_karma)] = []
        binned_data[(t.comment_karma, t.reply_karma)].append((t.comment_text, t.reply_text))

    return binned_data
