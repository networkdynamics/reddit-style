"""
    This file has all functions needed to compute linguistic alignment.
"""
from nltk.tokenize import sent_tokenize, word_tokenize
from random import shuffle
from collections import defaultdict

def create_fake_turns(real_turns):
    """
    Shuffles the real comment-reply tuples, and returns a set of fake comment-reply pairs.
    """
    comments = list(zip(*real_turns)[0])
    replies = list(zip(*real_turns)[1])
    shuffle(comments)
    shuffle(replies)
    fake_turns = zip(comments, replies)
    return fake_turns


def is_present(dim, comment):
    """
    Returns True if feature 'dim' is present in 'comment'; False otherwise.
    """
    sents = sent_tokenize(comment)
    for sent in sents:
        words = word_tokenize(sent)
        for word in words:
            if word.lower() in LIWC_dic[dim]:
                return True
    return False


def compute_alignment(cr_tuples, dimensions):
    """
    Given the comment-reply tuples for a 'bin', calculates the linguistic alignment score for the given dimensions.
    :param cr_tuples: list
    :param dimensions: list
    :return: dict (key = dimension, value = alignment score)
    """
    total_number_of_turns = float(len(cr_tuples))
    turns_with_C = 0.0

    # Initialise dictionary:
    map_dim_alignment = {}
    for dim in dimensions:
        map_dim_alignment[dim] = 0.0

    for tup in cr_tuples:
        for dim in dimensions:
            if is_present(dim, tup[0]) and is_present(dim, tup[1]):
                map_dim_alignment[dim] += 1

    fake_turns = create_fake_turns(cr_tuples)

    for tup in fake_turns:
        for dim in dimensions:
            if is_present(dim, tup[0]) and is_present(dim, tup[1]):
                map_dim_alignment[dim] -= 1

    map_dim_alignment = {k: v / total_number_of_turns for k, v in map_dim_alignment.iteritems()}

    return map_dim_alignment

## Testing: ##
# LIWC_dic = {'article': set(['a', 'an', 'the']),
#             'we': set(['we', 'us', 'our']),
#             'you': set(['you', 'your', 'thou']),
#             'shehe': set(['she', 'her', 'him'])
#             }
#
# cr_tuples = [['hey how are you?', 'yo i am good. And you?'],
#               ['wazza bugyd', 'ntg much'],
#               ['she is cool', 'yea I like her dress'],
#               ['she is not hot', 'you know']]
#
# dimensions = ['you', 'shehe', 'we']
# print compute_alignment(cr_tuples, dimensions)
