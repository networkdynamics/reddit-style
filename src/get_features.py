"""
This file is where we pull in all the various features, essentially creating the values we will use for stats
"""
import language_model
import json
import pandas as pd
import numpy as np
import extract_pairs
import get_token_categories
from collections import Counter, defaultdict
import csv

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

    if not valid_file_paths:
        raise ValueError("You haven't processed these date ranges yet")

    big_frame = pd.concat(
        [pd.read_csv(f, sep=',', index_col=0, header=0,
                     dtype={"karma":np.int32}) for f in valid_file_paths])

    return big_frame


def get_category_counts(pairs, relevant_categories, text_min, text_max):
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
            sents = language_model.preprocess_text(text, text_min, text_max)
            # awkard, cause I copied preprocess from kenlm tutorial.
            # which reconstructs into sents for output.
            tokens = [item for sublist in sents for item in sublist.split(" ")]
            counts = Counter(category for token in tokens for category in
                parse(token))
            category_counts = {cat: counts[cat] for cat in
                               relevant_categories}

            pair_category_counts.append(category_counts)

        all_text_counts.append(pair_category_counts)

    return all_text_counts

# TODO: need to confirm this after new data is generated
def restrict_df(df, date_limit, subreddit=None):
    """

    :param df:
    :param date_limit: where date_limit is in unix time.
    :param subreddit:
    :return:
    """
    print "BEFORE", df.shape
    restricted_df = df.loc[(df['created_time'] < date_limit)]
    if subreddit:
        restricted_df = restricted_df.loc[(restricted_df['subreddit'].eq(subreddit))]

    print "AFTER", df.shape
    return restricted_df

def get_pairs_interactions_karma_prolificness_date_limited(pairs, comment_df, post_df, subreddit=None):
    """
    Gets all data from before each pair
    :param pairs:
    :param subreddit:
    :return:
    """

    user_prolificness = []
    user_interactions = []
    user_karma = []
    for pair in pairs:


        user1 = pair[0]["author"]
        user2 = pair[1]["author"]
        print user1, user2
        date_limit = pair[0]["created_time"]
        comment_df = restrict_df(comment_df, date_limit, subreddit=subreddit)
        post_df = restrict_df(post_df, date_limit, subreddit=subreddit)

        print "restricted dataframes"

        #interactions
        res_interactions = get_user_interactions(user1, user2, comment_df)
        user_interactions.append(res_interactions)

        print "got interactions"

        #prolificness
        pair_prolificness = []
        for user in [user1, user2]:
            res_prolificness = get_user_prolificness(user, comment_df, post_df)
            pair_prolificness.append(res_prolificness)
        user_prolificness.append(pair_prolificness)

        print "got prolificness"

        #karma
        pair_karma = []
        for user in [user1, user2]:
            res_karma = get_user_karma(user, comment_df, post_df)
            pair_karma.append(res_karma)
        user_karma.append(pair_karma)

        print "got karma"

    return user_interactions, user_karma, user_prolificness


# TODO: refactor below to use apply ??
# TODO: include replies to top-level posts?
def get_user_interactions(user1, user2, df):
    """
    Gets the number of times two users have interacted in the given dataframe.
    :param user1:
    :param user2:
    :param df:
    :param subreddit: If value, then interactions only on that subreddit will be included.
    Otherwise will integrate all data from that date range.
    :return:
    """

    res = df.loc[((df['author'] == user1) & (df['parent_author'] == user2)) |
                 ((df['author'] == user2) & (df['parent_author'] == user1))]

    return res.shape[0]


def get_user_prolificness(user, comment_df, post_df):
    """
    Get user prolificness for the data given. Sum of number of comments and posts.
    :param user:
    :param comment_df:
    :param post_df:
    :param subreddit:
    :return:
    """
    res_comm = comment_df.loc[(comment_df['author'] == user)].shape[0]
    res_post = post_df.loc[(post_df['author'] == user)].shape[0]

    return res_comm + res_post


# TODO: double check logic here.
def get_user_karma(user, comment_df, post_df):
    """
    Get user karma for the given data.
    Tuple of comment and post karma
    :param user:
    :param comment_df:
    :param post_df:
    :param subreddit:
    :return:
    """

    comment_karma = comment_df.loc[(comment_df['author'] == user)]['karma'].sum()
    post_karma = post_df.loc[(post_df['author'] == user)]['karma'].sum()

    return comment_karma, post_karma


# TODO: test this.
def load_pairs(base_path, year, start_month, end_month, subreddits, num_pairs_cap):
    """
    Loads the parent child pairs found in the given file
    :param file_path:
    :return: The pairs organized by subreddit
    """

    base_path_full = base_path + "top_level_comments/"
    valid_file_paths = extract_pairs.list_file_appropriate_data_range(year,
                                                        start_month, end_month,
                                                        base_path_full)

    pairs = defaultdict(lambda: [])

    for file_path in valid_file_paths:
        with open(file_path, "rb") as f:
            ntlp_reader = csv.reader(f, delimiter=',',
                                   quotechar='|', quoting=csv.QUOTE_MINIMAL)
            ntlp_reader.next()

            count = 0

            for line in ntlp_reader:

                if count > num_pairs_cap:
                    break

                count += 1

                parent = json.loads(line[0])

                subreddit = parent["subreddit"]
                if subreddit not in subreddits:
                    continue

                child = json.loads(line[1])

                if "deleted" in child["author"] or "deleted" in parent["author"]:
                    continue

                pairs[subreddit].append((parent, child))

    return pairs


def get_language_model_match(pairs_list, lm, text_min, text_max):
    """
    Given a list of pairs, get the language model similarity score for the parent in the pair. Parent always comes first.
    :param pairs_list: List of tuples of json objects
    :return: A list of len(pairs_list)
    """

    all_text_parent = []
    all_text_child = []
    for pair in pairs_list:
        parent_ob = pair[0]
        child_ob = pair[1]

        all_text_parent.append(parent_ob["body"])
        all_text_child.append(child_ob["body"])

    res_parent, lengths_parent = language_model.text_scores(all_text_parent, lm, text_min, text_max)

    res_child, lengths_child = language_model.text_scores(all_text_child, lm, text_min, text_max)

    return zip(res_parent, res_child), zip(lengths_parent, lengths_child)

def write_to_csv(subreddits, year, start_month_pairs, end_month_pairs, start_month_metadata, end_month_metadata, ngrams, text_min,
                 text_max, base_path, relevant_categories, out_file, restrict_to_subreddit_only, num_pairs_cap, num_pairs_min):
    """
    Defines how the various value functions are called and how they are written to csv
    to ensure consistentcy
    :param subreddits:
    :param year:
    :param start_month:
    :param end_month:
    :param ngrams:
    :param text_min:
    :param text_max:
    :param base_path:
    :param relevant_categories:
    :param out_file:
    :param user_prolificness_subreddit:
    :param user_karma_subreddit:
    :param prior_interaction_subreddit:
    :return:
    """

    comment_df = load_dataframe(year, start_month_metadata, end_month_metadata,
                                             base_path, contribtype="comment")
    post_df = load_dataframe(year, start_month_metadata, end_month_metadata,
                                          base_path, contribtype="post")

    pairs = load_pairs(base_path, year, start_month_pairs, end_month_pairs,
                                    subreddits, num_pairs_cap)

    print "loaded dataframes and pairs"

    # language_model.create_subreddit_language_models(subreddits, year,
    #                                                 start_month_metadata, end_month_metadata,
    #                                                 ngrams, text_min, text_max,
    #                                                 base_path)

    # TODO: check that parent is always in the right pace
    categories_per_comment_header = []

    # so at the end we have all of your
    for category in relevant_categories:
        categories_per_comment_header.append(category + "_parent")
        categories_per_comment_header.append(category + "_child")

    csv_header_values = ["subreddit",
                         "parent_id",
                         "child_id",
                         "parent_name",
                         "child_name",
                         "parent_lm",
                         "child_lm",
                         "parent_user_proflificness",
                         "child_user_prolificness",
                         "parent_user_karma_comment",
                         "parent_user_karma_post",
                         "child_user_karma_comment",
                         "child_user_karma_post",
                         "pair_prior_interactions",
                         "parent_text_length_words",
                         "child_text_length_words"
                         ]

    csv_header_values = csv_header_values + categories_per_comment_header

    with open(out_file, "wb") as csvfile:

        cwriter = csv.writer(csvfile)

        cwriter.writerow(csv_header_values)

        for subreddit in subreddits:

            print "processing {} now".format(subreddit)

            subreddit_pairs = pairs[subreddit]

            if not subreddit_pairs:
                print "pairs didn't exist"
                continue

            #TODO: check that you can get rid of this
            #should already be done in new data but here's a double check
            #sucks for those who put deleted in their usernames but oh well



            if len(subreddit_pairs) < num_pairs_min:
                continue

            #TODO: fix after copy
            lm_base_path = "/home/ndg/projects/shared_datasets/reddit-style/"

            lm = language_model.load_language_model(subreddit, year,
                                                    start_month_metadata, end_month_metadata,
                                                    ngrams, text_min,
                                                    text_max, lm_base_path)


            print "loaded language model"

            # next 3 below values are a list of tuples

            pairs_entropy_values, pairs_lengths = get_language_model_match(subreddit_pairs, lm, text_min, text_max)

            print "got entropy values"


            if restrict_to_subreddit_only:
                restricted_subreddit = subreddit
            else:
                restricted_subreddit = None

            pairs_prior_interactions, pairs_user_karma, \
            pairs_user_prolificness = \
                get_pairs_interactions_karma_prolificness_date_limited(
                    subreddit_pairs, comment_df, post_df,
                    subreddit=restricted_subreddit)

            # list of lists of dictionaries
            pairs_category_counts = get_category_counts(
                subreddit_pairs, relevant_categories, text_min, text_max)

            print "got category counts"

            for i in range(len(subreddit_pairs)):
                pair = subreddit_pairs[i]

                # TODO: this is error city
                values = [subreddit,
                          pair[0]["id"],
                          pair[1]["id"],
                          pair[0]["author"],
                          pair[1]["author"],
                          pairs_entropy_values[i][0],
                          pairs_entropy_values[i][1],
                          pairs_user_prolificness[i][0],
                          pairs_user_prolificness[i][1],
                          pairs_user_karma[i][0][0],
                          pairs_user_karma[i][0][1], #TODO: fix with named tuples
                          pairs_user_karma[i][1][0],
                          pairs_user_karma[i][1][1],
                          pairs_prior_interactions[i],
                          pairs_lengths[i][0],
                          pairs_lengths[i][1],
                          ]

                category_values = []
                for category in relevant_categories:
                    category_values.append(
                        pairs_category_counts[i][0][category])
                    category_values.append(
                        pairs_category_counts[i][1][category])

                values = values + category_values

                cwriter.writerow(values)
