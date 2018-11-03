# coding=utf-8
"""
This file contains all methods on pre-processed data
"""
import pandas as pd
import numpy as np
import random


######################## For Cohesion ########################

def create_fake_turns(turns):
    """
    Takes in real turns, and creates fake turns.
    """
    temp1 = []
    temp2 = []
    for (p, q) in turns:
        temp1.append(p)
        temp2.append(q)
    # Shuffling:
    while True:
        temp1_ran = random.sample(temp1, len(temp1))
        temp2_ran = random.sample(temp2, len(temp2))
        fake_turns = zip(temp1_ran, temp2_ran)
        # Checking if any of the tuples are still same:
        p = len(set(turns).intersection(set(fake_turns)))
        if p == 0:
            break
    return fake_turns


def compute_cohesion(C, turns, liwc_output_path, subreddit):
    """
    Calculates cohesion for a given subreddit, along with other useful info (see below); Terms from eq (1) that are
    needed for Fisher's test.

    Parameters
    ----------
    turns:
    C: the dimension (pronoun etc.)
    liwc_output_path: path of the LIWC's output
    subreddit: name of subreddit

    Returns
    -------
    cohesion: cohesion value for the given subreddit.
    The other four terms are numerators and denominators of eq (5) and (6).
    """
    liwc_df = pd.read_csv(liwc_output_path, delimiter='\t')[['Filename', C]]

    ##### Calculating First Probability in eq (1) #####
    total_number_of_turns = len(turns)
    # 'real_turns' is  a list of tuples that I'll pass to create_fake_turns:
    real_turns = []
    turns_with_C = 0.0
    for tup in turns:
        conv_index = turns.index(tup)
        temp_df = liwc_df.loc[liwc_df.Filename.str.startswith(str(subreddit)+str(conv_index)+'_person')]
        if temp_df.shape[0] != 2:
            continue
        c_values = temp_df[C].values
        if 0 not in c_values:
            turns_with_C += 1
        # Creating a tuple of filenames (will be used in second_prob)
        real_turns.append(tuple(temp_df['Filename'].values))
    first_prob = turns_with_C / total_number_of_turns
    ##################################################

    ##### Calculating Second Probability in eq (1) #####
    # For second probability: converting df to list of tuples
    fake_turns = create_fake_turns(real_turns)
    # 'fn_C_map' is a dictionary to map filenames to C count:
    fn_C_map = dict(zip(liwc_df.Filename, liwc_df.iloc[:,1]))
    faketurns_withC = 0.0
    for f1, f2 in fake_turns:
        c1 = fn_C_map[f1]
        c2 = fn_C_map[f2]
        if c1 != 0.0 and c2 != 0.0:
            faketurns_withC += 1
    second_prob = faketurns_withC / total_number_of_turns
    ##################################################
    cohesion = first_prob - second_prob
    return cohesion, turns_with_C, total_number_of_turns, faketurns_withC, total_number_of_turns




######################## For Accommodation ########################

def average_dataset_accommodation(acc_dict):
    """
    Calculates the accommodation exhibited over the entire dataset.

    Parameters
    ----------
    acc_dict: the dictionary returned by 'compute_accommodation' (that maps userpair to its accommodation value).

    Returns
    -------
        float: the average accommodation over the entire dataset.
    """
    return np.array(acc_dict.values()).mean()


def compute_accommodation(all_basic_comment_tuples, C, liwc_output_path, subreddit_name, MIN_DENOM):
    """
    Calculates accommodation for a given subreddit;
    and returns a dictionary that maps userpair to its accommodation-value, along with other useful info (see below).

    Parameters
    ----------
    all_basic_comment_tuples: dictionary mapping (userpairs):[list of interactions]
    C: the dimension (pronoun etc.)
    liwc_output_path: path of the LIWC's output
    subreddit_name: name of subreddit
    MIN_DENOM: minimum denominator value for second term in equation (2)

    Returns
    -------
    accommodation: dictionary that maps (userpair) to its accommodation value.
    accom_terms: dictionary that maps (userpair) to a tuple (first_term, second_term)
    total_userpairs: total number of userpairs in all_basic_comment_tuples
    skipped_userpairs: number of userpairs skipped because of the MIN_DENOM condition
    """
    liwc_df = pd.read_csv(liwc_output_path, delimiter='\t')[['Filename', C]]
    total_userpairs = len(all_basic_comment_tuples.keys())

    accommodation = {}
    accom_terms = {}

    conv_index = 0
    skipped_userpairs = 0

    for user_pair, conversation in all_basic_comment_tuples.items():
        ##### Calculating Second Probability in eq (2) #####
        total_number_of_replies = len(conversation)

        if total_number_of_replies < MIN_DENOM:
            print "Skipping because second-term-denom is too small: ", total_number_of_replies
            continue

        # Selecting the second user (replier i.e. user_pair[1]) and make sure that it's only the current conversation:
        temp_df_1 = liwc_df.loc[liwc_df.Filename.str.startswith(str(subreddit_name) + '_' + str(conv_index) + '_' + str(user_pair[1]))]
        c_values_1 = temp_df_1[C].values
        user2_exhibit_C = np.count_nonzero(c_values_1)
        second_term = user2_exhibit_C / float(total_number_of_replies)
        #####################################################

        ##### Calculating First Probability in eq (2) #####
        temp_df_2 = liwc_df.loc[liwc_df.Filename.str.startswith(str(subreddit_name) + '_' + str(conv_index) + '_' + str(user_pair[0]))]
        c_values_2 = temp_df_2[C].values
        user1_exhibit_C = np.count_nonzero(c_values_2)

        # NOTE: We should technically have the same MIN_DENOM condition here, but for now we just want it to be non-zero.
        if user1_exhibit_C == 0:
            skipped_userpairs += 1
            continue

        df_user2 = liwc_df.loc[liwc_df.Filename.str.startswith(str(subreddit_name) + '_' + str(conv_index) + '_' + str(user_pair[1]))]
        df_user1 = liwc_df.loc[liwc_df.Filename.str.startswith(str(subreddit_name) + '_' + str(conv_index) + '_' + str(user_pair[0]))]
        df_concat = pd.concat([df_user2, df_user1])

        both_users_exhibit_C = 0.0
        R = df_concat.shape[0]

        for k in range(0, (R/2)):
            temp_concatdf = df_concat.loc[df_concat.Filename.str.endswith('_'+str(k)+'_.txt')]
            if temp_concatdf.shape[0] != 2:
                continue

            if 0.0 not in temp_concatdf[C].tolist():
                both_users_exhibit_C += 1

        first_term = both_users_exhibit_C / float(user1_exhibit_C)
        #####################################################

        accommodation[user_pair] = first_term - second_term
        accom_terms[user_pair] = (first_term, second_term)

        conv_index += 1

    return accommodation, accom_terms, total_userpairs, skipped_userpairs
