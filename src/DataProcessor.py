# coding=utf-8
"""
This file defines all methods used to process data before we pass it through the methods.
"""
import pandas as pd
import numpy as np


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

