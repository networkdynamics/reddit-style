# coding=utf-8
""""
Simple script written to accommodate multiprocessing. Given a file, extracts top level comments and immediate replies
"""
import gzip
import re
import sys
import csv
import json
import os


def extract_all_post_data_from_file(post_file_path, base_out_path="/home/ndg/projects/shared_datasets/reddit-style/"):
    file_name = post_file_path.split("/")[-1].replace(".gz", "")

    post_metadata_path = base_out_path + "post_metadata/" + file_name

    with open(post_metadata_path, "wb") as pm:
        pm_writer = csv.writer(pm, delimiter=',',
                               quotechar='|', quoting=csv.QUOTE_MINIMAL)
        pm_writer.writeheader(["post_id", "author", "karma", "subreddit"])

        fop = gzip.open(post_file_path, 'rb')
        for line in fop:
            post = json.loads(line)
            post_id = post["id"]
            author = post["author"]
            karma = post["score"]
            subreddit = post["subreddit"]

            pm_writer.writerow([post_id, author, karma, subreddit])

def extract_all_comment_data_from_file(comment_file_path, base_out_path="/home/ndg/projects/shared_datasets/reddit-style/"):
    """
    Iterates over the given file twice, the first time extracting all top-level comments
    The second time finding all the immediate replies to those comments.
    Attempts to be as efficient as possible.
    Writes pairs on a single line to stdout.
    :param comment_file_path: The file path to include
    :param include_multiple: Whether or not to include multiple direct replies per top-level comment
    :return: None
    """
    file_name = comment_file_path.split("/")[-1].replace(".gz", "")

    print file_name

    comment_metadata_path = base_out_path + "comment_metadata/" + file_name
    non_top_level_comments_path = base_out_path + "non_top_level_comments/" \
                                  + file_name
    top_level_comments_path = base_out_path + "top_level_comments/" + file_name

    # for p in [comment_metadata_path, non_top_level_comments_path,
    #           top_level_comments_path]:
    #     os.makedirs(p)

    with open(comment_metadata_path, "wb") as cm, \
            open(non_top_level_comments_path, "wb") as ntlc, \
            open(top_level_comments_path, "wb") as tlc:

        cm_writer = csv.writer(cm, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        ntlc_writer = csv.writer(ntlc, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)

        tlc_writer = csv.writer(tlc, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)

        cm_writer.writerow(["comment_id", "author", "parent_id",
                               "parent_author", "karma", "subreddit"])
        ntlc_writer.writerow(["comment_json"])
        tlc_writer.writerow(["parent_comment_json", "child_comment_json"])

        tl_comments = {}
        comments_author = {}
        fop = gzip.open(comment_file_path, 'rb')
        for line in fop:
            comm = json.loads(line)
            parent_id = comm["parent_id"]
            comm_id = comm["id"]
            author = comm["author"]
            if "t3_" in parent_id: # aka is a top_level comment
                tl_comments[comm_id] = line.strip()
            else:
                # only record parent if parent is a comment
                comments_author[comm_id] = author

        fop.seek(0) #go back to the staaaaaaart

        for line in fop:
            comm = json.loads(line)

            comm_id = comm["id"]
            author = comm["author"]
            parent_id = comm["parent_id"]
            short_parent_id = comm["parent_id"][3:] #get rid of t3/t1
            karma = comm["score"]
            subreddit = comm["subreddit"]

            if short_parent_id in tl_comments:
                tlc_writer.writerow([tl_comments[short_parent_id], line.strip()])
            elif "t3_" not in parent_id: #not a top level comment itself
                ntlc_writer.writerow([line.strip()])

            if short_parent_id in comments_author:
                parent_author = comments_author[short_parent_id]
            else:
                parent_author = None #top level comments that don't have a parent

            cm_writer.writerow([comm_id, author, short_parent_id,
                                parent_author, karma, subreddit])

    fop.close()

if __name__ == "__main__":
    file_path = sys.argv[1]
    extract_all_comment_data_from_file(file_path)

