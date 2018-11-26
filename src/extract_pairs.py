# coding=utf-8
""""
Simple script written to accommodate multiprocessing. Given a file, extracts top level comments and immediate replies
"""
import gzip
import re
import sys

def extract_top_level_pairs(file_path, include_multiple):
    """
    Iterates over the given file twice, the first time extracting all top-level comments
    The second time finding all the immediate replies to those comments.
    Attempts to be as efficient as possible.
    Writes pairs on a single line to stdout.
    :param file_name: The file path to include
    :param include_multiple: Whether or not to include multiple direct replies per top-level comment
    :return: None
    """

    tl_comments = {}
    fop = gzip.open(file_path, 'rb')
    for line in fop:
        if 'parent_id":"t3' in line: # aka is a top_level comment
            id = re.search('\"id\":\"(.+?)\"', line).group(1)
            tl_comments[id] = line.strip()

    fop.seek(0) #go back to the staaaaaaart

    for line in fop:
        pot_id = re.search('\"parent_id\":\"(.+?)\"', line).group(1)
        pot_id = pot_id[3:] #get rid of t3
        if pot_id in tl_comments:
            print tl_comments[pot_id], line.strip()

    fop.close()

if __name__ == "__main__":
    file_path = sys.argv[1]
    include_multiple = True # because I can't figure out params when using gnu parallel
    extract_top_level_pairs(file_path, include_multiple)

