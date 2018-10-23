# coding=utf-8
"""
This file contains all methods used to define the data pipeline for each experiment. This is the only place we should
change any code when running an experiment. Each experiment should have a new method. Liberal copy-pasting is
encouraged. No parameters should be used, but the @create_dir_wrap decorator with dir_name parameter
allows for the creation of a new directory with the same name as the method name.
"""
import DataProcessor
import Methods
from functools import wraps
import os

data_folder = "../data/"


def make_directory(dir_name):
    """
    Creates a directory.
    :param dir_name: Name of the directory.
    :return:
    """
    dir_name = data_folder + dir_name  # HARD CODING WHOOP
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def create_dir_wrap(func):
    @wraps(func)
    def tmp():
        dir_name = func.__name__
        make_directory(dir_name)
        return func(dir_name=dir_name)
    return tmp


@create_dir_wrap
def template_experiment(dir_name=None):
    PARAM1 = 3
    PARAM2 = 5
    #proc_data = DataProcessor.something(PARAM1, PARAM2)
    #results = Methods.something(proc_data)


if __name__ == "__main__":
    # call the experiment method
    pass
