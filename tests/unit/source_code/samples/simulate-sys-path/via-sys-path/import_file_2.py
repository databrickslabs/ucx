import sys


def func():
    sys.path.insert(0, "./some-folder")
    from some_file import stuff

    stuff()
