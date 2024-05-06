import sys


def func():
    sys.path.append("./some-folder")
    from some_file import stuff
    stuff()
