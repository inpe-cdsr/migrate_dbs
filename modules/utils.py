# -*- coding: utf-8 -*-


def delete_and_recreate_folder(folder):
    from os import mkdir
    from shutil import rmtree

    # delete folder
    rmtree(folder, ignore_errors=True)
    # recreate folder
    mkdir(folder)


def str2bool(value):
    # Source: https://stackoverflow.com/a/715468
    return str(value).lower() in ('true', 't', '1', 'yes', 'y')
