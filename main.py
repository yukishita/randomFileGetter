import configparser

import randomFileGetter.controller.controller

"""Configファイルの読み込み"""
config = configparser.ConfigParser()
config.read('config.ini')

""" randomFileGetter 起動 """
controller = randomFileGetter.controller.controller.controller( config )
controller.startup()