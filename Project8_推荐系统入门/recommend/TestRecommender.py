# coding=utf-8

import AlgorithmUtil as dis
from recommend.Recommender import *

# u1用户1    book1书1    3.5评分
users = {"u1": {'book1': 3.5, 'book2': 4, 'book3': 3, 'book4': 1},
         "u2": {'book2': 5, 'book5': 4, 'book1': 3.5, 'book4': 1},
         "u3": {'book3': 2.5, 'book4': 4, 'book5': 3, 'book6': 1},
         "u4": {'book6': 4, 'book4': 4, 'book3': 3, 'book1': 1.5}}

r = Recommender(users, 'pearson')
r.recommend('u2')
