# coding=utf-8

# manhatten曼哈顿距离
# Euclidean欧几里得距离
# minkowski闵克夫斯基距离
# pearson皮尔逊相关系数
# cosSimilarity余弦相似度
####应用场景####
# 1、数据存在“分数膨胀”，采用pearson皮尔逊相关系数
# 2、数据比较密集，采用manhatten曼哈顿距离或Euclidean欧几里得距离或minkowski闵克夫斯基距离
# 3、数据比较稀疏，采用余弦相似度
####自定义的数据格式####
# u1用户1    book1书1    3.5评分
# users = {"u1": {'book1': 3.5, 'book2': 4, 'book3': 3, 'book4': 1},
#          "u2": {'book2': 5, 'book5': 4, 'book1': 3.5, 'book4': 1},
#          "u3": {'book3': 2.5, 'book4': 4, 'book5': 3, 'book6': 1},
#          "u4": {'book6': 4, 'book4': 4, 'book3': 3, 'book1': 1.5}}

def manhattan(rating1, rating2):
    """计算曼哈顿距离，rating表示某个用户的所有物品及评分"""
    distance = 0  # manhatten距离
    for item in rating1:  # item表示被评分物品
        if item in rating2:  # 判断item是否同时在rating1和rating2中，即是否同时被两个用户评价过
            score1 = rating1[item]  # score代表分数
            score2 = rating2[item]
            distance += abs(score1 - score2)
    return distance


def euclidean(rating1, rating2):
    """计算欧几里得距离，rating表示某个用户的所有物品及评分"""
    distance = 0  # 欧几里得距离
    for item in rating1:  # item表示被评分物品
        if item in rating2:  # 判断item是否同时在rating1和rating2中，即是否同时被两个用户评价过
            score1 = rating1[item]  # score代表分数
            score2 = rating2[item]
            distance += pow((score1 - score2), 2)
            distance = pow(distance, 0.5)
    return distance


def minkowski(rating1, rating2, r=10):
    """计算闵克夫夫斯基距离，rating表示某个用户的所有物品及评分，r是闵克夫夫斯基距离公式要求的参数，r=10时为曼哈顿距离，r=2时为欧几里得距离"""
    distance = 0  # 闵克夫斯基距离
    for item in rating1:  # item表示被评分物品
        if item in rating2:  # 判断item是否同时在rating1和rating2中，即是否同时被两个用户评价过
            score1 = rating1[item]  # score代表分数
            score2 = rating2[item]
            distance += pow(abs(score1 - score2), r)
            distance = pow(distance, 1.0 / r)
    return distance


def pearson(rating1, rating2):
    """皮尔逊相关系数，rating表示某个用户的所有物品及评分"""
    sum_xy = 0.0  # xy乘积的和
    sum_x = 0.0  # x的和
    sum_y = 0.0  # y的和
    sum_x2 = 0.0  # x的平方的和
    sum_y2 = 0.0  # y的平方的和
    n = 0  # 用于计算rating的长度
    for item in rating1:
        if item in rating2:
            n += 1
            x = rating1[item]
            y = rating2[item]
            sum_xy += x * y
            sum_x += x
            sum_y += y
            sum_x2 += pow(x, 2)
            sum_y2 += pow(y, 2)
    # 分子
    n = n * 1.0
    numerator = sum_xy - sum_x * sum_y / n
    # 分母
    denominator = pow((sum_x2 - (sum_x ** 2) / n), 0.5) * pow((sum_y2 - (sum_y ** 2) / n), 0.5)
    if denominator == 0:
        pearsonCoefficient = 0
    else:
        pearsonCoefficient = numerator / denominator  # 皮尔逊相关系数近似值
    return pearsonCoefficient


def cosSimilarity(list1, list2):
    """计算x,y余弦相似度,x,y均为list"""
    sum_xy = 0.0  # xy乘积之和
    sum_x2 = 0.0  # x的平方之和
    sum_y2 = 0.0  # y的平方之和
    for index, item in enumerate(list1):
        sum_xy += list1[index] * list2[index]
        sum_x2 += list1[index] ** 2
        sum_y2 += list2[index] ** 2
    # 分母
    denominator = pow(sum_x2, 0.5) * pow(sum_y2, 0.5)
    return sum_xy * 1.0 / denominator


def itemAll(users):
    """返回users中所有的物品item"""
    itemSet = set()
    for user in users:
        for item in users[user]:
            itemSet.add(item)
    itemList = list(itemSet)
    itemList.sort()
    return itemList


def vect(username, users):
    """返回代表用户username的向量"""
    items = itemAll(users)  # 返回所有的物品list(item1,item2...)
    userVec = []  # 用户username的向量
    rating = users[username]  # 返回的是{'item1':3,'item2':4...}
    for item in items:
        if item in rating:
            userVec.append(rating[item])
        else:
            userVec.append(0)
    return userVec