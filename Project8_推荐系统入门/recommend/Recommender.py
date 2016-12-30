# coding=utf-8
import AlgorithmUtil as al
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
# u1用户1    book1书1    3.5评分     字典类型
# users = {"u1": {'book1': 3.5, 'book2': 4, 'book3': 3, 'book4': 1},
#          "u2": {'book2': 5, 'book5': 4, 'book1': 3.5, 'book4': 1},
#          "u3": {'book3': 2.5, 'book4': 4, 'book5': 3, 'book6': 1},
#          "u4": {'book6': 4, 'book4': 4, 'book3': 3, 'book1': 1.5}}

class Recommender:
    def __init__(self, data, k=3, metric='pearson', n=5):
        """k-NearestNeighbor，k最近邻算法
        data    训练数据
        k       k最近邻算法中的值
        metric  使用哪种距离计算方式
        n       推荐结果的数量
        """
        self.k = k
        self.n = n
        # 使用哪种距离计算方式
        if metric == 'pearson':
            self.fn = al.pearson  # 皮尔逊系数
        elif metric == 'manhattan':
            self.fn = al.manhattan  # 曼哈顿距离
        elif metric == 'euclidean':
            self.fn = al.euclidean  # 欧几里得距离
        elif metric == 'minkowski':
            self.fn = al.minkowski  # 闵克夫斯基距离
        else:
            self.fn = al.cosSimilarity  # 余弦相似度

        # 如果data是一个字典类型，则保存下来，否则忽略
        # if type(self.data)._name_ == 'dict':
        self.data = data

    def recommend(self, username):
        # 计算结果，推荐列表
        result = {}
        # 找k个最近邻用户
        kNearestNeighbors = self.compareDis(username)[:self.k]
        # 获取用户username评价过的商品
        userRatings = self.data[username]
        # 计算总距离
        totalDistance = 0.0
        for kValue in kNearestNeighbors:
            totalDistance += kValue[0]
        # 对推荐物品进行加权评分
        for kValue in kNearestNeighbors:
            weight = kValue[0] / totalDistance  # 计算各邻近用户权重即占比
            name = kValue[1]  # 用户姓名
            # 获得该用户的所有评价
            neighborRatings = self.data[name]
            # 获得没有评价过的商品
            for item in neighborRatings:
                if not item in userRatings:
                    if not item in result:
                        result[item] = neighborRatings[item] * weight
                    else:
                        result[item] += neighborRatings[item] * weight
        # 开始推荐
        recommendations = list(result.items())
        recommendations.sort(key=lambda tuple: tuple[1], reverse=True)
        print recommendations[:self.n]

    def compareDis(self, userName):
        """计算用户userName与其他用户的距离，返回排序后的(距离，用户)"""
        result = []  # 利用manhatten距离算出的结果
        for user in self.data:
            if user != userName:
                distance = self.fn(self.data[user], self.data[userName])  # 调用求距离的函数
                result.append((distance, user))  # (距离，用户)
        result.sort()
        return result