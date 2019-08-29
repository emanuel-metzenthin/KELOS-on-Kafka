import pandas as pd
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt

mean = [0, 0]
cov = [[1, 0], [0, 1]]


data = pd.read_csv('gmm_test_data_unlabeled.csv', header=None)

candidates = pd.read_csv('candidates.csv', header=None)
outliers = pd.read_csv('outliers.csv', header=None)
zeros = pd.DataFrame(np.zeros(data.shape[0]))
zeros.iloc[candidates[0]] = 2
zeros.iloc[outliers[0]] = 1


colors = {0:'white', 2:'yellow', 1:'red'}
# plt.scatter(data[0], data[1], c=data[2].apply(lambda x: colors[x]))
# plt.show()

plt.scatter(data[0].iloc[outliers[0]], data[1].iloc[outliers[0]], s=10, c=zeros[0].iloc[outliers[0]].apply(lambda x: colors[x]))
plt.show()

plt.scatter(data[0].iloc[candidates[0]], data[1].iloc[candidates[0]], s=10, c=zeros[0].iloc[candidates[0]].apply(lambda x: colors[x]))
plt.show()


