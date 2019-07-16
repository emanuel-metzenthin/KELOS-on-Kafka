import pandas as pd
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt

mean = [0, 0]
cov = [[1, 0], [0, 1]]

points = np.random.multivariate_normal(mean, cov, 2000)
probabilities = scipy.stats.multivariate_normal.pdf(points, mean, cov)
labels = probabilities < 0.005
labels = labels.reshape((labels.shape[0], 1))

data = np.concatenate((points, labels), axis=1)
df = pd.DataFrame(data)

df[[0, 1]].to_csv('test_data_unlabeled.csv', index=False, header=False)
df.to_csv('test_data_labeled.csv', index=False, header=False)

colors = {0:'blue', 1:'red'}
plt.scatter(df[0], df[1], c=df[2].apply(lambda x: colors[x]))
plt.show()
