import pandas as pd
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt
from sklearn.neighbors import LocalOutlierFactor

total_samples = 30000
weights = [0.6, 0.3, 0.1]
means = [[0, 0], [5, 0], [3, 4]]
covs = [[[1, -0.5], [-0.5, 1]], [[1, 0.2], [0.2, 1]], [[2, 0], [0, 2]]]

samples_per_distribution = np.random.multinomial(total_samples, weights)

results = []

for i in range(len(weights)):
	points = np.random.multivariate_normal(means[i], covs[i], samples_per_distribution[i])
	
	results.append(points)
	
data = np.concatenate(results, axis=0)
np.random.shuffle(data)

clf = LocalOutlierFactor(n_neighbors=200, contamination=0.01)
outliers = pd.DataFrame(clf.fit_predict(data))

df = pd.DataFrame(np.concatenate([data, outliers], axis=1))


df[[0, 1]].to_csv('gmm_test_data_unlabeled.csv', index=False, header=False, float_format='%.5f')
df.to_csv('gmm_test_data_labeled.csv', index=False, header=False, float_format='%.5f')

colors = {1:'blue', -1:'red'}
plt.scatter(df[0], df[1], s=10, c=df[2].apply(lambda x: colors[x]))
plt.show()