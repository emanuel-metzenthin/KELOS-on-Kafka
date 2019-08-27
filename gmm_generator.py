import pandas as pd
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt

total_samples = 10000
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
df = pd.DataFrame(data)

df[[0, 1]].to_csv('gmm_test_data_unlabeled.csv', index=False, header=False, float_format='%.5f')

plt.scatter(df[0], df[1], s=10)
plt.show()