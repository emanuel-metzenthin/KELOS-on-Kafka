import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

cluster_1 = np.random.multivariate_normal([0, 0, 0], [[1, 0, 0], [0, 3, 0], [0, 0, 1]], 2000)
cluster_2 = np.random.multivariate_normal([4, 4, 4], [[1.5, 0, 0], [0, 1, 0], [0, 0, 1]], 2000)
cluster_3 = np.random.multivariate_normal([10, -5, 3], [[1, 0, 0], [0, 3, 0], [0, 0, 10]], 2000)
cluster_4 = np.random.multivariate_normal([-2, -15, -5], [[1, 0, 0], [0, 3, 0], [0, 0, 10]], 2000)

data = np.concatenate((cluster_1, cluster_2, cluster_3, cluster_4))
print(data)

df = pd.DataFrame(data)

plt.scatter(df[0], df[2])

df.to_csv('cluster_test_data.csv', index=False, header=False)