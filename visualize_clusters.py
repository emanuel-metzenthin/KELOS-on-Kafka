import matplotlib.pyplot as plt
import pandas as pd


data = pd.read_csv('assignments.csv', header=None)
centroids = pd.read_csv('clusters.csv', header=None)


fig = plt.figure(figsize=(20,20))
ax = fig.add_subplot(1, 1, 1)
start = 0
end = 1000
cluster_start = 0
cluster_end = 245

_ = ax.scatter(data[2][start:end],data[3][start:end],c=data[1][start:end], cmap='viridis', s=20)
_ = ax.scatter(centroids[1][cluster_start:cluster_end],centroids[2][cluster_start:cluster_end],c='black', s=20, alpha=0.5)

for i in range(cluster_start, cluster_end):
    _ = ax.annotate(f'C: {centroids[0][i]}', xy=(centroids[1][i], centroids[2][i]), textcoords='data')

for i in range(start, end):
    _ = ax.annotate(f'{data[1][i]}', xy=(data[2][i], data[3][i]), textcoords='data')
	
fig.savefig('clusters.png')
