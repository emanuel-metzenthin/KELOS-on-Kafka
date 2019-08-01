import matplotlib.pyplot as plt
import pandas as pd


data = pd.read_csv('assignments.csv', header=None)
fig = plt.figure()
ax = fig.add_subplot(111)
limit = 100

scatter = ax.scatter(data[2][:limit],data[3][:limit],c=data[1][:limit],s=20)

for i in range(limit):
    _ = ax.annotate(f'{data[1][i]}', xy=(data[2][i], data[3][i]), textcoords='data')
	
fig.savefig('clusters.png')
