import pandas as pd
from sklearn.neighbors import LocalOutlierFactor
import numpy as np

elements_per_window = 3000
aggregation_windows = 3
n = 100


data = pd.read_csv('gmm_test_data_unlabeled.csv', header=None)
kelos_results = pd.read_csv('outliers.csv', header=None)
kelos_results.columns = ['index', '0', '1']

in_common = 0

window_start = 0
window_step = int(elements_per_window / aggregation_windows)
window_end = window_step
kelos_start = 0
kelos_end = n

while window_end < len(data):
	# We don't have actual ground truth labels for this, so to at least get an idea of our performance we compare to LOF
	contamination = n / (window_end - window_start)
	clf = LocalOutlierFactor(n_neighbors=200, contamination=contamination)
	outliers = clf.fit_predict(data[window_start:window_end])
	
	outliers = data[window_start:window_end].reset_index().loc[outliers==-1]
	
	in_common += len(pd.merge(outliers, kelos_results[kelos_start:kelos_end], how='inner', on=['index']))
	
	
	if window_end - window_start == elements_per_window:
		window_start += window_step
	window_end += window_step
	kelos_start = kelos_end
	kelos_end += n

print(f'Similarity to LOF: {in_common / kelos_start}')
