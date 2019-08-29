import pandas as pd
import numpy as np

elements_per_window = 6000
aggregation_windows = 3
n = 1000 # There are sometimes a lot of outliers in one window, so we have to set n very high for this


real_outliers = pd.read_csv('http_labeled.csv', header=None)[3]
kelos_results = pd.read_csv('outliers.csv', header=None)
kelos_results.columns = ['index', '0', '1', '2']

total_considered = 0
total_found = 0

window_start = 0
window_step = int(elements_per_window / aggregation_windows)
window_end = window_step
kelos_start = 0
kelos_end = n

while window_end < len(real_outliers):
	records_in_window = real_outliers[window_start:window_end]
	outliers_in_window = records_in_window[records_in_window == 1].reset_index()
	outlier_count = sum(outliers_in_window[3])
	
	# Only take as many results into consideration as there are outliers in the window
	relevant_results = kelos_results[kelos_start:(kelos_start + outlier_count)]
	
	total_found += len(pd.merge(outliers_in_window, relevant_results, how='inner', on=['index']))
	total_considered += outlier_count
	
	
	if window_end - window_start == elements_per_window:
		window_start += window_step
	window_end += window_step
	kelos_start = kelos_end
	kelos_end += n

print(f'P@|O|: {total_found / total_considered}')
