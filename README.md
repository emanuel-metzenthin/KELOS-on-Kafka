# KELOS-on-Kafka
KELOS (Scalable Kernel Density Estimation-based Local Outlier Detection over Large Data Streams) implementation using Kafka Streams

# 1 Abstract

# 2 Installation instructions

# 3 Motivation

Techniques for outlier detection are applicable and needed for a variety of domains, ranging from fincanial fraud to network security. The goal is to identify data point which are very different from the rest of the dataset. In the case of credit card fraud this could be card transactions in a particularly high frequency or unusual location. The concept of local outliers works with the assumption that most datasets in the real-world are skewed and that their data points are distributed differently in different areas. Therefore a point is considered an outlier if the density at that spot is considerably lower than at the surrounding points, rather than taking the global dataset density into account. 

Due to an increase in the data sizes and sources we have to deal with nowadays algorithms that work on data streams instead of whole datasets are in high demand. There exist a few approaches to do outlier detection using streaming. The KELOS algorithm is an efficient and scalable solution to the problem. There exists no implementation of such an algorithm in a broadly adopted streaming framework. That is why we chose to bring KELOS to life using Kafka Streams. 

# 4 Related work

# 5 Architecture

## 5.1 Data Abstractor

## 5.2 Density Estimator

## 5.3 Outlier Detector

# 6 Evaluation

## 6.1 Experimental Setup

## 6.2 Results

# 7 Conclusion and future work

# References
