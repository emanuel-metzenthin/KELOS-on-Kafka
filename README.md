# KELOS-on-Kafka
KELOS (Scalable Kernel Density Estimation-based Local Outlier Detection over Large Data Streams) implementation using Kafka Streams

# 1 Abstract

# 2 Installation instructions

# 3 Motivation

Techniques for outlier detection are applicable to and needed for a variety of domains, ranging from financial fraud detection to network security [4]. The goal is to identify data points which are very different from the rest of the dataset. In the case of credit card fraud this could be card transactions accuring in a particularly high frequency or unusual location. The concept of *local* outliers works with the assumption that most datasets in the real-world are skewed and that their data points thus have varying distribution properties [1]. Therefore a point is considered an outlier only if the density at that spot is considerably lower than at the surrounding points, rather than taking the global dataset density into account. 

Due to an increase in the data sizes and sources we have to deal with nowadays, algorithms that work on data streams instead of whole datasets are in high demand. There exist a few approaches to do outlier detection using data streaming [2][3][4]. The KELOS algorithm [4] is an efficient and scalable solution to the problem. However no implementation of such an algorithm in a broadly adopted streaming framework is available. That is why we chose to implement KELOS using Kafka Streams. 

# 4 Related work

# 5 Architecture

## 5.1 Introduction to the KELOS algorithm

The KELOS algorithm [4] computes the Top-N outliers per sliding stream window. It works using Kernel Density Estimators, leveraging the statistical properties of the dataset to compute the density at a specific point.
The key difference to other local outlier detection algorithms is the introduction of abstract kernel centers. Before doing the density calculation, the data points get clustered. Then the density measure is first only computed for theses clusters - weighted by the number of points in it instead of the points themselves.

A kernel function, typically a gaussian probability density function, is used in order to get the density. For efficiency the density is computed heuristaclly per dimension and then multiplied rather then using euclidean distances.

Then an outlier score (KLOME score) gets computed and lower and upper bounds of that score determined for each cluster. Based on these bounds the clusters that will definitely not contain outliers get pruned. Finally for all remaining clusters and the contained points the KLOME scores get calculated again the outliers identified.

## 5.2 Architecture Overview

We stuctured our implementation following the same schema as [4] in their publication. 


## 5.2 Data Abstractor

KELOS uses a micro-clustering approach, where newly arriving data points are simply added to the nearest existing cluster if the distance is smaller than a certain threshold. Otherwise a new cluster with that point will be created. 

For the density kernel a few statistical properties of the clusters have to be stored. These are the *cardinality*, *linear sum* of points per dimension and the *minimum* and *maximum* values per dimension. As these values have additive properties and the kernel is computed per dimension a sliding window semantic can be achieved in the following way:

The whole window gets split into several panes the size of the window step size. The cluster metrics then only have to be computed for pane of the new step size duration. The pane can merely expire and the metrics of the panes in between are kept and merged with the new pane. Figure 1 shows this process.

![Figure 1: Sliding window semantics](./sliding-window-semantic.jpg)

To adapt this windowing technique we set the window step size of our whole Kafka application to the pane size. The clustering is then performed in two Processors. The first (ClusteringProcessor) clusters the points in one cluster pane and forwards the metrics for that pane. The second (AggregationProcessor) merges the last panes with the current one and forwards cluster metrics for the complete window ending at the current timestamp.

## 5.3 Density Estimator

## 5.4 Outlier Detector

# 6 Evaluation

## 6.1 Experimental Setup

## 6.2 Results

# 7 Conclusion and future work

# References

[1] Markus M. Breunig, Hans-Peter Kriegel, Raymond T. Ng, and Jörg Sander.
2000. LOF: Identifying Density-based Local Outliers. In SIGMOD. 93–104.

[2] Dragoljub Pokrajac, Aleksandar Lazarevic, and Longin Jan Latecki. 2007. In-
cremental local outlier detection for data streams. In CIDM. IEEE, 504–515.

[3] MahsaSalehi,ChristopherLeckie,JamesC.Bezdek,TharshanVaithianathan, and Xuyun Zhang. 2016. Fast Memory Efficient Local Outlier Detection in
Data Streams. TKDE 28, 12 (2016), 3246–3260.

[4] Xiao Qin , Lei Cao , Elke A. Rundensteiner and Samuel Madden. 2019. Scalable Kernel Density Estimation-based Local Outlier Detection over Large Data Streams. 22nd International Conference on Extending Database Technology (EDBT)
