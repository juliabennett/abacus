# abacus

Approximate sliding window counts on streaming data.

## Description 

Scala implementation of Datar-Gionis-Indyk-Motwani (DGIM) algorithm for approximate sliding window counts of ones in a binary stream. For any k <= N for some fixed window length N, the DGIM algorithm approximates a count of the number of ones in the last k positions using sublinear storage. 

Includes example integrations processing live streams of bitcoin transactions and tweets. Stream processing and web communication are powered by a suite of [Akka libraries](https://akka.io/docs/).

[Mining of Massive Datasets](http://www.mmds.org/) is a great reference for learning about DGIM and other stream processing algorithms. 
