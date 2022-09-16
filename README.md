# Operating Systems Project

Multithreaded Top-k algorithm using multithreaded IO file proccessing

## Objectives
- To learn how to perform a multi-threaded reading on different workloads.
- To learn how to arrange the parallelism between file reading and file block parsing.
- Understanding how to reduce memory usage by aggregating information.

## Problem

Need to find Top-K most frequent hours in given multiple files. Requires fast IO and searching solution

Input file format:
```
1657459905, ASKJD
1657459803, ASDJKNJ
... 
```
Timestamp range is fixed between 1645491600 and 1679046032. 

Outputs:
```
Top K frequently accessed hour:
Fri Mar 17 16:00:00 2023		3600
Fri Mar 17 15:00:00 2023		3600
Fri Mar 17 14:00:00 2023		3600
Fri Mar 17 13:00:00 2023		3600
Fri Mar 17 12:00:00 2023		3600
```