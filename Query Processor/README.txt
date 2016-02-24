README
The interface2.py file contains methods of both assigment 1 and assignment 2. 
RangeQuery() and PointQuery() functons will automatically look for current partition scheme and query accordingly. It is required to maintain only one type of partition scheme ( either range partition or roundrobin partition) at a time.
The order in which the functions need to be called has been commented in the main function. Previous partitioning scheme need to be deleted using 'deletepartitions()' function prior to creating new partitioning scheme.
