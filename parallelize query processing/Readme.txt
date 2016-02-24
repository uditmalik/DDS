PARALLEL SORT:
The parallel sort uses 5 threads and 5 partitions respectively. Given a column name it finds the minimum and maximum values of the column in the table to get the range of the values then I use the range and number of partitions to calculate the metadata range partition table. Once created I perform horizontal range partitioning on the table. Moreover, now that I have 5 partitions. I use multithreading to create parallel threads and run each thread for a partition. 
I sort each partition by using ORDER by Sql Declarative syntax to sort partition. Then after sorting I insert the values into a sorted table with a tuple order of the sorted order of the table.
PARALLEL JOIN: The parallal join of two tables in my program has been implemented as a Partially nested Sort Merge algorithm. Its not fully nested, nor fully sort merge. The way I have implemented is that I rangepartition both the tables into 5 partitions based on the same metatable. Then I using parallel threads to perform join operation on respective partitions, for ex partition1 of table 1 with partition 1 of table 2. Moreover, then I put the result of all the threads the output table.
*NOTE: The parallel join won’t work if the join columns of table1 or table 2 are same then there would be two columns rather than one  column.
Testing: 
*NOTE: The program assumes that a ratings table is already present in the database. Please specify the names of role, password of the database inside the code before testing by editing lines 6-8.
<* PLEASE DELETE OTHER TABLES BEFORE RUNNING THE CODE>
To test parallal sort. Please uncomment the function call (ParallelSort) in the main at the end of the program.
To test parallal join. Please uncomment the function call (ParallelJoin) in the main at the end of the program.
