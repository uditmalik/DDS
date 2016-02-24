README
Assumption :
1. There is only 1 input file.
2. Input format Tablename:JoinKey:Tuple
3. The remaining tuple should not be separated by “:”
Description :
Map function 1.
Splits the input file on “:”
2. outputs key as join key and value as tablename+tuple
Reduce Function1.
Values of all the same key go to the same reducer
2. Collect values of a particular key in arraylist.
3. In nested loop compare if the table names of the tuples are same or not. If not join the
tuples of the two tables and output key as join key and value as result of concatenation
of two tuples.
4. Remove the first element from the arraylist to prevent duplication.