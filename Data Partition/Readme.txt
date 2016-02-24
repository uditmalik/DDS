README

Step1 : call create_db() if database doesn't exist.
step2 : call loadratings() to create the main table and load it with data. If table exists, don't call this function.
step3 : call roundrobinpartition() or rangepartition() to create the partitions and load the fragments with data. 
step4 : once partition for a particular technique is created, comment the function and then run the corresponding insert function.
step5 : before calling other partition technique, delete the existing partitions. 
step5 : for deleting call deletpartition(). This will delete all the partitions for the main table. 

Note : Call only one function at a time. Comment all other functions. Also the connection closes after every function is called.

