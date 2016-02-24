import psycopg2
import thread
import threading
import re

dbname = "test"
user = "postgres"
password = "data"

def getopenconnection():
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_table(openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("CREATE TABLE movies (userid int , movieid int,rating float);")
    conn.commit()

def insert(userid,movieid,rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("INSERT INTO movies VALUES (%s, %s, %s)",(userid,movieid,rating,))
    conn.commit()

# Range Partioning Algorithm
def rangepartition(tablename, numberofpartitions, minimum, maximum, attr, openconnection):

    # create a cursor
    cur = openconnection.cursor()
    # create the MetaData table
    cur.execute("CREATE TABLE "+tablename+"meta(Low REAL, High REAL, TableName TEXT)")
    #caluculating the band for partioning
    N = numberofpartitions
    band = N/(maximum-minimum)
    # create the fragments and insert the relevent metadata into the RangeMetaData table
    for i in range(0, N):
        cur.execute("CREATE TABLE "+tablename+"part" + str(i) + " AS SELECT * FROM "+tablename+" WHERE FALSE;")
        # NOTE: the 'high' of one range and the 'low' of the next range will be the same 
        # number in the metadata table thus the rnage is inclusive of the 'high' but not the 'low'
        low = '0' if i == 0 else str(i*band)
        high = '5' if i == N-1 else str((i+1)*band)
        cur.execute("INSERT INTO "+tablename+"meta"+" VALUES('" + low + "','" + high + "','"+tablename+"part" + str(i) + "')")

    # Populate the partitioned tables according to the Ratings value
    openconnection.commit()
    cur.execute("select column_name from information_schema.columns where table_name = '"+str(tablename)+"'")
    names = cur.fetchall()  
    a_index=names.index((attr,))
    cur.execute("SELECT * FROM "+tablename)
    rows = cur.fetchall()
    
    for row in rows:
        cur.execute("SELECT TableName FROM "+tablename+"meta WHERE Low<" + str(row[a_index]) + " AND High>=" + str(row[a_index]))
        name = cur.fetchone()[0]
        cur.execute("INSERT INTO " + name + " VALUES" + str(row))

    openconnection.commit()
    cur.close()
    pass

def firstthread(tablename,partition,col,openconnection):

    conn=openconnection
    cur = openconnection.cursor()
    re_part = "Repartitioned_"+tablename+str(partition-1)
    cur.execute("CREATE TABLE "+re_part+" AS SELECT * FROM "+tablename+"part"+str(partition-1)+" ORDER BY "+col+";")
    conn.commit()

def ParallelSort(Table, SortingColumnName, OutputTable, openconnection):

    conn = openconnection
    cur = conn.cursor()
    cur.execute("SELECT MIN("+SortingColumnName+") FROM "+Table+";")
    min_list = cur.fetchall()
    min_tuple = min_list[0]
    minimum = min_tuple[0]

    cur.execute("SELECT MAX("+SortingColumnName+") FROM "+Table+";")
    max_list = cur.fetchall()
    max_tuple = max_list[0]
    maximum = max_tuple[0]

    rangepartition(Table, 5, minimum, maximum, SortingColumnName, conn)
    cur.execute("SELECT COUNT(*) FROM "+Table+"meta;")
    thread_count_list = cur.fetchall()
    thread_count_values = thread_count_list[0]
    thread_count = int(re.sub('[^0-9]','',str(thread_count_values)))
    print thread_count
    thread_list = []

    for t in range(thread_count):

        threads = threading.Thread(firstthread(Table,t+1,SortingColumnName,openconnection))
        thread_list.append(threads)

    for tr in thread_list:
        tr.start()
    for thread in thread_list:
        thread.join()

    cur.execute("CREATE TABLE "+OutputTable+" AS SELECT * FROM "+Table+" WHERE FALSE;")

    cur.execute("ALTER TABLE "+OutputTable+" ADD tupleOrder INT;") 
 
    index=0
    for t in range(thread_count):
        cur.execute("SELECT * FROM Repartitioned_"+Table+str(t)+";")
        data=cur.fetchall();
        for q in data:
            index = index + 1            
            q=q+(index,)
            cur.execute("INSERT INTO "+OutputTable+" VALUES"+str(q)+";")

    conn.commit()

def secondthread(tablename1,tablename2,partition,col1,col2,openconnection):
    conn = openconnection
    cur = conn.cursor()
    new_partition = "Jointable_"+str(partition-1)
    cur.execute("CREATE TABLE "+new_partition+" AS SELECT * FROM "+tablename1+"part"+str(partition-1)+" JOIN "+tablename2+"part"+str(partition-1)+" ON("+tablename1+"part"+str(partition-1)+"."+col1+"="+tablename2+"part"+str(partition-1)+"."+col2+");")    
    openconnection.commit()
    
def ParallelJoin(Table1, Table2,Table1JoinColumn, Table2JoinColumn,OutputTable,openconnection):

    conn = openconnection
    cur = conn.cursor()
    cur.execute("SELECT MIN("+Table1JoinColumn+") FROM "+Table1+";")
    min_list = cur.fetchall()
    min_tuple = min_list[0]
    minimum = min_tuple[0]

    cur.execute("SELECT MAX("+Table2JoinColumn+") FROM "+Table2+";")
    max_list = cur.fetchall()
    max_tuple = max_list[0]
    maximum = max_tuple[0]

    rangepartition(Table1, 5, minimum, maximum, Table1JoinColumn, conn)
    rangepartition(Table2, 5, minimum, maximum, Table2JoinColumn, conn)

    cur.execute("SELECT COUNT(*) FROM "+Table1+"meta;")
    thread_count_list = cur.fetchall()
    thread_count_values = thread_count_list[0]
    thread_count = int(re.sub('[^0-9]','',str(thread_count_values)))
    
    thread_List = []
    for t in range(thread_count):
        threads = threading.Thread(secondthread(Table1,Table2,t+1,Table1JoinColumn,Table2JoinColumn,openconnection))
        thread_List.append(threads)
    for tr in thread_List:
        tr.start()
    for th in thread_List:
        th.join()
    
    cur.execute("CREATE TABLE "+OutputTable +" AS SELECT * FROM Jointable_0 WHERE FALSE;")

    for t in range(thread_count):
        cur.execute("INSERT INTO "+OutputTable +" SELECT * FROM Jointable_"+str(t)+";")        

    conn.commit()

if __name__ == '__main__':
    conn = getopenconnection()
    #ASSUMING THE RATINGS TABLE IS ALREADY PRESENT
    #ParallelSort('ratings','ratings','sorted_movies',conn)
    #ParallelJoin('ratings','test','ratings','rat','join_movies',conn)
    conn.close()
