# ------------------------------TEAM 14------------------------------# 
#-----------------------------Assignment 2---------------------------#

#---Sai Raj Madhur Kumar Channagiri-----1207689518-------------------#
#---Venkata Satya Charan Uppuluri-------1207800707-------------------#
#---Andrew Marshall---------------------1201166633-------------------#
#---Rishabh Wadhawan-------------------1207688543-------------------#
#---Udit Malik--------------------------1207456233-------------------#

#--------------------------------------------------------------------------------------------------------------------------------------#

import psycopg2

DATABASE_NAME = 'dds_assgn1'

# Get Connection
def getopenconnection(user='postgres', password='root', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Load the ratings
#--------------------------------------------------------------------------------------------------------------------------------------#
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    # Create, populate, and process the Ratings table
    cur.execute("DROP TABLE IF EXISTS Ratings")
    # The additional columns will be dropped once the data is copied from the file. This is to avoid lengthy file preprocessing.
    cur.execute("CREATE TABLE Ratings(UserID INTEGER, Holder1 TEXT, MovieID INTEGER, Holder2 TEXT, Rating REAL, Holder3 TEXT, Holder4 INTEGER)")
    cur.execute("COPY Ratings FROM '" + ratingsfilepath + "' WITH DELIMITER ':' CSV")
    cur.execute("ALTER TABLE Ratings DROP COLUMN Holder1, DROP COLUMN Holder2, DROP COLUMN Holder3, DROP COLUMN Holder4")
    
    openconnection.commit()
    cur.close()

#--------------------------------------------------------------------------------------------------------------------------------------#
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()

    # create the RangeMetaData table
    cur.execute("CREATE TABLE RangeMetaData(Low REAL, High REAL, TableName TEXT)")

    N = numberofpartitions
    band = N/5

    # create the fragments and insert the relevent metadata into the RangeMetaData table
    for i in range(0, N):
        cur.execute("CREATE TABLE RatingsRange" + str(i) + "(UserID INTEGER, MovieID INTEGER, Rating REAL)")
        # NOTE: the 'high' of one range and the 'low' of the next range will be the same 
        # number in the metadata table thus the rnage is inclusive of the 'high' but not the 'low'
        low = '0' if i == 0 else str(i*band)
        high = '5' if i == N-1 else str((i+1)*band)
        cur.execute("INSERT INTO RangeMetaData VALUES('" + low + "','" + high + "','RatingsRange" + str(i) + "')")

    # Populate the partitioned tables according to the Ratings value
    cur.execute("SELECT * FROM Ratings")
    rows = cur.fetchall()
    for row in rows:
        cur.execute("SELECT TableName FROM RangeMetaData WHERE Low<" + str(row[2]) + " AND High>=" + str(row[2]))
        tablename = cur.fetchone()[0]

        cur.execute("INSERT INTO " + tablename + " VALUES" + str(row))

    openconnection.commit()
    cur.close()

#--------------------------------------------------------------------------------------------------------------------------------------#
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()

    # create the RRMetaData table
    cur.execute("CREATE TABLE RRMetaData(Num INTEGER, Idx INTEGER)")
    cur.execute("INSERT INTO RRMetaData VALUES('" + str(numberofpartitions) + "','0')")

    for i in range(0, numberofpartitions):
        cur.execute("CREATE TABLE RatingsRR" + str(i) + "(UserID INTEGER, MovieID INTEGER, Rating REAL)")

    # Populate the partitioned tables according to the round robin algorithm
    cur.execute("SELECT * FROM Ratings")
    rows = cur.fetchall()
    for row in rows:
        cur.execute("SELECT * FROM RRMetaData")
        idx = cur.fetchone()[1]
        tablename = "RatingsRR" + str(idx)
        cur.execute("INSERT INTO " + tablename + " VALUES" + str(row))
        # update the index in the metadata table
        cur.execute("UPDATE RRMetaData SET idx = " + str((idx+1)%numberofpartitions) + " WHERE 1=1")

    openconnection.commit()
    cur.close()

#--------------------------------------------------------------------------------------------------------------------------------------#
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM RRMetaData")
    row = cur.fetchone()
    numberofpartitions = row[0]
    idx = row[1]
    tablename = "RatingsRR" + str(idx)
    cur.execute("INSERT INTO " + tablename + " VALUES(" + str(userid) + "," + str(itemid) + "," + str(rating) + ")")
    # update the index in the metadata table
    cur.execute("UPDATE RRMetaData SET idx = " + str((idx+1)%numberofpartitions) + " WHERE 1=1")

    openconnection.commit()
    cur.close()

#--------------------------------------------------------------------------------------------------------------------------------------#
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT TableName FROM RangeMetaData WHERE Low<" + str(rating) + " AND High>=" + str(rating))
    tablename = cur.fetchone()[0]
    cur.execute("INSERT INTO " + tablename + " VALUES(" + str(userid) + "," + str(itemid) + "," + str(rating) + ")")

    openconnection.commit()
    cur.close()
#--------------------------------------------------------------------------------------------------------------------------------------#
def pointquery(ratingstablename, ratingvalue, openconnection):
    cur = openconnection.cursor()
    results = []
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('rangemetadata',))
    rangepartition = cur.fetchone()[0]
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('rrmetadata',))
    rr = cur.fetchone()[0]

    if(rangepartition):

        cur.execute('SELECT tablename FROM rangemetadata WHERE '+str(ratingvalue)+'>=low AND '+str(ratingvalue)+'<=high;')
        fragments = cur.fetchall()
        for frag in fragments:
            
            cur.execute('SELECT * FROM '+str(frag[0])+' WHERE rating = '+str(ratingvalue)+';')
            tuples = cur.fetchall()
            for t in tuples:
                results.append(str(frag[0])+' - '+str(t[0])+' - '+str(t[1])+' - '+str(t[2]))

    
    elif(rr):
        cur.execute('SELECT num from rrmetadata;')
        
        num_partitions = cur.fetchone()[0]

        for i in range(num_partitions):
            
            tablename = 'ratingsrr'+str(i)
            cur.execute('SELECT * FROM '+tablename+' WHERE rating = '+str(ratingvalue)+';')
            tuples = cur.fetchall()
            for t in tuples:
                results.append(tablename+' - '+str(t[0])+' - '+str(t[1])+' - '+str(t[2]))
    f = open('PointQueryOut.txt','w')
    for i in results:
        f.write(str(i)+'\n')
    f.close()    

#--------------------------------------------------------------------------------------------------------------------------------------#
def RangeQuery(Ratings, RatingMinValue, RatingMaxValue, openconnection):
    cur = openconnection.cursor()
    results = []

    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('rangemetadata',))
    rangepartition = cur.fetchone()[0]
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('rrmetadata',))
    rr = cur.fetchone()[0]

    if(rangepartition):

        cur.execute('SELECT tablename FROM rangemetadata WHERE (low>='+str(RatingMinValue)+' AND high<='+str(RatingMaxValue)+' ) OR (low ='+str(RatingMaxValue)+' OR high ='+str(RatingMinValue)+');')
        fragments = cur.fetchall()
        
        for frag in fragments:
            
            cur.execute('SELECT * FROM '+str(frag[0])+' WHERE rating <='+str(RatingMaxValue)+'AND rating >= '+str(RatingMinValue)+';')
            tuples = cur.fetchall()
            for t in tuples:
                results.append(str(frag[0])+' - '+str(t[0])+' - '+str(t[1])+' - '+str(t[2]))
            

    elif(rr):
        cur.execute('SELECT num from rrmetadata;')
        
        num_partitions = cur.fetchone()[0]
        for i in range(num_partitions):
            
            tablename = 'ratingsrr'+str(i)
            cur.execute('SELECT * FROM '+tablename+' WHERE rating >='+str(RatingMinValue)+' AND rating<='+str(RatingMaxValue)+';')
            tuples = cur.fetchall()
            for t in tuples:
                results.append(tablename+' - '+str(t[0])+' - '+str(t[1])+' - '+str(t[2]))


    f = open('RangeQueryOut.txt','w')
    for i in results:
        f.write(str(i)+'\n')
    f.close()

#--------------------------------------------------------------------------------------------------------------------------------------#
def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

#--------------------------------------------------------------------------------------------------------------------------------------#
def deletepartitions(openconnection):
    cur = openconnection.cursor()

    # Find out what partitions have been created
    existsrr = 0
    existsrange = 0
    cur.execute("select * from pg_tables where schemaname='public'")
    rows = cur.fetchall()
    for row in rows:
        if row[1] == "rrmetadata" : 
            existsrr = 1
        if row[1] == "rangemetadata" : 
            existsrange = 1

    # delete range partitions and metadata if it exists
    if existsrange :
        cur.execute("SELECT * FROM RangeMetaData")
        rows = cur.fetchall()
        for row in rows:
            tablename = row[2]
            cur.execute("DROP TABLE IF EXISTS " + str(tablename))
        cur.execute("DROP TABLE IF EXISTS RangeMetaData")
    
    # delete round robin partitions and metadata if it exists
    if existsrr :
        cur.execute("SELECT * FROM RRMetaData")
        numtables = cur.fetchone()[0]
        for i in range(0, numtables):
            cur.execute("DROP TABLE IF EXISTS RatingsRR" + str(i))
        cur.execute("DROP TABLE IF EXISTS RRMetaData")

    openconnection.commit()
    cur.close()

#--------------------------------------------------------------------------------------------------------------------------------------#
if __name__ == '__main__':
    try:

        create_db(DATABASE_NAME)

##      loadratings('Ratings', 'C:\Users\chara_000\Desktop\assignment2\ratings.dat', getopenconnection())
        #rangepartition('Ratings', 5, getopenconnection())
        #roundrobinpartition('Ratings', 3, getopenconnection())
##        roundrobininsert("Ratings", 99, 99, 4.5, getopenconnection())
##        rangeinsert("Ratings", 99, 99, 4.5, getopenconnection())
        #deletepartitions(getopenconnection())
        #RangeQuery('ratings',0,5,getopenconnection())
        #pointquery('ratings',0,getopenconnection())

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
#--------------------------------------------------------------------------------------------------------------------------------------#
