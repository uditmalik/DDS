import psycopg2
import re
RATINGS_TABLE_NAME = 'Ratings'


def getopenconnection(user='postgres', password='udit', dbname='dds_assgn1'):

    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(r,filepath, openconnection):

    con=openconnection
    # con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    try:
        cur.execute('CREATE TABLE '+r+' (UserId INTEGER NOT NULL,MovieId INTEGER NOT NULL,Ratings FLOAT );')
    except:
        print "Table not created"
    lines=open(filepath)
    print ("I am here")
    for line in lines:
        parts=line.split("::")

        cur.execute("INSERT INTO "+r+" VALUES  (%s,%s,%s)",(parts[0],parts[1],parts[2],))
    con.commit()
    con.close()



def rangepartition(r, numberofpartitions,openconnection):

    con=openconnection
    cur=con.cursor()
    a=5.0/numberofpartitions

    global lst
    lst= [0]*(numberofpartitions)
    lst.append(0)
    for j in range(numberofpartitions):

        lst[j+1] = lst[j] + a

    for i in range(numberofpartitions):
        alpha = r+str(i+1)
        q= "CREATE TABLE "+alpha+" (UserId INT,MovieId INT,Ratings FLOAT) INHERITS("+r+");"
        cur.execute(q)
    con.commit()
    cur.execute("SELECT * FROM "+r+"")
    rows=cur.fetchall()



    for row in rows:
        for i in range(len(lst)-1):
            if(row[2]>lst[i] and row[2]<=lst[i+1]):
                cur.execute("INSERT INTO "+alpha+" VALUES(%s,%s,%s)", (row[0],row[1],row[2],))



    con.commit()

    cur.close()
    con.close()


def roundrobinpartition(ratingstablename, numberofpartitions,openconnection):
    con=openconnection

    cur=con.cursor()
    for i in range(numberofpartitions):
        q="CREATE TABLE "+ratingstablename+str(i+1)+" (UserId INT,MovieId INT,Ratings FLOAT) INHERITS ("+ratingstablename+");"
        cur.execute(q)
        con.commit()

    cur.execute("SELECT * FROM "+ratingstablename+"")
    rows=cur.fetchall()
    l= 1
    for row in rows:


        if (l) <= numberofpartitions:
            cur.execute("INSERT INTO "+ratingstablename+str(l)+" VALUES(%s,%s,%s)", (row[0],row[1],row[2],))
            l=l+1;

        elif(l>numberofpartitions and l%numberofpartitions!=0):
                cur.execute("INSERT INTO "+ratingstablename+str((l)%numberofpartitions)+" VALUES(%s,%s,%s)", (row[0],row[1],row[2],))
                l=l+1;
        elif(l>numberofpartitions and l%numberofpartitions==0):
            cur.execute("INSERT INTO "+ratingstablename+str(numberofpartitions)+" VALUES(%s,%s,%s)", (row[0],row[1],row[2],))
            l=l+1;

    con.commit()

    cur.close()
    con.close()

def roundrobininsert(ratingstablename, userid, itemid, rating,openconnection):
    con = openconnection
    cur=con.cursor()
    cur.execute(" SELECT COUNT(*) FROM  pg_catalog.pg_inherits ")
    m=cur.fetchone()
    d=str(m)

    f=int(re.sub('[^0-9]','',d))
    lk=[]
    global u
    for i in range(f):
        cur.execute("SELECT COUNT(*) FROM "+ratingstablename+str(i+1)+"")
        rows=cur.fetchall()
        lk.append(rows)

    h=len(lk)
    flag=0
    for i in range(h-1):
        if(lk[i]>lk[i+1]):
            flag=1
            u=i+2
            break
    if(flag==0):
        u=1

    cur.execute("INSERT INTO "+ratingstablename+str(u)+" VALUES(%s,%s,%s)", (userid,itemid,rating,))
    con.commit()

    cur.close()
    con.close()


def rangeinsert(ratingstablename, userid, itemid, rating,openconnection):
    con=openconnection
    cur=con.cursor()
    cur.execute(" SELECT COUNT(*) FROM  pg_catalog.pg_inherits ")
    m=cur.fetchone()
    d=str(m)
    f=int(re.sub('[^0-9]','',d))
    a=5.0/f
    ar= [0]*(f)
    ar.append(0)
    for j in range(f):

        ar[j+1] = ar[j] + a
    for i in range(len(ar)-1):
            if(rating>ar[i] and rating<=ar[i+1]):
                cur.execute("INSERT INTO "+ratingstablename+str(i+1)+" VALUES(%s,%s,%s)", (userid,itemid,rating,))
    con.commit()

    cur.close()
    con.close()

def deletpartitions(ratingstablename,openconnection):
    con=openconnection
    cur=con.cursor()
    cur.execute(" SELECT COUNT(*) FROM  pg_catalog.pg_inherits ")
    m=cur.fetchone()
    d=str(m)
    f=int(re.sub('[^0-9]','',d))
    for sa in range(f):
        cur.execute("DROP TABLE "+ratingstablename+str(sa+1)+";")
    con.commit()
def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = psycopg2.connect(user='postgres', host='localhost', password='udit')
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
