#!/usr/bin/python2.7
#
# Assignment1 Library
#

import psycopg2
import os
import sys

DATABASE_NAME = 'ddsassignment2'

def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def createDB(dbname='ddsassignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
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
    con.commit()
    con.close()

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)

    cur.execute("CREATE TABLE "+ratingstablename+" (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")
    
    loadout = open(ratingsfilepath,'r')
    
    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('UserID','temp1','MovieID','temp3','Rating','temp5','Timestamp'))
    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
    
    cur.close()
    openconnection.commit()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    name = "RangeRatingsPart"
    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" %ratingstablename)
        if not bool(cursor.rowcount):
            print "Please Load Ratings Table first!!!"
            return
        cursor.execute("CREATE TABLE IF NOT EXISTS RangeRatingsMetadata(PartitionNum INT, MinRating REAL, MaxRating REAL)")
        MinRating = 0.0
        MaxRating = 5.0
        step = (MaxRating-MinRating)/(float)(numberofpartitions)    
        i = 0;
        while i < numberofpartitions:
            newTableName = name + `i`
            cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating REAL)" %(newTableName))
            i+=1;

        i = 0;
        while MinRating < MaxRating:
            lowerLimit = MinRating
            upperLimit = MinRating + step
            if lowerLimit < 0:
                lowerLimit = 0.0

            if lowerLimit == 0.0:
                cursor.execute("SELECT * FROM %s WHERE Rating >= %f AND Rating <= %f" %(ratingstablename,lowerLimit, upperLimit))
                rows = cursor.fetchall()
                newTableName = name + `i`
                for row in rows:
                    cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" %(newTableName, row[0], row[1], row[2]))

            if lowerLimit != 0.0:
                cursor.execute("SELECT * FROM %s WHERE Rating > %f AND Rating <= %f" %(ratingstablename,lowerLimit, upperLimit))
                rows = cursor.fetchall()
                newTableName = name + `i`
                for row in rows:
                    cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" %(newTableName, row[0], row[1], row[2]))
            cursor.execute("INSERT INTO RangeRatingsMetadata (PartitionNum, MinRating, MaxRating) VALUES(%d, %f, %f)" %(i,lowerLimit, upperLimit))
            MinRating = upperLimit
            i+=1;

        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    name = "RoundRobinRatingsPart"
    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'"%(ratingstablename))
        if not bool(cursor.rowcount):
            print "Please Load Ratings Table first!!!"
            return
        cursor.execute("CREATE TABLE IF NOT EXISTS RoundRobinRatingsMetadata(PartitionNum INT, TableNextInsert INT)")
        x = 0
        upperLimit = numberofpartitions
        cursor.execute("SELECT * FROM %s" %ratingstablename)
        rows = cursor.fetchall()
        lastInserted = 0 
        for row in rows:
            if x < upperLimit:
                newTableName = name + `x` 
                cursor.execute("CREATE TABLE %s(UserID INT, MovieID INT, Rating REAL)" %(newTableName))
                cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (newTableName, row[0], row[1], row[2]))
                x += 1
                lastInserted = lastInserted+1
                y = (lastInserted % numberofpartitions)
            else:
                newTableName = name + `y` 
                cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (newTableName, row[0], row[1], row[2]))        
                lastInserted = (lastInserted+1)%numberofpartitions
                y = lastInserted
        cursor.execute("INSERT INTO RoundRobinRatingsMetadata (PartitionNum, TableNextInsert) VALUES(%d,%d)" %(numberofpartitions,lastInserted))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


