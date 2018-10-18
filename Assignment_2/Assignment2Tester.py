#!/usr/bin/python2.7
#
#Tester for students
# Do not hard code values in your program for ratings.
# table name and input file name.
# Do not close con objects in your program.
# Invalid ranges will not be tested.
# Order of output does not matter, only correctness will be checked.
# Use discussion board extensively to clear doubts.
# Sample output does not correspond to data in test_data.dat.
#

import Assignment1 as Assignment1
import Assignment2_Interface as Assignment2
if __name__ == '__main__':
    try:
        #Creating Database ddsassignment2
        print "Creating Database named as ddsassignment2"
        Assignment1.createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment2 database"
        con = Assignment1.getOpenConnection();

        # Clear the database existing tables
        print "Delete tables"
        Assignment1.deleteTables('all', con);

        # Loading Ratings table
        print "Creating and Loading the ratings table"
        Assignment1.loadRatings('ratings', 'test_data.dat', con);

        # Doing Range Partition
        print "Doing the Range Partitions"
        Assignment1.rangePartition('ratings', 5, con);

        # Doing Round Robin Partition
        print "Doing the Round Robin Partitions"
        Assignment1.roundRobinPartition('ratings', 5, con);

        # Deleting Ratings Table because Point Query and Range Query should not use ratings table instead they should use partitions.
        Assignment1.deleteTables('ratings', con);

        # Calling RangeQuery
        print "Performing Range Query"
        Assignment2.RangeQuery(1.5, 3.5, con, "./rangeResult.txt");
        #Assignment2.RangeQuery(1,4,con, "./rangeResult.txt");

        # Calling PointQuery
        print "Performing Point Query"
        Assignment2.PointQuery(4.5, con, "./pointResult.txt");
        #Assignment2.PointQuery('2,con, "./pointResult.txt");
        
        # Deleting All Tables
        Assignment1.deleteTables('all', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
