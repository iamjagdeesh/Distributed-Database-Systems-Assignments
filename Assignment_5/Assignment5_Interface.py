#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
from math import cos, sin, sqrt, atan2, radians

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    business_docs = collection.find({'city': {'$regex':cityToSearch, '$options':"$i"}})
    with open(saveLocation1, "w") as file:
        for business in business_docs:
            name = business['name']
            full_address = business['full_address'].replace("\n", ", ")
            city = business['city']
            state = business['state']
            file.write(name.upper() + "$" + full_address.upper() + "$" + city.upper() + "$" + state.upper() + "\n")

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    business_docs = collection.find({'categories':{'$in': categoriesToSearch}}, {'name': 1, 'latitude': 1, 'longitude': 1, 'categories': 1})
    lat1 = float(myLocation[0])
    lon1 = float(myLocation[1])
    with open(saveLocation2, "w") as file:
        for business in business_docs:
            name = business['name']
            lat2 = float(business['latitude'])
            lon2 = float(business['longitude'])
            d = DistanceFunction(lat2, lon2, lat1, lon1)
            if d <= maxDistance:
                file.write(name.upper() + "\n")

def DistanceFunction(lat2, lon2, lat1, lon1):
    R = 3959
    pi1 = radians(lat1)
    pi2 = radians(lat2)
    delta_pi = radians(lat2-lat1)
    delta_lambda = radians(lon2-lon1)
    a = (sin(delta_pi/2) * sin(delta_pi/2)) + (cos(pi1) * cos(pi2) * sin(delta_lambda/2) * sin(delta_lambda/2))
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    d = R * c

    return d