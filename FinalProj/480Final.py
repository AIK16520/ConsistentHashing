import math
import mmh3
import os
import pandas as pd
import csv
SEED=480
"""
LOADING DATA SETS
"""

currDir=os.path.dirname(__file__)
trainingFile=os.path.join(currDir, "Datasets","DS1","TrainingHistory.csv")
testingFile=os.path.join(currDir, "Datasets","DS1","TestingHistory.csv")

"""
Represents a server in the consistent hash ring.

Attributes:
    - name (str): The name or identifier of the server.
    - requests (list): A list of requests made to the server.

Methods:
    - add_request(request): Adds a request to the server.
    - display_requests(): Displays the server's requests.
    
"""
class Server:
    def __init__(self, name):
        self.name = name
        self.requests = []

    def add_request(self, request):
        self.requests.append(request)

    def display_requests(self):
        print(f"Requests for Server {self.name}:")
        for request in self.requests:
            print(request)

"""
    Represents a consistent hash ring for distributing servers and requests.

Attributes:
    - totalNodes (int): The total number of nodes in the hash ring.
    - ring (list): A list representing the hash ring, with each element containing either an empty string or a Server instance.
    - servers (list): A list containing Server instances.
    - requests (list): A list containing requests to be distributed in the hash ring.

 Methods:
    - add_multiple_Servers(number): Adds multiple servers to the hash ring.
    - add_Server(Server_name): Adds a server to the hash ring.
    - add_newRequest(request): Adds a request to the hash ring.
    - display_ring(): Displays the hash ring along with the requests associated with each server.
    """

class ConsistentHashRing:
    def __init__(self, totalNodes, servers=[], requests=[]):
        self.totalNodes=totalNodes
        self.ring=[""]*self.totalNodes
        self.servers=servers
        self.requests=requests
        


        for server in self.servers:
            key=mmh3.hash(server,SEED)%self.totalNodes
            while self.ring[key]!="":
                key+=math.ceil(self.totalNodes/16)
            self.ring[key]=server

        for request in self.requests:
            key=mmh3(request,SEED)%self.totalNodes
            while self.ring[key]=="":
                key+=1
            self.ring[key].add_request(request)
    def add_multiple_Servers(self, number):
        for x in range(number):
            self.add_Server(f"Server{x}")

    def add_Server(self, Server_name):
        newServer=Server(Server_name)
        key=mmh3.hash(newServer.name,SEED)%self.totalNodes
        while self.ring[key]!="":
            key+=math.ceil(self.totalNodes/16)
        
        self.ring[key]=newServer
    def add_newRequest(self, request):
        key=mmh3.hash(request,SEED)%self.totalNodes
        while self.ring[key]=="":
            key+=1
            key=key%self.totalNodes
        self.ring[key].add_request(request)
    def display_ring(self):
        for x in range(self.totalNodes):
            if self.ring[x]=="":
                print (f"node {x} is empty")
            else:
                self.ring[x].display_requests()

"""
TESTING RING WITH RANDOM 5 SERVERS
"""
ring = ConsistentHashRing(totalNodes=8)

    

ring.add_multiple_Servers(5) 


print("Initial Hash Ring:")
ring.display_ring()


ring.add_newRequest("GET /api/data")
ring.add_newRequest("POST /api/update")
ring.add_newRequest("GET /api/users")

    # Display the updated ring with requests
print("\nUpdated Hash Ring with Requests:")
ring.display_ring()


"""
TESTING USING DATASET
"""
print("WITH DS")
DsRing=ConsistentHashRing(totalNodes=500)

DsRing.add_multiple_Servers(100)

all_requests = []

with open(testingFile, 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        if row:  # Check if the row is not empty
            all_requests.append(row[0])

for req in all_requests:
    
    DsRing.add_newRequest(req)

DsRing.display_ring()

