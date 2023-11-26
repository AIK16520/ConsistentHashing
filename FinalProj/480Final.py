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
    def __init__(self, name,capacity):
        self.name = name
        self.requests = [""]
        self.capacity=capacity
        self.requests = [""]*self.capacity

    def add_request(self, request):
        if self.requests[self.capacity-1]=="":
            self.requests.append(request)
        else:
            print("SERVER FULL")

    def display_requests(self):
        print(f"Requests for Server {self.name}:")
        for request in self.requests:
            if request!="":
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
        self.totalReq=0
        self.totalServer=0
        


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
    def add_multiple_Servers(self, number,capacity):
        for x in range(number):
            self.add_Server(f"Server{x}",capacity)

    def add_Server(self, Server_name,capacity):
        newServer=Server(Server_name,capacity)
        key=mmh3.hash(newServer.name,SEED)%self.totalNodes
        while self.ring[key]!="":
            key+=math.ceil(self.totalNodes/16)
        self.totalServer+=1
        self.ring[key]=newServer
        temp=[]
        for server in self.ring:
            if server!="":
                for x in range (math.ceil(self.totalNodes/self.totalServer)):
                    tempReq=server.requests[x]
                    temp.append(tempReq)
                    newServer.add_request(tempReq)
        
        

    def delete_Server(self,Server_name):
        key=mmh3.hash(Server_name,SEED)%self.totalNodes
        print(f"temp is noth")
        temp=[]
        print(f"server is {self.ring[key].name}")
        if self.ring[key].name==Server_name:
            print("in if")
            
            temp=self.ring[key].requests
            
            self.ring[key]=""
        print(f"tmep size is {len(temp)}")
        
        for req in range (len(temp)):
            
            if temp[req]!="":
                self.add_newRequest(temp[req])
            
        print(f"server is later {self.ring[key]}")
        

    def add_newRequest(self, request):
        key=mmh3.hash(request,SEED)%self.totalNodes
        while self.ring[key]=="":
            key+=1
            key=key%self.totalNodes
        self.ring[key].add_request(request)
        self.totalReq+=1
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

    

ring.add_multiple_Servers(5,2) 


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

DsRing.add_multiple_Servers(100,100)

all_requests = []

with open(testingFile, 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        if row:  # Check if the row is not empty
            all_requests.append(row[0])

for req in all_requests:
    
    DsRing.add_newRequest(req)
print("BEFORE DELETE")
DsRing.display_ring()
DsRing.delete_Server("Server93")
print("AFTER DELETE")
DsRing.display_ring()
