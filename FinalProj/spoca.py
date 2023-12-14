import math
import mmh3
import os
import pandas as pd
import csv
import random 
from sklearn.utils import murmurhash3_32
SEED=500

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
        def __init__(self, name, capacity):
                self.name = name
                self.requests = []
                self.capacity=capacity
                self.severOverload = 0
                self.alive = True

        def add_request(self, request):
                if len(self.requests) < self.capacity:
                        self.requests.append(request)
                else:
                        print("SERVER FULL")
                        self.severOverload += 1
                        self.alive = False

        def numRequests(self):
                return len(self.requests)

        def display_requests(self):
                c=len(self.requests)
                print(f"Capacity for Server {self.name}: {c}/{self.capacity} ")

"""
    Represents a SPOCA routing funtion for distributing servers and requests.

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
        self.servers=servers
        self.requests=requests
        self.totalReq=0
        self.totalServer=0
        self.totalCapacity=0
        self.recentRequests=[]
        self.requestHistory={}
        self.historyCapacity=100
        self.extraRun=0
        
        self.totalServer=len(self.servers)
        for server in self.servers:
            self.totalCapacity+=server.capacity

        for request in self.requests:
            key=murmurhash3_32(request,SEED)%self.totalNodes
            while key>=self.totalCapacity:
                key=murmurhash3_32(key,SEED)%self.totalNodes
            serverKey=-1
            while key>0:
                serverKey+=1
                key-=self.servers[serverKey].capacity
            self.servers[serverKey].add_request(request)

    def add_multiple_Servers(self, number,capacity):
        for x in range(number):
            self.add_Server(f"Server{x}",capacity)

    def add_multiple_Servers_different_capcities(self, number,capacities):
        for x in range(number):
            self.add_Server(f"Server{x}",capacities[x])

    def add_Server(self, Server_name,capacity):
        newServer=Server(Server_name,capacity)
        self.servers.append(newServer)
        self.totalCapacity+=newServer.capacity
        self.totalServer+=1

    def delete_Server(self,Server_name):
        key=0
        while self.servers[key].name != Server_name and key < self.totalServer:
            key+=1
        freeRequests=[]
        if key < self.totalServer:
            self.servers[key].alive = False
            freeRequests=self.servers[key].requests
        for freeRequest in freeRequests:
            self.add_newRequest(freeRequest)

    def add_newRequest(self, request):
        print("Finding server for "+request)
        serverKey=self.findServerKey(request)
        print("Placed in server "+self.servers[serverKey].name)
        self.servers[serverKey].add_request(request)
        self.totalReq+=1
    
    def findServerKey(self, request):
        # print("Total capacity: "+str(self.totalCapacity))
        key=murmurhash3_32(request,SEED)%self.totalNodes
        while key>=self.totalCapacity:
            # print("New key: "+str(key))
            key=murmurhash3_32(key,SEED)%self.totalNodes
        print("Key is "+str(key))
        serverKey=-1
        while key>0:
            serverKey+=1
            print("serverKey: "+str(serverKey))
            key-=self.servers[serverKey].capacity
        if self.servers[serverKey].alive:
            return serverKey
        print(self.servers[serverKey])
        self.extraRun+=1
        return self.findServerKey(key)

    def display_ring(self):
        for server in self.servers:
            server.display_requests()

    def calculate_load_distribution(self):
        used_capacity=0
        total_capacity=0

        for server in self.servers:
            used_capacity+=server.numRequests()
            total_capacity+=server.capacity
        
        return used_capacity/total_capacity

        used_capacity = sum(sum(1 for req in server.requests if req != "") for server in self.ring if server != "")
        total_capacity = sum(server.capacity for server in self.ring if server != "")
        return used_capacity / total_capacity if total_capacity != 0 else 0

    def get_total_requests(self):
        return self.totalReq

    def get_total_servers(self):
        return self.totalServer
    
"""
TESTING RING WITH RANDOM 5 SERVERS
"""
ring = ConsistentHashRing(totalNodes=20)

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
DsRing=ConsistentHashRing(totalNodes=50000, servers=[], requests=[])

DsRing.add_multiple_Servers(100,100)

all_requests = []

with open(testingFile, 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        if row:  # Check if the row is not empty
            all_requests.append(row[0])

for req in all_requests:
    DsRing.add_newRequest(req)

load_distribution = DsRing.calculate_load_distribution()
total_requests = DsRing.get_total_requests()
total_servers = DsRing.get_total_servers()
# efficiency = DsRing.calculate_efficiency()
# load_balancing = DsRing.calculate_load_balancing()
# scaling = DsRing.calculate_scaling()

print(f"Load Distribution: {load_distribution}")
print(f"Total Requests: {total_requests}")
print(f"Total Servers: {total_servers}")
# print(f"Efficiency: {efficiency}")
# print(f"Load Balancing: {load_balancing}")
# print(f"Scaling: {scaling}")