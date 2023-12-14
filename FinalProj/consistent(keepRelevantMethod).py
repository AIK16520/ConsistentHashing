import collections
import time
import seaborn as sns
import math
from matplotlib import pyplot as plt
import mmh3
import os
import pandas as pd
import csv
import random
SEED=480
DATASIZE=1000

"""
LOADING DATA SETS
"""

currDir=os.path.dirname(__file__)
trainingFile=os.path.join(currDir, "Datasets","DS1","TrainingHistory.csv")
testingFile=os.path.join(currDir, "Datasets","DS1","TestingHistory.csv")
dSet=os.path.join(currDir,"Datasets","dataset.csv")

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
        
        self.dead=False

  
    def numRequests(self):

        c=0
        for request in self.requests:
            if request!="":
                c+=1
        return c
    def matchingRequests(self, matchRequest):
        c=0
        for request in self.requests:
            if request==matchRequest:
                c+=1
        return c
    def add_request(self, request, force):
        if not force and random.random > math.pow(1-float(self.numRequests()-self.matchingRequests(request)), 2):
            return False
        if self.numRequests()<self.capacity:
            
            self.requests.append(request)
            return True
        else:
            self.dead=True
            return False
            

    def display_requests(self):
        c=0
        for request in self.requests:
            if request!="":
                c+=1
        print(f"Capacity for Server {self.name}: {c}/{self.capacity} ")
        

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
    def __init__(self, totalNodes, servers=set(), requests=[]):
        self.totalNodes=totalNodes
        self.ring=[""]*self.totalNodes
        self.servers=servers
        self.requests=requests
        self.totalReq=0
        self.totalServer=0
        
        # for server in self.servers:
        #     key=mmh3.hash(server,SEED)%self.totalNodes
        #     while self.ring[key]!="":
        #         key+=math.ceil(self.totalNodes/16)
        #     self.ring[key]=server

        # for request in self.requests:
        #     key=mmh3(request,SEED)%self.totalNodes
        #     while self.ring[key]=="":
        #         key+=1
        #     self.ring[key].add_request(request)
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
                    newServer.add_request(tempReq, True)
        self.servers.add(newServer)
        
        

    def delete_Server(self,Server_name):
        key=mmh3.hash(Server_name,SEED)%self.totalNodes
        
        temp=[]
        
        if self.ring[key].name==Server_name:
            
            
            temp=self.ring[key].requests
            
            self.ring[key]=""
        
        
        for req in range (len(temp)):
            
            if temp[req]!="":
                self.add_newRequest(temp[req])
            
    
            
    def calculate_load_distribution(self):
        used_capacity=0
        total_capacity=0

        for server in self.ring:
            if server!="":
                used_capacity+=server.numRequests()
                total_capacity+=server.capacity
        
        return used_capacity/total_capacity
        

    def get_total_requests(self):
        return self.totalReq

    def get_total_servers(self):
        return self.totalServer

    def get_dead_servers(self):
        dead=0
        for server in self.ring:
            if isinstance(server,Server):
                if server.numRequests()==server.capacity:
                    server.dead=True
                    dead+=1
        return dead
    def get_active_servers(self):
        not_dead=0
        for server in self.ring:
            if isinstance(server,Server):
                if server.dead==False and server.numRequests()>0:
                    not_dead+=1
        return not_dead
    def get_alive_servers(self):
        not_dead=0
        for server in self.ring:
            if isinstance(server,Server):
                if server.dead==False:
                    
                    not_dead+=1
        return not_dead
    def add_newRequest(self, request):
        start=time.time()
        
        count=0
        if self.get_alive_servers()>0:

            key=mmh3.hash(request,SEED)%self.totalNodes
            while self.ring[key]=="" :
            

                    key+=1
                    key=key%self.totalNodes
            while self.ring[key].add_request(request, False)==False:
                count+=1
                if count>=100:
                    
                    return
                key+=1
                key=key%self.totalNodes
                while self.ring[key]=="" :
            

                    key+=1
                    key=key%self.totalNodes
            end=time.time()

            return end-start
        else:
            return False
    def display_ring(self):
        for x in range(self.totalNodes):
            
            if self.ring[x]!="":
                
                self.ring[x].display_requests()
        print(f"Dead Servers: {self.get_dead_servers()} ")
        print(f"Active Servers: {self.get_active_servers()} ")
        print(f"Alive Servers: {self.get_alive_servers()} ")
    



# """
# TESTING RING WITH RANDOM 5 SERVERS
# """
# ring = ConsistentHashRing(totalNodes=8)

    

# ring.add_multiple_Servers(5,2) 


# ring.add_newRequest("GET /api/data")
# ring.add_newRequest("POST /api/update")
# ring.add_newRequest("GET /api/users")
# ring.add_newRequest("GET /api/userdsds")
# ring.add_newRequest("GET /api/useasdsdssdrs")
# ring.add_newRequest("DELETE /api/users")
# ring.add_newRequest("PUT /api/users")
# ring.add_newRequest("PATCH /api/users1")
# ring.add_newRequest("PUT /api/users123")
# ring.add_newRequest("DELETE /api/users123")


#     # Display the updated ring with requests
# print("\nUpdated Hash Ring with Requests:")
# ring.display_ring()


all_requests = []

with open(dSet, 'r',errors="ignore") as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        if row:
            if len(all_requests)<DATASIZE:
                all_requests.append(row[2])

# print("WITH DS")
# DsRing=ConsistentHashRing(totalNodes=5000)

# DsRing.add_multiple_Servers(10,300)  
#DsRing.add_newRequest(req)
# DsRing.display_ring()
# load_distribution = DsRing.calculate_load_distribution()


# print(f"Load Distribution: {load_distribution}")

print("expirement")

def visualization_from_dataset(total_nodes, num_servers, server_capacity, all_requests, threshold):
    # Initialize ConsistentHashRing
    ring = ConsistentHashRing(total_nodes)

    # Lists to store data for visualizations
    
    print("adding server")
    loadDist=[]
    deadServer=[]
    active_servers_data=[]
    alive_servers_data=[]
    health_status_data = []
    time_taken=[]
    latency=[]
    timestamps=[]
  

    for i in range(num_servers):
        print(i)
        ring.add_Server(f"Server{i}", server_capacity)

    
    print("adding req")
    for i in range(len(all_requests)):
        time=(ring.add_newRequest(all_requests[i]))
        
        
        time_taken.append(time)
        loadDist.append(ring.calculate_load_distribution())
        deadServer.append(ring.get_dead_servers())
        active_servers_data.append(ring.get_active_servers())
        alive_servers_data.append(ring.get_alive_servers())
        health_status_data.append([server.numRequests() for server in ring.ring if isinstance(server, Server)])
        server_index = mmh3.hash(all_requests[i], SEED) % total_nodes
        latency.append((i,server_index,time_taken[i]))
    
    


    for server in ring.servers:
        
        print (server.name)
        if isinstance(server, Server):
    
            
            countReq=collections.Counter(server.requests)
            countReq.pop('')
            
            
            plt.figure(figsize=(8, 5))
            plt.bar(countReq.keys(), countReq.values())
            plt.axhline(y=threshold*server_capacity, color='red', linestyle='--', label=f'Threshold ({threshold*server_capacity})')
            plt.title(f"Heavy Hitters for Server {server.name}")
            plt.xlabel("Requests")                
            plt.ylabel("Request Count")
            plt.xticks(rotation=45, ha='right')
            plt.show()
            

    


   
    plt.figure(figsize=(12, 8))
    plt.plot(loadDist, deadServer, label='Dead Server vs Load Distribution')
    plt.title('System Load Over Time')
    plt.xlabel('Load Distribution')
    plt.ylabel('Dead Server')
    plt.legend()
    plt.show()


    plt.figure(figsize=(12, 8))
    plt.plot(range(1, len(all_requests) + 1), deadServer,  color='red', label='Dead Servers')
    plt.plot(range(1, len(all_requests) + 1), active_servers_data,  color='orange', label='Active Servers')
    plt.plot(range(1, len(all_requests) + 1), alive_servers_data,  color='green', label='Alive Servers')
    
    plt.title('Server Health Status Over Iterations')
    plt.xlabel('Iteration')
    plt.ylabel('Number of Servers')
    plt.legend()
    plt.show()
    health_status_per_server = list(zip(*health_status_data))

    
    plt.figure(figsize=(12, 8))
    sns.heatmap(health_status_per_server, cmap="YlGnBu", annot=True, fmt="d", xticklabels=1, yticklabels=1)
    
    plt.title('Server Health Status Heatmap Over Iterations')
    plt.xlabel('Iteration')
    plt.ylabel('Server Index')
    plt.show()

    plt.figure(figsize=(12, 8))
    plt.scatter(range(1, len(time_taken) + 1), time_taken)
    plt.title('Time vs. Request Index')
    plt.xlabel('Request Index')
    plt.ylabel('Time (seconds)')
    plt.grid(True)
    plt.show()

    plt.figure(figsize=(12, 8))
    plt.scatter(range(0, len(time_taken)), time_taken)
    plt.title('Time vs. Request Index')
    plt.xlabel('Request Index')
    plt.ylabel('Time (seconds)')
    plt.grid(True)
    plt.show()

visualization_from_dataset(total_nodes=5000, num_servers=10, server_capacity=110, all_requests=all_requests, threshold=0.25)




