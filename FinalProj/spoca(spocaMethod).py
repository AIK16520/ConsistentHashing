import collections
import time
import numpy as np
import seaborn as sns
import math
from matplotlib import pyplot as plt
import mmh3
import os
import pandas as pd
import csv
import random
SEED=480
DATASIZE=10000

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
            key=mmh3.hash(request,SEED)%self.totalNodes
            while key>=self.totalCapacity:
                key=mmh3.hash(key,SEED)%self.totalNodes
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
        start=time.time()
        print("Finding server for "+request)
        serverKey=self.findServerKey(request)
        print("Placed in server "+self.servers[serverKey].name)
        self.servers[serverKey].add_request(request)
        self.totalReq+=1
        end=time.time()
        return end-start
    
    def findServerKey(self, request):
        # print("Total capacity: "+str(self.totalCapacity))
        key=mmh3.hash(request,SEED)%self.totalNodes
        if request in self.recentRequests:
            key=mmh3.hash(bytes(self.requestHistory[request]),SEED)%self.totalNodes
        while key>=self.totalCapacity:
            # print("New key: "+str(key))
            key=mmh3.hash(bytes(key),SEED)%self.totalNodes
        tempKey=key
        serverKey=-1
        while key>0:
            serverKey+=1
            print("serverKey: "+str(serverKey))
            key-=self.servers[serverKey].capacity
        if self.servers[serverKey].alive:
            if len(self.recentRequests)>=self.historyCapacity:
                self.recentRequests.pop(0)
            self.recentRequests.append(request)
            self.requestHistory[request]=tempKey
            return serverKey
        print(self.servers[serverKey])
        self.extraRun+=1
        return self.findServerKey(bytes(tempKey))

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

    def get_total_requests(self):
        return self.totalReq

    def get_total_servers(self):
        return self.totalServer
    
    def get_dead_servers(self):
        dead=0
        for server in self.servers:
                if len(server.requests)==server.capacity:
                        server.alive=False
                        dead+=1
        return dead
    
    def calculate_load_distribution(self):
        used_capacity=0
        total_capacity=0
        
        for server in self.servers:
            used_capacity+=server.numRequests()
            total_capacity+=server.capacity
        return float(used_capacity)/total_capacity
    
    def get_active_servers(self):
        not_dead=0
        for server in self.servers:
            if server.alive and server.numRequests()>0:
                not_dead+=1
        return not_dead
    
    def get_alive_servers(self):
        not_dead=0
        for server in self.servers:
            if server.alive:
                not_dead+=1
        return not_dead

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
  
    
    for i in range(num_servers//2):
        print(i)
        ring.add_Server(f"Server{i}", server_capacity[0])
    for x in range(num_servers//2,num_servers):
        print(x)
        
        ring.add_Server(f"Server{x}", server_capacity[1])

    
    print("adding req")
    for i in range(len(all_requests)):
        time=(ring.add_newRequest(all_requests[i]))
        
        
        time_taken.append(time)
        loadDist.append(ring.calculate_load_distribution())
        deadServer.append(ring.get_dead_servers())
        active_servers_data.append(ring.get_active_servers())
        alive_servers_data.append(ring.get_alive_servers())
        health_status_data.append([server.numRequests() for server in ring.servers if isinstance(server, Server)])
        server_index = mmh3.hash(all_requests[i], SEED) % total_nodes
        latency.append((i,server_index,time_taken[i]))
    
    

    heavy_hitters_map={}
    infrequent_hitter_map={}
    for server in ring.servers:
        heavy_hitters_map[server.name]=0
        infrequent_hitter_map[server.name]=0


    for server in ring.servers:
        
        print (server.name)
        if isinstance(server, Server):
    
            
            countReq=collections.Counter(server.requests)
            for key,val in countReq.items():
                if val>=threshold*server.capacity:
                    
                    heavy_hitters_map[server.name]+=1
                 
                else:
                    
                    infrequent_hitter_map[server.name]+=1
                  
                
            
            
            
            plt.figure(figsize=(8, 5))
            plt.bar(countReq.keys(), countReq.values())
            plt.axhline(y=threshold*server.capacity, color='red', linestyle='--', label=f'Threshold ({threshold*server.capacity})')
            plt.title(f"Heavy Hitters for Server {server.name} total servers: {num_servers}, threshold{threshold}")
            plt.xlabel("Requests")                
            plt.ylabel("Request Count")
            plt.xticks(rotation=45, ha='right')
            figure=os.path.join(currDir,"SpocaHistoryOutputs",f"CHBaseline-HHFS{server.name}_sNum{serverNum}_cap{server_capacity},thold{threshold}.png")
            plt.savefig(figure)
            # plt.show()
            

    


   
    plt.figure(figsize=(12, 8))
    plt.plot(loadDist, deadServer, label='Dead Server vs Load Distribution')
    plt.title(f'System Load Over Time total servers: {num_servers}, threshold{threshold}')
    plt.xlabel('Load Distribution')
    plt.ylabel('Dead Server')
    plt.legend()
    figure=os.path.join(currDir,"SpocaHistoryOutputs",f"CHBaseline-SLOT_sNum{serverNum}_cap{server_capacity},thold{threshold}.png")
    plt.savefig(figure)
    # plt.show()


    plt.figure(figsize=(12, 8))
    plt.plot(range(1, len(all_requests) + 1), deadServer,  color='red', label='Dead Servers')
    plt.plot(range(1, len(all_requests) + 1), active_servers_data,  color='orange', label='Active Servers')
    plt.plot(range(1, len(all_requests) + 1), alive_servers_data,  color='green', label='Alive Servers')
    
    plt.title(f'Server Health Status Over Iterations total servers: {num_servers}, threshold{threshold}')
    plt.xlabel('Iteration')
    plt.ylabel('Number of Servers')
    plt.legend()
    figure=os.path.join(currDir,"SpocaHistoryOutputs",f"CHBaseline-SHSOI_sNum{serverNum}_cap{server_capacity},thold{threshold}.png")
    plt.savefig(figure)
    # plt.show()
    health_status_per_server = list(zip(*health_status_data))

    
    plt.figure(figsize=(12, 8))
    sns.heatmap(health_status_per_server, cmap="YlGnBu", annot=True, fmt="d", xticklabels=1, yticklabels=1)
    
    plt.title(f'Server Health Status Heatmap Over Iterations total servers: {num_servers}, threshold{threshold}')
    plt.xlabel('Iteration')
    plt.ylabel('Server Index')
    figure=os.path.join(currDir,"SpocaHistoryOutputs",f"SHSHOI_sNum{serverNum}_cap{server_capacity},thold{threshold}.png")
    plt.savefig(figure)
    # plt.show()

    plt.figure(figsize=(12, 8))
    plt.scatter(range(1, len(time_taken) + 1), time_taken)
    plt.title(f'Time vs. Request Index total servers: {num_servers}, threshold{threshold}')
    plt.xlabel('Request Index')
    plt.ylabel('Time (seconds)')
    plt.grid(True)
    figure=os.path.join(currDir,"SpocaHistoryOutputs",f"TVRI_sNum{serverNum}_cap{server_capacity},thold{threshold}.png")
    plt.savefig(figure)
    # plt.show()



    ts = int(server.capacity * threshold)

    

    # Count heavy hitters and infrequent hitters for each server

    heavy_hitter_counts = [(heavy_hitters_map[server.name]) if server.name in heavy_hitters_map else 0 for server in ring.servers]
    infrequent_hitter_counts = [(infrequent_hitter_map[server.name]) if server.name in infrequent_hitter_map else 0 for server in ring.servers]
    print(heavy_hitter_counts,heavy_hitters_map)
    print(infrequent_hitter_counts,infrequent_hitter_map)
    # Visualize heavy hitters and infrequent hitters for each server
    bar_width = 0.35
    bar_positions = np.arange(num_servers)

    fig, ax = plt.subplots(figsize=(10, 6))

    bar1 = ax.bar(bar_positions, heavy_hitter_counts, bar_width, label='Heavy Hitters')
    bar2 = ax.bar(bar_positions, infrequent_hitter_counts, bar_width, label='Infrequent Hitters', bottom=heavy_hitter_counts)

    ax.set_xlabel('Server Index')
    ax.set_ylabel('Count')
    ax.set_title(f'Counts of Heavy Hitters and Infrequent Hitters for Each Server total servers: {num_servers}, threshold{threshold}')
    ax.set_xticks(bar_positions)
    ax.set_xticklabels([f'Server {i}' for i in range(num_servers)])
    ax.legend()
    figure=os.path.join(currDir,"SpocaHistoryOutputs",f"CHBaseline-COHHAIHFES_sNum{serverNum}_cap{server_capacity},thold{threshold}.png")
    plt.savefig(figure)
    # plt.show()

#visualization_from_dataset(total_nodes=5000, num_servers=9, server_capacity=[50,500], all_requests=all_requests, threshold=0.15)
serverNum=[10,100,500,1000]
threshold=[0.1,0.15,0.25,0.5]
server_cap=[[10,100],[50,500]]
for x in serverNum:
    for y in threshold:
        for z in server_cap:
            visualization_from_dataset(total_nodes=50000, num_servers=x, server_capacity=z, all_requests=all_requests, threshold=y)
