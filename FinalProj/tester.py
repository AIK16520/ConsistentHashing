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

"""
LOADING DATA SETS
"""

currDir=os.path.dirname(__file__)
trainingFile=os.path.join(currDir, "Datasets","DS1","TrainingHistory.csv")
testingFile=os.path.join(currDir, "Datasets","DS1","TestingHistory.csv")
dSet=os.path.join(currDir,"Datasets","dataset.csv")

SEED=480
DATASIZE=1000

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
        health_status_data.append([server.numRequests() for server in ring.ring if isinstance(server, Server)])
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
            countReq.pop('')
            for key,val in countReq.items():
                if val>=threshold*server.capacity:
                    
                    heavy_hitters_map[server.name]+=1
                 
                else:
                    
                    infrequent_hitter_map[server.name]+=1
                  
                
            
            
            
            plt.figure(figsize=(8, 5))
            plt.bar(countReq.keys(), countReq.values())
            plt.axhline(y=threshold*server.capacity, color='red', linestyle='--', label=f'Threshold ({threshold*server.capacity})')
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
    ax.set_title('Counts of Heavy Hitters and Infrequent Hitters for Each Server')
    ax.set_xticks(bar_positions)
    ax.set_xticklabels([f'Server {i}' for i in range(num_servers)])
    ax.legend()

    plt.show()

visualization_from_dataset(total_nodes=5000, num_servers=9, server_capacity=[50,500], all_requests=all_requests, threshold=0.15)




