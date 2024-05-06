import os
import re
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np


def extract_query_ids(file_path):
    query_ids = []
    with open(file_path, 'r') as file:
        for line in file:
            match = re.match(r'-- query_id: (\d+)', line)
            if match:
                query_ids.append(int(match.group(1)))
    return query_ids


def find_query_dependencies(directory):
    dependencies = {}
    id_to_file = {}
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                query_ids = extract_query_ids(file_path)
                file_name = file_path.split('/')[-1].split('.')[0]
                for qid in query_ids:
                    id_to_file[qid] = file_name  # Map each query_id to its file path
                dependencies[file_name] = query_ids
    return dependencies, id_to_file

def build_dependency_graph(directory, id_to_file):
    dependency_graph = nx.DiGraph()
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                file_name = file_path.split('/')[-1].split('.')[0]
                with open(file_path, 'r') as f:
                    content = f.read()

                found_ids = set(re.findall(r'\.query_(\d+)', content))
                for query_id in found_ids:
                    query_id = int(query_id)  # Convert to integer for matching
                    if query_id in id_to_file:
                        dependent_file = id_to_file[query_id]
                        dependency_graph.add_edge(file_name, dependent_file, label=f'query_{query_id}')

    return dependency_graph

def draw_dependencies_graph(dependency_graph, directory = None):
    pos = nx.nx_agraph.graphviz_layout(dependency_graph, prog="fdp", args="")
    nx.draw(dependency_graph, pos, with_labels=True, node_size=200, node_color="skyblue", font_size=7, font_weight="bold")
    plt.title("Query Dependencies")
    
    # Save or show the plot based on the 'save' flag
    if directory:
        base_name = os.path.basename(directory)  # Extracts the last part of the directory path as the base name
        filename = f'{base_name}_dependency_graph.png'  # Constructs filename
        full_path = os.path.join(directory, 'docs', 'imgs', filename)  # Constructs full file path in a safe manner
        plt.savefig(full_path, format='png', dpi=1200) 
        plt.close() 
    else:
        plt.show() 


if __name__ == "__main__":
    directory = input("Enter directory path: ").strip()
    dependencies, id_to_file = find_query_dependencies(directory)
    
    print("Query ID to File Mapping:")
    for query_id, file_path in id_to_file.items():
        print(f"Query ID {query_id} is in file {file_path}")
    
    dependency_graph = build_dependency_graph(directory, id_to_file)
    
   #print("Dependency Graph:")
   # print(dependency_graph.edges(data=True))

    draw_dependencies_graph(dependency_graph, directory)