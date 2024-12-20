import torch
from torch_geometric.data import HeteroData

# Test importing PyTorch Geometric packages
try:
    import torch_geometric
    import torch_scatter
    import torch_sparse
    import torch_geometric.transforms as T
    print("PyTorch Geometric and its dependencies are successfully imported.")
except ImportError as e:
    print(f"Error importing package: {e}")
    exit(1)

# Create a simple heterograph with 2 types of nodes and edges
# Type 1: 'paper' nodes
# Type 2: 'author' nodes
# Edge types: 'written_by' and 'cites'

data = HeteroData()

# Node data (example features)
data['paper'].x = torch.tensor([[1], [2], [3]], dtype=torch.float)  # 3 papers
data['author'].x = torch.tensor([[4], [5]], dtype=torch.float)     # 2 authors

# Edge data (example connections)
data['paper', 'written_by', 'author'].edge_index = torch.tensor([[0, 1], [0, 1]], dtype=torch.long)
data['paper', 'cites', 'paper'].edge_index = torch.tensor([[0, 2], [2, 0]], dtype=torch.long)

# Print the heterograph's node and edge data
print(f"Node types: {data}")  # Use list() to print the keys

for node_type in data.node_types:
    print(f"{node_type} nodes:")
    print(data[node_type].x)

print(f"\nEdge types: {data.edge_types}")
for edge_type in data.edge_types:
    print(f"Edges of type {edge_type}:")
    print(data[edge_type].edge_index)

# Check if the graph is created properly by printing the heterograph structure
print("\nHeterograph creation is successful!")
