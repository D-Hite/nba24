import torch
from torch_geometric.data import HeteroData
import pandas as pd
from sklearn.preprocessing import StandardScaler

# Load team data (team nodes with TEAM_NODE_ID)
team_data = pd.read_csv("./datasets/team_sample.csv")  # Replace with the correct path

# Load player data (player nodes with TEAM_NODE_ID and PLAYER_NODE_ID)
player_data = pd.read_csv("./datasets/player_sample.csv")  # Replace with the correct path

# 1. Extract and normalize player features
player_features = player_data[['MINUTES', 'EFG_PCT', 'FTA_RATE', 'TM_TOV_PCT', 
                               'OREB_PCT', 'USG_PCT']].values

scaler = StandardScaler()
player_features = scaler.fit_transform(player_features)
player_features = torch.tensor(player_features, dtype=torch.float)

# 2. Extract and normalize team features
team_features = team_data[['EFG_PCT', 'FTA_RATE', 'TM_TOV_PCT', 'OREB_PCT', 
                           'OPP_EFG_PCT', 'OPP_FTA_RATE', 'OPP_TOV_PCT', 'OPP_OREB_PCT']].values

team_features = scaler.fit_transform(team_features)
team_features = torch.tensor(team_features, dtype=torch.float)

# Create HeteroData object for the graph
data = HeteroData()

# 3. Add player and team node features to the graph
data['player'].x = player_features
data['team'].x = team_features

# 4. Create player-team edges
player_team_edges = []
for _, row in player_data.iterrows():
    player_node_id = row['PLAYER_NODE_ID']  # Player's unique node_id
    team_node_id = row['TEAM_NODE_ID']  # Team's node_id (from player dataset)
    player_team_edges.append([player_node_id, team_node_id])  # Edge from player to team

# Convert player-team edge list to tensor
player_team_edge_index = torch.tensor(player_team_edges, dtype=torch.long).t().contiguous()

# Add player-team edges to the graph data
data['player', 'plays_for', 'team'].edge_index = player_team_edge_index

# 5. Create edges between home and away teams for the matchups (team-to-team edges)
team_matchup_edges = []
for _, home_team_data in team_data.iterrows():
    game_id = home_team_data['GAME_ID']
    home_team_node_id = home_team_data['TEAM_NODE_ID']
    away_team_data = team_data[team_data['GAME_ID'] == game_id].iloc[1]  # Assuming the second row is the away team
    away_team_node_id = away_team_data['TEAM_NODE_ID']
    
    # Add edges for team-to-team matchups (home to away)
    team_matchup_edges.append([home_team_node_id, away_team_node_id])

# Convert team-team edge list to tensor
team_matchup_edge_index = torch.tensor(team_matchup_edges, dtype=torch.long).t().contiguous()

# Add team-to-team edges to the graph data
data['team', 'plays_against', 'team'].edge_index = team_matchup_edge_index

# Print the graph structure to verify
print(f"Heterodata made: {data}")

# # Debugging shapes and types
# print("Player features shape:", player_features.shape)
# print("Team features shape:", team_features.shape)
# print("Edge index dict:", data.edge_index_dict)

print(f"data.x_dict = {type(data.x_dict)}, data.edge_index_dict = {type(data.edge_index_dict)}")


# import torch
# import torch.nn as nn
# from torch_geometric.nn import HeteroConv, GCNConv
# from torch.optim import Adam
# import torch.nn.functional as F
# from sklearn.metrics import mean_squared_error, mean_absolute_error

# class GNNModel(nn.Module):
#     def __init__(self, player_in_channels, team_in_channels, hidden_channels, out_channels):
#         super(GNNModel, self).__init__()

#         # Define HeteroConv layer for the graph
#         self.conv1 = HeteroConv({
#             ('player', 'plays_for', 'team'): GCNConv(player_in_channels, hidden_channels),
#             ('team', 'plays_against', 'team'): GCNConv(team_in_channels, hidden_channels),
#         })

#         # Output layer to predict PLUS_MINUS for each team
#         self.fc = nn.Linear(hidden_channels, out_channels)

#     def forward(self, data):
#         # Debugging: Check if 'x_dict' and 'edge_index_dict' contain tensors
#         print(f"x_dict (player): {data['player'].x.shape}")
#         print(f"x_dict (team): {data['team'].x.shape}")
        
#         # Ensure edge_index_dict is in the correct format
#         print(f"edge_index_dict ('player', 'plays_for', 'team'): {data['player', 'plays_for', 'team'].edge_index.shape}")
#         print(f"edge_index_dict ('team', 'plays_against', 'team'): {data['team', 'plays_against', 'team'].edge_index.shape}")

#         # Apply the HeteroConv layer(s)
#         x = self.conv1(data.x_dict, data.edge_index_dict)

#         # Debugging the shape of the output
#         print("Convolved player features shape:", x['player'].shape)
#         print("Convolved team features shape:", x['team'].shape)

#         # Get the output features for teams
#         team_x = x['team']  # Extract the team node features after propagation

#         # Predict PLUS_MINUS for each team
#         out = self.fc(team_x)
        
#         return out

# # Set input dimensions (based on the dataset's feature dimensions)
# player_in_channels = 6  # 6 features for players
# team_in_channels = 8  # 8 features for teams
# hidden_channels = 64  # Number of hidden channels (you can experiment with this)
# out_channels = 1  # We are predicting a single value (PLUS_MINUS)

# # Initialize the model
# model = GNNModel(player_in_channels, team_in_channels, hidden_channels, out_channels)

# # Assuming 'PLUS_MINUS' is the target variable for the teams
# labels = torch.tensor(team_data['PLUS_MINUS'].values, dtype=torch.float)

# # Prepare the data for training (HeteroData object `data` created previously)
# train_data = data  # The data object with player and team features and edges
# train_labels = labels  # The target labels for the teams

# # Define optimizer
# optimizer = Adam(model.parameters(), lr=0.01)

# # Training loop
# epochs = 100
# for epoch in range(epochs):
#     model.train()
    
#     optimizer.zero_grad()
#     output = model(train_data)
    
#     # Loss (Mean Squared Error for regression)
#     loss = nn.MSELoss()(output.squeeze(), train_labels)
    
#     loss.backward()
#     optimizer.step()
    
#     if epoch % 10 == 0:
#         print(f'Epoch {epoch}/{epochs}, Loss: {loss.item()}')

# # After training, the model can be used for predictions
# model.eval()
# with torch.no_grad():
#     predictions = model(train_data)

# # Evaluate the model
# mse = mean_squared_error(train_labels, predictions.squeeze())
# mae = mean_absolute_error(train_labels, predictions.squeeze())

# print(f'Mean Squared Error: {mse}')
# print(f'Mean Absolute Error: {mae}')
