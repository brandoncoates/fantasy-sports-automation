import pandas as pd

# Load the player name map once
name_map_path = "shared/player_name_map.csv"
name_map_df = pd.read_csv(name_map_path)

# Create a dictionary for faster lookup
name_dict = dict(zip(name_map_df["raw_name"], name_map_df["canonical_name"]))

def normalize_name(name):
    """
    Convert raw player name to canonical version if available.
    If not found, return the original name.
    """
    return name_dict.get(name, name)
