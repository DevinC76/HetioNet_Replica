import pandas as pd
from pymongo import MongoClient

def load_edges_to_mongodb():
    try:
        client = MongoClient()
        db = client["hetio_db"]
        edges_collection = db["edges"]

        # Read edges.tsv
        df_edges = pd.read_csv("Data/edges.tsv", sep="\t")
        edge_records = df_edges.to_dict(orient="records")

        if not edge_records:
            print("[WARN] edges.tsv is empty.")
            return

        inserted_count = 0
        for edge in edge_records:
            edge_key = {
                "source": edge["source"],
                "target": edge["target"],
                "metaedge": edge["metaedge"]
            }
            # Check for duplicates
            if edges_collection.count_documents(edge_key, limit=1) == 0:
                edges_collection.insert_one(edge_key)
                inserted_count += 1

        print(f"[INFO] Inserted {inserted_count} new edges into MongoDB.")

    except Exception as e:
        print(f"[ERROR] Failed to load edges into MongoDB: {e}")

if __name__ == "__main__":
    load_edges_to_mongodb()
