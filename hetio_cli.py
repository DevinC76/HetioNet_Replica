import pandas as pd
from pymongo import MongoClient

def load_databases():
    print("[INFO] Loading HetioNet data into databases...")

    # === Connect to MongoDB ===
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["hetio_db"]
        nodes_collection = db["nodes"]

        # === Clear existing data ===
        nodes_collection.delete_many({})
        print("[INFO] Cleared existing documents in 'nodes' collection.")

        # === Read nodes.tsv ===
        df = pd.read_csv("nodes.tsv", sep="\t")
        records = df.to_dict(orient="records")

        if records:
            nodes_collection.insert_many(records)
            print(f"[INFO] Successfully inserted {len(records)} nodes into MongoDB.")
        else:
            print("[WARN] nodes.tsv is empty.")

    except Exception as e:
        print(f"[ERROR] Failed to load data into MongoDB: {e}")

    print("[INFO] MongoDB node loading complete.")
    input("Press Enter to return to the main menu...")


def run_query_1():
    disease_id = input("Enter Disease ID (e.g., Disease::DOID:263): ").strip()
    print(f"[INFO] Running Query 1 for {disease_id}...")
    # TODO: Implement query using Neo4j and/or MongoDB
    # Example: get_disease_info(disease_id)
    print("[INFO] Query 1 results:")
    # print(result)


def run_query_2():
    print("[INFO] Running Query 2 to find potential new compound treatments...")
    # TODO: Implement query logic for compound-disease inference
    print("[INFO] Query 2 results:")
    # print(result)


def main():
    while True:
        print("\n========== HetioNet Database System ==========")
        print("1. Load Database")
        print("2. Run Query 1 - Disease info and related drugs/genes/locations")
        print("3. Run Query 2 - Discover new compound treatments")
        print("4. Exit")
        print("==============================================")
        choice = input("Enter your choice (1–4): ").strip()

        if choice == '1':
            load_databases()
        elif choice == '2':
            run_query_1()
        elif choice == '3':
            run_query_2()
        elif choice == '4':
            print("Exiting HetioNet system. Goodbye.")
            break
        else:
            print("Invalid input. Please choose a number from 1 to 4.")

if __name__ == "__main__":
    main()
