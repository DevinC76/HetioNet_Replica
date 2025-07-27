# hetio_cli.py
import pandas as pd
from pymongo import MongoClient
def load_databases():
    print("[INFO] Loading HetioNet data into databases...")
    # TODO: Implement loading logic for MongoDB and Neo4j
    # Example: load_nodes(), load_edges()
    print("[INFO] Databases loaded successfully.")


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
