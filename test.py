# hetio_cli.py
import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase


# Loading nodes.tsv and edges.tsv into MongoDB and Neo4j
def load_databases(m_db, n_db):
    print("[INFO] Loading HetioNet data into databases...")

    # === Load into MongoDB ===
    try:
        nodes_collection = m_db["nodes"]
        nodes_collection.delete_many({})
        print("[INFO] Cleared existing documents in 'nodes' collection.")

        df = pd.read_csv("Sample_data/sample_nodes.tsv", sep="\t")
        records = df.to_dict(orient="records")

        if records:
            nodes_collection.insert_many(records)
            print(f"[INFO] Successfully inserted {len(records)} nodes into MongoDB.")
        else:
            print("[WARN] nodes.tsv is empty.")

    except Exception as e:
        print(f"[ERROR] Failed to load data into MongoDB: {e}")

    print("[INFO] MongoDB node loading complete.")

    # === Load into Neo4j ===
    try:
        with n_db.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("[INFO] Cleared existing data from Neo4j.")

            for record in records:
                session.run(
                    """
                    CREATE (n:`%s` {id: $id, name: $name})
                    """ % record["kind"],
                    id=record["id"],
                    name=record["name"]
                )
            print(f"[INFO] Inserted {len(records)} nodes into Neo4j.")

            edges_df = pd.read_csv("Sample_data/sample_edges.tsv", sep="\t")
            for _, row in edges_df.iterrows():
                session.run(
                    """
                    MATCH (a {id: $source}), (b {id: $target})
                    CREATE (a)-[r:`%s`]->(b)
                    """ % row["metaedge"],
                    source=row["source"],
                    target=row["target"]
                )
            print(f"[INFO] Inserted {len(edges_df)} edges into Neo4j.")

    except Exception as e:
        print(f"[ERROR] Failed to load data into Neo4j: {e}")

    input("Press Enter to return to the main menu...")



def run_query_1(m_db, n_db):
    disease_id = input("Enter Disease ID (e.g., Disease::DOID:263): ").strip()
    print(f"[INFO] Running Query 1 for {disease_id}...")

    cypher_query = """
    MATCH (d:Disease {id: $disease_id})
    OPTIONAL MATCH (drug:Compound)-[:CtD]->(d)
    OPTIONAL MATCH (gene:Gene)<-[:DdG]-(d)
    OPTIONAL MATCH (d)-[:DlA]->(loc:Anatomy)
    RETURN 
      d.name AS disease_name,
      collect(DISTINCT drug.name) AS treating_drugs,
      collect(DISTINCT gene.name) AS downregulated_genes,
      collect(DISTINCT loc.name) AS disease_locations
    """

    try:
        with n_db.session() as session:
            result = session.run(cypher_query, disease_id=disease_id)
            record = result.single()
            if record:
                print(f"Disease Name: {record['disease_name']}\n")

                treating_drugs = record['treating_drugs']
                if treating_drugs and any(treating_drugs):
                    print("Drugs that treat this disease:")
                    for drug in treating_drugs:
                        print(f"  - {drug}")
                else:
                    print("No known drugs that treat this disease.")

                downregulated_genes = record['downregulated_genes']
                if downregulated_genes and any(downregulated_genes):
                    print("\nGenes down-regulated by this disease:")
                    for gene in downregulated_genes:
                        print(f"  - {gene}")
                else:
                    print("\nNo known genes down-regulated by this disease.")

                disease_locations = record['disease_locations']
                if disease_locations and any(disease_locations):
                    print("\nAnatomical locations affected by this disease:")
                    for loc in disease_locations:
                        print(f"  - {loc}")
                else:
                    print("\nNo known anatomical locations associated with this disease.")

            else:
                print("[WARN] No disease found with the given ID.")

    except Exception as e:
        print(f"[ERROR] Query failed: {e}")

    input("\nPress Enter to continue...")

def run_query_2(m_db, n_db):
    print("[INFO] Running Query 2 to find potential new compound treatments...")
    
    # Step 1: Get new disease details
    disease_id = input("Enter new Disease ID (e.g., Disease::DOID:new123): ").strip()
    disease_name = input("Enter Disease Name: ").strip()
    anatomy_name = input("Enter associated Anatomy name (must already exist): ").strip()

    try:
        with n_db.session() as session:
            # Step 2: Create the new disease node
            session.run("""
                MERGE (d:Disease {id: $disease_id})
                SET d.name = $disease_name
            """, disease_id=disease_id, disease_name=disease_name)

            # Step 3: Connect the disease to existing anatomy
            session.run("""
                MATCH (d:Disease {id: $disease_id}), (a:Anatomy {name: $anatomy_name})
                MERGE (d)-[:DlA]->(a)
            """, disease_id=disease_id, anatomy_name=anatomy_name)

            # Step 4: Run the compound inference query
            query = """
            MATCH (d:Disease {id: $disease_id})-[:DlA]->(a:Anatomy)
            MATCH (a)-[reg1:AdG]->(g:Gene)
            MATCH (c:Compound)-[reg2:CuG|CbG]->(g)
            WHERE type(reg1) = 'AdG' AND type(reg2) IN ['CuG', 'CbG']
              AND NOT (c)-[:CtD|CpD]->(d)
              AND (
                (type(reg1) = 'AdG' AND type(reg2) = 'CbG') OR
                (type(reg1) = 'AdG' AND type(reg2) = 'CuG')
              )
            RETURN DISTINCT c.name AS compound_name, c.id AS compound_id
            """

            result = session.run(query, disease_id=disease_id)

            print("\n[INFO] Potential new compound treatments:\n")
            found = False
            for record in result:
                found = True
                print(f"  - {record['compound_name']} (ID: {record['compound_id']})")

            if not found:
                print("  No potential treatments found based on current data.")

    except Exception as e:
        print(f"[ERROR] Failed to run Query 2: {e}")

    input("\nPress Enter to continue...")


def main():
    try:
        #MongoDB connection

        print("[INFO] Connecting to MongoDB...")
        client = MongoClient()
        mongo_db = client["hetio_sample_db"]
        print("[INFO] Connected to MongoDB successfully.")

        # Neo4j connection
        print("[INFO] Connecting to Neo4j...")
        neo_db = GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j", "hetionet"),database="hetionetsample")
        with neo_db.session() as session:
            result = session.run("RETURN 'Connected to Neo4j successfully' AS message")
            for record in result:
                print("[INFO]",record["message"])

    except Exception as e:
        print(f"[ERROR] Could not connect to MongoDB or Neo4j: {e}")
        return
    
    while True:
        print("\n========== HetioNet Database System ==========")
        print("1. Load Database")
        print("2. Run Query 1 - Disease info and related drugs/genes/locations")
        print("3. Run Query 2 - Discover new compound treatments")
        print("4. Exit")
        print("==============================================")
        choice = input("Enter your choice (1–4): ").strip()

        if choice == '1':
            load_databases(mongo_db,neo_db)
        elif choice == '2':
            run_query_1(mongo_db,neo_db)
        elif choice == '3':
            run_query_2(mongo_db, neo_db)
        elif choice == '4':
            print("Exiting HetioNet system. Goodbye.")
            neo_db.close()
            break
        else:
            print("Invalid input. Please choose from 1 to 4.")

if __name__ == "__main__":
    main()
