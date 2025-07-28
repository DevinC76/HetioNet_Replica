import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase


def load_databases(m_db, n_db):
    print("[INFO] Loading data into databases...")

    #Load data onto MongoDB
    try:
        nodes_collection = m_db["nodes"]
        df = pd.read_csv("Data/nodes.tsv", sep="\t")
        records = df.to_dict(orient="records")

        if records:
            for record in records:
                if nodes_collection.count_documents({"id": record["id"]}, limit=1) == 0: #check if node already exists
                    nodes_collection.insert_one(record)
            print(f"[INFO] Finished inserting nodes into MongoDB.")
        else:
            print("[WARN] nodes.tsv is empty.")

    except Exception as e:
        print(f"[ERROR] Failed to load data into MongoDB: {e}")

    print("[INFO] MongoDB node loading complete.")


    # Load data onto Neo4j
    try:
        with n_db.session() as session:
            #Insert Nodes
            for record in records:
                query = (
                    f"MERGE (n:{record['kind']} {{id: $id}}) "
                    "ON CREATE SET n.name = $name"
                )
                session.run(query, id=record["id"], name=record["name"])
            print(f"[INFO] Nodes processed in Neo4j (new nodes added).")

            #Insert Edges
            edges_df = pd.read_csv("Data/edges.tsv", sep="\t")
            for _, row in edges_df.iterrows():
                rel_type = row["metaedge"].replace(">", "")  # Changing Gr>G to GrG cause neo4j no allow '>'
                query = (
                    f"""
                    MATCH (a {{id: $source}}), (b {{id: $target}})
                    MERGE (a)-[r:{rel_type}]->(b)
                    """
                )
                session.run(query, source=row["source"], target=row["target"])
            print(f"[INFO] Edges processed in Neo4j (new edges added).")

    except Exception as e:
        print(f"[ERROR] Failed to load data into Neo4j: {e}")

    input("\n Press Enter to return to the main menu...")


def query_disease_info(n_db, disease_id):
    query = """
    MATCH (d:Disease {id: $disease_id})
    OPTIONAL MATCH (c1:Compound)-[:CtD]->(d)
    OPTIONAL MATCH (c2:Compound)-[:CpD]->(d)
    OPTIONAL MATCH (g:Gene)-[:GaD]->(d)
    OPTIONAL MATCH (d)-[:DlA]->(a:Anatomy)
    RETURN 
        d.name AS disease_name,
        collect(DISTINCT c1.name) + collect(DISTINCT c2.name) AS drugs,
        collect(DISTINCT g.name) AS genes,
        collect(DISTINCT a.name) AS anatomies
    """

    try:
        with n_db.session() as session:
            result = session.run(query, disease_id=disease_id)
            record = result.single()

            if record:
                print("[INFO] Disease Information:")
                print(f"  - Name: {record['disease_name']}")

                if record['drugs']:
                    print(f"  - Drugs (treat/palliate): {record['drugs']}")
                else:
                    print(f"  - Drugs (treat/palliate): None")

                if record['genes']:
                    print(f"  - Genes (cause): {record['genes']}")
                else:
                    print(f"  - Genes (cause): None")

                if record['anatomies']:
                    print(f"  - Occurs in: {record['anatomies']}")
                else:
                    print(f"  - Occurs in: None")
            else:
                print("[WARN] No disease found with the given ID.")

    except Exception as e:
        print(f"[ERROR] Failed to query disease info: {e}")


def run_query_2(n_db):
    print("[INFO] Running Query 2 to find potential new compound treatments...")

    disease_id = input("Enter existing Disease ID (e.g., Disease::DOID:0050156): ")

    cypher_query = """
    MATCH (d:Disease {id: $disease_id})-[:DlA]->(a:Anatomy)
    MATCH (a)-[ag:AuG|AdG]->(g:Gene)
    MATCH (c:Compound)-[cg:CuG|CdG]->(g)
    WHERE (
        (type(ag) = "AuG" AND type(cg) = "CdG") OR
        (type(ag) = "AdG" AND type(cg) = "CuG")
    )
    AND NOT (c)-[:CtD|CpD]->(d)
    RETURN DISTINCT c.id AS compound_id, c.name AS compound_name
    """

    try:
        with n_db.session() as session:
            result = session.run(cypher_query, {"disease_id": disease_id})
            compounds = result.data()

            if not compounds:
                print("[INFO] No new compound treatments inferred for this disease.")
            else:
                print(f"[INFO] Potential new compound treatments for {disease_id}:")
                for compound in compounds:
                    print(f" - {compound['compound_name']} ({compound['compound_id']})")

    except Exception as e:
        print(f"[ERROR] Failed to run Query 2: {e}")

    input("Press Enter to return to the main menu...")



def main():
    try:
        #MongoDB connection

        print("[INFO] Connecting to MongoDB...")
        client = MongoClient()
        mongo_db = client["hetio_db"]
        print("[INFO] Connected to MongoDB successfully.")

        # Neo4j connection
        print("[INFO] Connecting to Neo4j...")
        neo_db = GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j", "hetionet"),database="hetionet")
        with neo_db.session() as session:
            result = session.run("RETURN 'Connected to Neo4j successfully' AS message")
            for record in result:
                print("[INFO]",record["message"])

    except Exception as e:
        print(f"[ERROR] Could not connect to MongoDB or Neo4j: {e}")
        return
    
    # CLI Loop
    while True:
        print("\n========== HetioNet Database System ==========")
        print("1. Load Database")
        print("2. Info and related drugs/genes/locations")
        print("3. Discover new compound treatments")
        print("4. Exit")
        print("==============================================")
        choice = input("Enter 1–4): ").strip()
        print("\n")
        if choice == '1':
            load_databases(mongo_db,neo_db)
        elif choice == '2':
            query_disease_info(neo_db)
        elif choice == '3':
            run_query_2(neo_db)
        elif choice == '4':
            neo_db.close()
            break
        else:
            print("Invalid input)")

if __name__ == "__main__":
    main()
