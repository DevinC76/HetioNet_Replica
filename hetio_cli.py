import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase


def load_databases(m_db, n_db):
    print("[INFO] Loading data into databases...")

    # Load nodes.tsv
    try:
        nodes_collection = m_db["nodes"]
        df_nodes = pd.read_csv("Data/nodes.tsv", sep="\t")
        node_records = df_nodes.to_dict(orient="records")

        if node_records:
            with n_db.session() as session:
                for record in node_records:
                    # Check if node already exists in MongoDB
                    if nodes_collection.count_documents({"id": record["id"]}, limit=1) == 0:
                        # Insert into MongoDB
                        nodes_collection.insert_one(record)

                        # Insert into Neo4j
                        query = (
                            f"MERGE (n:{record['kind']} {{id: $id}}) "
                            "ON CREATE SET n.name = $name"
                        )
                        session.run(query, id=record["id"], name=record["name"])

            print(f"[INFO] Finished inserting nodes into both MongoDB and Neo4j.")
        else:
            print("[WARN] nodes.tsv is empty.")
    except Exception as e:
        print(f"[ERROR] Failed to load nodes: {e}")

    print("[INFO] Node loading complete.")

    # Load edges.tsv
    try:
        edges_collection = m_db["edges"]
        df_edges = pd.read_csv("Data/edges.tsv", sep="\t")
        edge_records = df_edges.to_dict(orient="records")

        if edge_records:
            with n_db.session() as session:
                for edge in edge_records:
                    edge_key = {
                        "source": edge["source"],
                        "target": edge["target"],
                        "metaedge": edge["metaedge"]
                    }
                    # Check if edge exists in MongoDB
                    if edges_collection.count_documents(edge_key, limit=1) == 0:
                        # Insert into MongoDB
                        edges_collection.insert_one(edge_key)

                        # Insert into Neo4j
                        rel_type = edge["metaedge"].replace(">", "")  #Neo no allow > 
                        query = (
                            f"""
                            MATCH (a {{id: $source}}), (b {{id: $target}})
                            MERGE (a)-[r:{rel_type}]->(b)
                            """
                        )
                        session.run(query, source=edge["source"], target=edge["target"])

            print(f"[INFO] Finished inserting edges into both MongoDB and Neo4j.")
        else:
            print("[WARN] edges.tsv is empty.")
    except Exception as e:
        print(f"[ERROR] Failed to load edges: {e}")

    print("[INFO] Edge loading complete.")

    input("\nPress Enter to return to the main menu...")



def query_disease_info(n_db):
    disease_id = input("Enter Disease ID (e.g., Disease::DOID:1234): ")

    query = """
    MATCH (d:Disease {id: $disease_id})
    OPTIONAL MATCH (c1:Compound)-[:CtD]->(d)
    OPTIONAL MATCH (c2:Compound)-[:CpD]->(d)
    OPTIONAL MATCH (d)-[:DaG]->(g:Gene)
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
                disease_name = record['disease_name'].title() if record['disease_name'] else 'Unknown'

                drugs = ', '.join(sorted([drug.title() for drug in record['drugs']])) if record['drugs'] else 'None'
                genes = ', '.join(sorted([gene.title() for gene in record['genes']])) if record['genes'] else 'None'
                anatomies = ', '.join(sorted([anat.title() for anat in record['anatomies']])) if record['anatomies'] else 'None'

                print("[INFO] Disease Information:")
                print(f"Name        : {disease_name}")
                print(f"Drugs       : {drugs}")
                print(f"Genes       : {genes}")
                print(f"Occurs In   : {anatomies}")
            else:
                print("[WARN] No disease found with the given ID.")

    except Exception as e:
        print(f"[ERROR] Failed to query disease info: {e}")



def new_treatment(n_db):
    print("[INFO] Running Query 2 to find potential new compound treatments...")

    disease_id = input("Enter existing Disease ID (e.g., Disease::DOID:0050156): ")

    query = """
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
            result = session.run(query, {"disease_id": disease_id})
            compounds = result.data()

            if not compounds:
                print("[INFO] No compound for this disease.")
            else:
                print(f"[INFO] New compound treatments for {disease_id} are :")
                for compound in compounds:
                    print(f"{compound['compound_name']}")

    except Exception as e:
        print(f"[ERROR] Failed to run: {e}")

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
            new_treatment(neo_db)
        elif choice == '4':
            neo_db.close()
            break
        else:
            print("Invalid input)")

if __name__ == "__main__":
    main()
