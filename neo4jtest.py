from neo4j import GraphDatabase

# Replace 'password123' with your actual password
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "hetionet"))

def test_connection():
    with driver.session() as session:
        result = session.run("RETURN 'Neo4j connected' AS message")
        for record in result:
            print(record["message"])

test_connection()
