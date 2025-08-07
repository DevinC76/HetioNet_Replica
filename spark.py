from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, desc, count

def run_spark():
    spark = SparkSession.builder.appName("HetioNet").getOrCreate()

    # Load nodes and edges data
    nodes = spark.read.option("header", True).option("sep", "\t").csv("Data/nodes.tsv")
    edges = spark.read.option("header", True).option("sep", "\t").csv("Data/edges.tsv")

    # Top 5 Drugs by Number of Genes
    compounded_gene = edges.filter(col("metaedge").isin("CdG", "CuG"))
    compounded_gene = compounded_gene.groupBy("source").agg(countDistinct("target").alias("gene"))

    compounded_disease = edges.filter(col("metaedge").isin("CtD", "CpD"))
    compounded_disease = compounded_disease.groupBy("source").agg(countDistinct("target").alias("disease"))

    top_5_Drugs_by_n_of_genes = compounded_gene.join(compounded_disease, on="source", how="outer")
    top_5_Drugs_by_n_of_genes = top_5_Drugs_by_n_of_genes.orderBy(desc("gene")).limit(5)

    print("\nTop 5 Drugs by Number of Genes:")
    top_5_Drugs_by_n_of_genes.show()

    # Number of Diseases Associated with 1, 2, 3, ... Drugs
    drug_disease_edges = edges.filter(col("metaedge").isin("CtD", "CpD"))

    disease_drug_counts = drug_disease_edges.groupBy("target").agg(countDistinct("source").alias("num_drugs"))

    diseases_per_num_drugs = disease_drug_counts.groupBy("num_drugs").agg(count("*").alias("num_diseases")).orderBy(desc("num_diseases")).limit(5)

    print("\nTop 5 Number of Diseases by # of Drugs Association:")
    diseases_per_num_drugs.show()

    # Top 5 Drugs with Most Gene Connections
    drug_names = nodes.filter(col("kind") == "Compound").select("id", "name")

    top_genes = compounded_gene.orderBy(desc("gene")).limit(5)

    top_5_drugs_with_most_gene_connections = top_genes.join(drug_names,top_genes.source == drug_names.id,how="left").select("name", "gene")

    print("\nTop 5 Drugs with Most Gene Connections:")
    top_5_drugs_with_most_gene_connections.show()

