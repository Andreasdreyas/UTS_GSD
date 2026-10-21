import argparse
import os
import sys

import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Impor dataset RetailRocket (events.csv) ke database Neo4J."
    )
    parser.add_argument("-u", "--user", default="neo4j", help="Username Neo4j")
    parser.add_argument("-p", "--password", default="new12345", help="Password Neo4j")
    parser.add_argument("-b", "--bolt", default="neo4j://127.0.0.1:7687", help="Neo4j URI")
    # parser.add_argument("-s", "--source", default="data", help="Folder sumber data")
    parser.add_argument("-s", "--source", default="uts-graf/ecommerce-dataset", help="Folder sumber data")
    return parser.parse_args()


def create_constraints(session):
    session.run("CREATE CONSTRAINT visitor_id IF NOT EXISTS "
                "FOR (v:Visitor) REQUIRE v.visitorId IS UNIQUE")
    session.run("CREATE CONSTRAINT item_id IF NOT EXISTS "
                "FOR (i:Item) REQUIRE i.itemId IS UNIQUE")


REL = {"view": "VIEWED", "addtocart": "ADDED_TO_CART", "transaction": "BOUGHT"}


def import_batch(tx, batch):
    # 1) buat simpul
    tx.run("""
        UNWIND $batch AS row
        MERGE (v:Visitor {visitorId: row.visitorid})
        MERGE (i:Item {itemId: row.itemid})
    """, batch=batch)
    # 2) buat relasi sesuai jenis event
    for ev, rel in REL.items():
        extra = ", transactionId: row.transactionid" if ev == "transaction" else ""
        tx.run(f"""
            UNWIND $batch AS row
            WITH row WHERE row.event = '{ev}'
            MATCH (v:Visitor {{visitorId: row.visitorid}})
            MATCH (i:Item {{itemId: row.itemid}})
            CREATE (v)-[:{rel} {{timestamp: row.timestamp{extra}}}]->(i)
        """, batch=batch)


def main():
    args = parse_arguments()

    csv_file_path = os.path.join(args.source, "events.csv")
    if not os.path.exists(csv_file_path):
        print(f"Error: File '{csv_file_path}' tidak ditemukan!")
        sys.exit(1)

    try:
        driver = GraphDatabase.driver(args.bolt, auth=(args.user, args.password))
        driver.verify_connectivity()
    except Exception as e:
        print(f"Gagal terhubung ke database: {e}")
        sys.exit(1)

    batch_size = 20000

    with driver.session() as session:
        create_constraints(session)

        print(f"Membaca file data dari: {csv_file_path}")
        with open(csv_file_path, encoding="utf-8") as f:
            total_rows = sum(1 for _ in f) - 1
        total_chunks = (total_rows // batch_size) + 1
        chunks = pd.read_csv(csv_file_path, chunksize=batch_size)

        print("Memulai proses impor data ke Neo4j...")
        for chunk in tqdm(chunks, total=total_chunks, desc="Mengimpor Batch Data"):
            # Bersihkan null/NaN pada transactionid (ubah 123.0 jadi "123")
            chunk["transactionid"] = (
                chunk["transactionid"].astype("Int64").astype(str).replace("<NA>", "")
            )
            records = chunk.to_dict(orient="records")
            session.execute_write(import_batch, records)

    driver.close()
    print("Proses impor berhasil.")


if __name__ == "__main__":
    main()