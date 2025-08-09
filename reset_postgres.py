# reset_postgres_full.py
import psycopg2

POSTGRES_HOST = "localhost"
POSTGRES_DB = "f1stream_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

def reset_database():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()

        print("📌 Suppression de toutes les tables...")
        # Récupérer toutes les tables
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
        """)
        tables = cur.fetchall()

        for schema, table in tables:
            print(f"🗑️ Suppression de la table {schema}.{table}")
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;')

        print("📌 Suppression des schémas personnalisés...")
        # Supprimer tous les schémas sauf ceux par défaut
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public');
        """)
        schemas = cur.fetchall()

        for schema in schemas:
            schema_name = schema[0]
            print(f"🗑️ Suppression du schéma {schema_name}")
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;')

        print("✅ Base PostgreSQL nettoyée avec succès.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    reset_database()
