import requests
import simplejson as json
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
import psycopg2
import requests

# Connexion à PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    dbname="f1stream_db",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

# Création de la table si elle n'existe pas
cur.execute("""
    CREATE TABLE IF NOT EXISTS drivers (
        driver_number TEXT PRIMARY KEY,
        driver_name TEXT,
        headshot_url TEXT
    );
""")
conn.commit()

# Récupération des pilotes depuis OpenF1
url = "https://api.openf1.org/v1/drivers"
resp = requests.get(url)
drivers = resp.json()

# Insertion ou mise à jour dans la table PostgreSQL
seen = set()
for d in drivers:
    number = d.get("driver_number")
    name = d.get("full_name")
    headshot = d.get("headshot_url")
    
    if number and name and number not in seen:
        seen.add(number)
        cur.execute("""
            INSERT INTO drivers (driver_number, driver_name, headshot_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (driver_number) DO UPDATE SET
                driver_name = EXCLUDED.driver_name,
                headshot_url = EXCLUDED.headshot_url;
        """, (str(number), name, headshot))

conn.commit()
cur.close()
conn.close()

print("✅ Table 'drivers' remplie avec succès.")
BASE_URL = "https://api.openf1.org/v1"

RACE_RESULTS_TOPIC = "race_results_topic"

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"📨 Message delivered to {msg.topic()} [{msg.partition()}]")

def create_topics_if_not_exist(bootstrap_servers, topics):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    existing_topics = admin_client.list_topics(timeout=5).topics.keys()
    new_topics = []
    for topic in topics:
        if topic not in existing_topics:
            print(f"📌 Création du topic Kafka : {topic}")
            new_topics.append(NewTopic(topic, num_partitions=1, replication_factor=1))
    if new_topics:
        fs = admin_client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"✅ Topic créé : {topic}")
            except Exception as e:
                print(f"⚠️ Erreur création topic {topic} : {e}")
    else:
        print("✅ Tous les topics existent déjà.")

def get_all_2023_race_sessions():
    """Récupère toutes les sessions de course (Race) en 2023"""
    r = requests.get(f"{BASE_URL}/sessions", params={"year": 2023})
    r.raise_for_status()
    sessions = r.json()
    race_sessions = [s for s in sessions if s.get("session_name") == "Race"]
    return race_sessions

def get_meetings_dict():
    """Crée un dictionnaire meeting_key -> meeting_name"""
    r = requests.get(f"{BASE_URL}/meetings", params={"year": 2023})
    r.raise_for_status()
    meetings = r.json()
    return {m["meeting_key"]: m["meeting_name"] for m in meetings}

def fetch_session_result(session_key):
    """Récupère le classement final d'une course via session_key"""
    r = requests.get(f"{BASE_URL}/session_result", params={"session_key": session_key})
    if r.status_code == 200:
        return r.json()
    return []

if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "localhost:9092"

    # 1️⃣ Création du topic Kafka
    create_topics_if_not_exist(BOOTSTRAP_SERVERS, [RACE_RESULTS_TOPIC])

    producer = SerializingProducer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    # 2️⃣ Chargement des meetings pour les noms de GP
    meetings_dict = get_meetings_dict()

    # 3️⃣ Récupération des sessions de course 2023
    race_sessions = get_all_2023_race_sessions()
    print(f"📊 {len(race_sessions)} courses trouvées en 2023")

    # 4️⃣ Envoi d'un message toutes les 5 secondes
    for session in race_sessions:
        meeting_key = session["meeting_key"]
        session_key = session["session_key"]
        gp_name = meetings_dict.get(meeting_key, "Inconnu")
        date_start = session["date_start"]

        results = fetch_session_result(session_key)
        if not results:
            continue

        for res in results:
            enriched_data = {
                "grand_prix": gp_name,
                "date": date_start,
                "position": res.get("position"),
                "driver_number": res.get("driver_number"),
                "laps_completed": res.get("number_of_laps"),
                "dnf": res.get("dnf"),
                "gap_to_leader": res.get("gap_to_leader"),
                "meeting_key": meeting_key,
                "session_key": session_key
            }

            producer.produce(
                RACE_RESULTS_TOPIC,
                key=str(res.get("driver_number")),
                value=json.dumps(enriched_data),
                on_delivery=delivery_report
            )
            producer.flush()
            print(f"✅ Envoyé : {enriched_data}")

            # Attendre 5 secondes avant le prochain message
            time.sleep(5)
