import time
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from kafka_producer import delivery_report

# Configuration Kafka commune
conf = {
    'bootstrap.servers': 'localhost:9092',
}

# Consumer configuré pour lire le topic des résultats
consumer = Consumer(conf | {
    'group.id': 'f1-results-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

# Producer optionnel pour republier
producer = SerializingProducer(conf)

if __name__ == "__main__":
    # Abonnement au topic des résultats de course
    consumer.subscribe(['race_results_topic'])
    print("📥 En écoute sur Kafka : race_results_topic...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Erreur Kafka: {msg.error()}")
                    break
            else:
                try:
                    # Décodage JSON
                    data = json.loads(msg.value().decode('utf-8'))

                    # Affichage formaté
                    print(
                        f"🏁 {data.get('grand_prix', 'Inconnu')} | "
                        f"Date: {data.get('date', 'N/A')} | "
                        f"Pos: {data.get('position', 'N/A')} | "
                        f"Driver: #{data.get('driver_number', 'N/A')} | "
                        f"Laps: {data.get('laps_completed', 'N/A')} | "
                        f"DNF: {data.get('dnf', False)} | "
                        f"Gap: {data.get('gap_to_leader', 'N/A')}"
                    )

                    # Republier vers un topic "processed" si besoin
                    processed_topic = "processed_race_results_topic"
                    producer.produce(
                        processed_topic,
                        key=str(data.get("driver_number", "unknown")),
                        value=json.dumps(data),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)

                except Exception as e:
                    print(f"⚠️ Erreur traitement message: {e}")
                    continue

            # Petit délai pour éviter de spammer la console
            time.sleep(0.2)

    except KeyboardInterrupt:
        print("🛑 Arrêt demandé par l'utilisateur.")
    except KafkaException as e:
        print(f"❌ Kafka Exception: {e}")
    finally:
        consumer.close()
