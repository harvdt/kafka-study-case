import time
import random
from kafka import KafkaProducer
import json

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # ganti port jika menggunakan port lain
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Fungsi untuk mensimulasikan data suhu
def generate_sensor_data(sensor_id):
    suhu = random.uniform(50.0, 100.0)  # Kisaran suhu antara 50°C hingga 100°C
    return {'sensor_id': sensor_id, 'suhu': round(suhu, 2)}

try:
    while True:
        # Mensimulasikan data dari 3 sensor
        for sensor_id in ["S1", "S2", "S3"]:
            data = generate_sensor_data(sensor_id)
            producer.send('sensor-suhu', value=data)
            print(f"Sent data: {data}")
        time.sleep(1)  # Kirim data setiap 1 detik

except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.close()
