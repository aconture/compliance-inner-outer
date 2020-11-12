from influxdb import InfluxDBClient


client = InfluxDBClient(host='172.29.14.123', port=8086, username='admin', password='Welcome1')


json_body = [
    {
        "measurement": "core_history",
        "tags": {
            "ne": "RSC1NA",
            "ok": "63",
            "ok_reserva": "63",
            "revisar_1": "3",
            "finv": "42"
        },
        "time": "2020-11-11T8:01:00Z",
        "fields": {
            "duration": 132
        }
    },
    {
        "measurement": "core_history",
        "tags": {
            "ne": "ROC2.SLO1",
            "ok": "63",
            "okreserva": "63",
            "revisar1": "3",
            "finv": "42"
        },
        "time": "2020-11-11T8:01:00Z",
        "fields": {
            "duration": 132
        }
    },
    {
        "measurement": "core_history",
        "tags": {
            "ne": "ROC2.HOR1",
            "ok": "63",
            "ok_reserva": "63",
            "revisar_1": "3",
            "finv": "42"
        },
        "time": "2020-11-11T8:01:00Z",
        "fields": {
            "duration": 132
        }
    },
]


#client.drop_database('influx_airflow')
client.create_database('influx_airflow')
print(client.get_list_database())


#print ("=============================================================")
#print ("=============================================================")
#print ("=============================================================")
#print ("SALIDA")
#print (json_body)
#client.switch_database('test_influx123')
#client.write_points(json_body)