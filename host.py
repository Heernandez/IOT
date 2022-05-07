import mysql.connector
import json
import paho.mqtt.publish as publicar
import paho.mqtt.client as mqtt

class Host:

    def __init__(self):
        self.mydb = mysql.connector.connect(host="localhost",user="luish",password="luis123",database="Especializacion")
     

    def ejecutar_query(self,payload):
        mycursor = self.mydb.cursor()
        sql = "select * from ESP32A"
        if(payload["ESP"] == "ESP32A"):
            sql = "INSERT INTO esp32a " \
                  "VALUES (current_timestamp(),{},{});".format(
                payload["Temperatura"], payload["Humedad"])

        if(payload["ESP"] == "ESP32B"):
            sql = "INSERT INTO esp32b " \
                  "VALUES (current_timestamp(),{});".format(payload["NuevoEstado"])
        try:
            mycursor.execute(sql)
            self.mydb.commit()
            #self.mydb.close()
        except Exception as e:
            print("Formato incorrecto \n", e)



def on_connect(client, userdata, flags, rc):

    # Topico al que desea suscribirse
    client.subscribe("CargaData")


def on_message(client, userdata, msg):
    # Operacion a realizar cuando llegue un mensaje
    try:
        print(str(msg.topic) + ": " + str(msg.payload))
        
        datoJson = json.loads(msg.payload)
        print(datoJson)
        h.ejecutar_query(datoJson)
    except Exception as e:
        print("Falla ", e)


h = Host()

# Creo un cliente o creo un objeto de tipo cliente
cliente = mqtt.Client()
# Permite definir los parametros de conexión tales como el topico
cliente.on_connect = on_connect
cliente.on_message = on_message  # Determina que hacer cuando llega un mensaje
cliente.connect("broker.hivemq.com", 1883, 60)  # Parametros del broker
# Mantiene el programa corriendo hasta que el usuario decida detenerlo

cliente.loop_forever()
