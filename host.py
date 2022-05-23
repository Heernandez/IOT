import mysql.connector
import json
import paho.mqtt.publish as publicar
import paho.mqtt.client as mqtt
import os
from datetime import datetime
import sqlite3


class Host:

    def __init__(self):
        self.id  = "1573972812"
        self.tkn = "5347196612:AAFZVEr7SNfJT06guh12HoYlMUza4QSSfDA"
        #msg = "Hola IOT"
        self.mydb = sqlite3.connect('Especializacion.db')
        
        #self.mydb = mysql.connector.connect(host="localhost",user="luish",password="luis123",database="Especializacion")
     

    def ejecutar_query(self,payload):
        mycursor = self.mydb.cursor()
        sql = "select * from ESP32A"
        fecha = "'"+datetime.today().strftime('%Y-%m-%d %H:%M:%S')+"'"
        if(payload["ESP"] == "ESP32A"):
            sql = "INSERT INTO esp32a VALUES ({},{},{});".format(fecha,payload["Temperatura"], payload["Humedad"])
            self.notificar(payload["Temperatura"],payload["Humedad"])
        if(payload["ESP"] == "ESP32B"):
            sql = "INSERT INTO esp32b VALUES ({},{});".format(fecha,payload["NuevoEstado"])
        try:
            mycursor.execute(sql)
            mycursor.execute("commit;")
            mycursor.close()
        except Exception as e:

            print("\nFormato incorrecto \n{}\n{}".format(sql,e))

    def notificar(self,T=98,H=98):
        #msg = "{'Temperatura':'92','Humedad':'10'}"
        msg = "Temperatura: {}%0AHumedad: {}".format(T,H)
        msg = msg.replace(" ","%20")
        #completeMsg = "https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}".format(self.tkn,self.id,msg)
        #command = 'curl {}'.format(completeMsg)
        command2 = "curl -G -d 'chat_id={}' -d 'text={}' https://api.telegram.org/bot{}/sendMessage".format(self.id,msg,self.tkn)
        #print(command2)
        os.system(command2)


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
