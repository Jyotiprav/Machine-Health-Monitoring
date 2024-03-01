import paho.mqtt.client as paho
def send_critical_alert_to_PLC(broker_ip_address):
    try:
        broker=broker_ip_address #PLC
        port=1883
        def on_publish(client,userdata,result):             #create function for callback
            print("data published \n")
            pass
        client1= paho.Client("Alert Code Publisher")                           #create client object
        client1.on_publish = on_publish                          #assign function to callback
        client1.connect(broker,port)                                 #establish connection
        ret= client1.publish("Alert_CODE","2")                   #publish
        return "Code 2 sent to PLC"
    except Exception as E:
        print(E)