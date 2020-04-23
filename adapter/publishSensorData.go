package main

import (
	"encoding/json"
	"fmt"
	GoSDK "github.com/clearblade/Go-SDK"
	mqttTypes "github.com/clearblade/mqtt_parsing"
	mqtt "github.com/clearblade/paho.mqtt.golang"
	"github.com/d2r2/go-dht"
	"github.com/stianeikeland/go-rpio"
	"log"
	"strings"
	"time"
)

var (
	blinkerPin     = rpio.Pin(10) //physical pin 19
	hygroThermoPin = 17           //physical pin 11
	deviceClient   *GoSDK.DeviceClient
	topic          string
	mqttCallback   MQTTMessageReceived
)

type message struct {
	Time  time.Time `json:"time"`
	Value float32   `json:"value"`
}

type MQTTMessageReceived func(*mqttTypes.Publish)

func main() {

	//initialize go sdk, point to edge (localhost url, system key/secret for the edge)
	//NewDeviceClientWithServiceAccountAndAddrs(localhost, localhost:1883, systemkey, systemsecret, deviceName, service account token string)
	client := GoSDK.NewDeviceClientWithServiceAccountAndAddrs("localhost", "localhost:1883", "", "", "", "") //todo real data

	//init mqtt (lines 109 + 110 of adapter-go-library todo

	//use deviceclient to sub / pub
	//todo sub to topic that sets LED state, pass through to setLedState
	//todo pub to topic that stores hygrothermo data -- two pubs, one for each
	//payload, err := client.Subscribe("LED", 0) //todo check qos
	err := initMQTT("LED", mqttCallback)
	if err != nil {
		log.Fatal(err)
	}
	//res := message{}
	//payload
	//topics are not defined! pub/sub automagically creates them
	//add a timer that publishes every (10) sec
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case t := <-ticker.C:
				_, humidity := getHygroThermoData()

				payload := marshal(t, humidity)

				err2 := client.Publish("Hygrometer", payload, 0)
				if err2 != nil {
					log.Fatal(err2)
				}
			}

		}
	}()
	ticker2 := time.NewTicker(10 * time.Second)
	done2 := make(chan bool)
	go func() {
		for {
			select {
			case <-done2:
				return
			case t := <-ticker2.C:
				temperature, _ := getHygroThermoData()

				payload := marshal(t, temperature)

				err2 := client.Publish("Thermometer", payload, 0)
				if err2 != nil {
					log.Fatal(err2)
				}
			}

		}
	}()
	//subscriber is always listening

	//create collection on platform
	//create deployment to sync to edge

	//inside platform, create stream services inside edge (one incoming, one outgoing)
	//1. store incoming data into collection on edge
	//2. publish to message relay topic (check suffixes in docs -- from edge to platform and vice versa) /_platform

	//set dial widgets to read directly from mqtt datasource, historical graph reads from collection

}

func marshal(t time.Time, v float32) (result []byte) {
	result, err := json.Marshal(message{t, v})
	if err != nil {
		log.Fatal(err)
	}
	return result
}

func unmarshal(payload []byte) (t time.Time, v float32) {
	res := message{}
	err := json.Unmarshal(payload, &res)
	if err != nil {
		log.Fatal(err)
	}
	return res.Time, res.Value
}

func initMQTT(topicToSubscribe string, messageReceivedCallback MQTTMessageReceived) error {
	log.Println("[INFO] initMQTT - Initializing MQTT")
	topic = topicToSubscribe
	mqttCallback = messageReceivedCallback
	callbacks := GoSDK.Callbacks{OnConnectionLostCallback: onConnectLost, OnConnectCallback: onConnect}
	if err := deviceClient.InitializeMQTTWithCallback("", "", 30, nil, nil, &callbacks); err != nil { //todo clientid
		return fmt.Errorf("Failed to initialize MQTT connection: %s", err.Error())
	}
	return nil
}

func onConnectLost(client mqtt.Client, connerr error) {
	log.Printf("[INFO] onConnectLost - Connection to MQTT broker was lost: %s\n", connerr.Error())
}

func onConnect(client mqtt.Client) {
	log.Println("[INFO] OnConnect - Connected to ClearBlade Platform MQTT broker")

	if topic != "" && mqttCallback != nil {
		// this is a bit fragile, relying on a specific error message text to check if error was lack of permissions or not, it it's not we want to retry,
		// but if it is we want to quit out because this won't ever work
		log.Println("[INFO] onConnect - Subscribing to provided topic " + topic)
		var cbSubChannel <-chan *mqttTypes.Publish
		var err error
		for cbSubChannel, err = deviceClient.Subscribe(topic, 0); err != nil; {
			if strings.Contains(err.Error(), "Connection lost before Subscribe completed") {
				log.Fatalf("[FATAL] onConnect - Ensure your device has subscribe permissionns to topic %s\n", topic)
			} else {
				log.Printf("[ERROR] onConnect - Error subscribing to MQTT topic: %s\n", err.Error())
				log.Println("[ERROR] onConnect - Retrying in 30 seconds...")
				time.Sleep(time.Duration(30 * time.Second))
				cbSubChannel, err = deviceClient.Subscribe(topic, 0)
			}
		}
		go cbMessageListener(cbSubChannel)
	} else {
		log.Println("[INFO] onConnect - no topic of mqtt callback supplied, will not subscribe to any MQTT topics")
	}
}

func cbMessageListener(onPubChannel <-chan *mqttTypes.Publish) {
	for {
		select {
		case message, ok := <-onPubChannel:
			if ok {
				log.Printf("[DEBUG] cbMessageListener - message received on topic %s with payload %s\n", message.Topic.Whole, string(message.Payload))
				mqttCallback(message)
				_, value := unmarshal(message.Payload)
				if int(value) != 0 { //dirty dirty bad boy, fix the struct
					setLEDState(true)
				} else {
					setLEDState(false)
				}
			}
		}
	}
}

func getHygroThermoData() (temperature float32, humidity float32) {

	temperature, humidity, _, err :=
		dht.ReadDHTxxWithRetry(dht.DHT11, hygroThermoPin, true, 10)
	if err != nil {
		log.Fatal(err)
	}

	return temperature, humidity
}

func setLEDState(on bool) {
	// Open and map memory to access gpio, check for errors
	if err := rpio.Open(); err != nil {
		log.Fatal(err)
	}

	//Unmap gpio memory when done
	defer func() {
		if err := rpio.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Set pin to output mode
	blinkerPin.Output()

	if on {
		blinkerPin.High()
	} else {
		blinkerPin.Low()
	}
}
