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
	"time"
)

var (
	blinkerPin     = rpio.Pin(10) //physical pin 19
	hygroThermoPin = 17           //physical pin 11
	deviceClient   *GoSDK.DeviceClient
	//topic          string
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
	deviceClient = GoSDK.NewDeviceClientWithServiceAccountAndAddrs("localhost", "localhost:1883", "", "", "", "") //todo real data

	//init mqtt (lines 109 + 110 of adapter-go-library

	//use deviceclient to sub / pub
	//sub to topic that sets LED state, pass through to setLedState
	err := initMQTT(mqttCallback)
	if err != nil {
		log.Fatal(err)
	}

	subscribe("LED")

	//topics are not defined! pub/sub automagically creates them
	//pub to topic that stores hygrothermo data -- two pubs, one for each
	//add a timer that publishes every (10) sec
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case t := <-ticker.C:
				temperature, humidity := getHygroThermoData()

				hygroPayload := marshal(t, humidity)
				thermoPayload := marshal(t, temperature)

				err := Publish("Hygrometer", hygroPayload)
				if err != nil {
					log.Fatal(err)
				}
				err = Publish("Thermometer", thermoPayload)
				if err != nil {
					log.Fatal(err)
				}

			}

		}
	}()

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

func Publish(topic string, message []byte) error {
	return deviceClient.Publish(topic, message, 0)
}

func initMQTT(messageReceivedCallback MQTTMessageReceived) error {
	mqttCallback = messageReceivedCallback
	callbacks := GoSDK.Callbacks{OnConnectionLostCallback: onConnectionLost, OnConnectCallback: onConnect}
	if err := deviceClient.InitializeMQTTWithCallback("", "", 30, nil, nil, &callbacks); err != nil { //todo clientid
		return fmt.Errorf("failed to connect %s", err.Error())
	}
	return nil
}

func onConnectionLost(_ mqtt.Client, err error) {
	log.Println(err)
}

func onConnect(_ mqtt.Client) {
	log.Println("onConnect - connected to mqtt")
}

func subscribe(topic string) {
	if mqttCallback != nil {
		var cbSubChannel <-chan *mqttTypes.Publish
		var err error
		cbSubChannel, err = deviceClient.Subscribe(topic, 0)
		if err != nil {
			log.Fatal(err)
		}
		go cbMessageListener(cbSubChannel)
	}
}

func cbMessageListener(onPubChannel <-chan *mqttTypes.Publish) {
	for {
		select {
		case message, ok := <-onPubChannel:
			if ok {
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
