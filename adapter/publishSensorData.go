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
	"math/rand"
	"strconv"
	"time"
)

var (
	blinkerPin     = rpio.Pin(10) //physical pin 19
	hygroThermoPin = 17           //physical pin 11
	deviceClient   *GoSDK.DeviceClient
	//topic          string
	mqttCallback MQTTMessageReceived
)

type message struct {
	Time  time.Time `json:"time"`
	Value float32   `json:"value"`
}

type MQTTMessageReceived func(*mqttTypes.Publish)

func main() {

	deviceClient = GoSDK.NewDeviceClientWithServiceAccountAndAddrs("localhost", "localhost:1883", "ace685ea0bda92f7fdef909ed8ac01", "ACE685EA0B9685A6C4A5ED8290C701", "pi_device", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJhY2U2ODVlYTBiZGE5MmY3ZmRlZjkwOWVkOGFjMDEgOjogcGlfZGV2aWNlIiwic2lkIjoiMWIwMTE5YTktMDA5NC00YThiLThjYzItNjNiMDBhZjQwMTIxIiwidXQiOjMsInR0IjoxLCJleHAiOi0xLCJpYXQiOjE1ODc2Nzc3MDl9.2hPLLZSItooF2V7sp_4dQz9aillm0aLczcMAUo9NQjE")

	err := initMQTT(mqttCallback)
	if err != nil {
		log.Fatal(err)
	}

	subscribe("LED")

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

	//inside platform, create stream services inside edge (one incoming, one outgoing)
	//1. store incoming data into collection on edge
	//2. publish to message relay topic (check suffixes in docs -- from edge to platform and vice versa) /_platform

	//create device objects for hygrometer and thermometer
	//set dial widgets to read from device objects, historical graph reads from collection
	//stream service updates device objects from collection
	//stream service reads switch state and sends to collection

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
	if err := deviceClient.InitializeMQTTWithCallback("pi_device-"+strconv.Itoa(rand.Intn(10000)) /*ensures broker will accept new connection in the case of a retry*/, "", 30, nil, nil, &callbacks); err != nil {
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
