package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"time"
)

type WeightNotification struct {
	apps                 []App
	timeTakenForUpdateMs float64
}

type GlobalLoadBalancer struct {
	Nodes                  []int
	Apps                   []int
	HostCapPerSec          int
	AreStatsLogged         bool
	WeightUpdateIntervalMs int

	requestForEndpointCh chan []Endpoint
	receiveEndpointCh    chan Endpoint
	notifyReqCompletedCh chan int
	newReqCountCh        chan map[int]int
	notifyNewWeightCh    chan WeightNotification

	logWriter *bufio.Writer
}

/*
Load balancing logic:
	callbacks to
		- getting an endpoint for sending a request,
		- informing the load balancer that a request has been completed
	global request loadbalancing
		- keeps state for the number of requests sent to each app
		- updates every k seconds the weights of each app's load balancer
*/

type App struct {
	ReqCount       int
	EndpointWeight map[int]float64
}

func (lb *GlobalLoadBalancer) initializeApps() []App {
	apps := make([]App, len(lb.Apps)+1)
	for i := 1; i <= len(lb.Apps); i++ {
		apps[i] = App{0, make(map[int]float64)}
	}
	return apps
}

func (lb *GlobalLoadBalancer) getWeightsFromGurobi(
	hostCap float64, newReqCounts map[int]int) (string, error) {

	baseURL := "http://localhost:5000"
	resource := "/"
	params := url.Values{}
	params.Add("host_cap", fmt.Sprintf("%f", hostCap))
	params.Add("t0", fmt.Sprintf("%d", newReqCounts[1]))
	params.Add("t1", fmt.Sprintf("%d", newReqCounts[2]))
	params.Add("t2", fmt.Sprintf("%d", newReqCounts[3]))

	u, _ := url.ParseRequestURI(baseURL)
	u.Path = resource
	u.RawQuery = params.Encode()
	urlStr := fmt.Sprintf("%v", u)

	res, err := http.Get(urlStr)
	if err != nil {
		return "", err
	}

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	return string(resBody), nil

}

func (lb *GlobalLoadBalancer) getNewAppEndpointWeights(
	weights string) ([]App, error) {

	var response GurobiResponse
	err := json.Unmarshal([]byte(weights), &response)
	check(err)

	if response.Status != 2 {
		return nil, fmt.Errorf("gurobi returned status %d", response.Status)
	}

	apps := make([]App, len(lb.Apps)+1)
	apps[1] = App{0, make(map[int]float64)}
	apps[2] = App{0, make(map[int]float64)}
	apps[3] = App{0, make(map[int]float64)}

	totalApp1 := response.App1Node1 + response.App1Node2
	apps[1].EndpointWeight[1] = response.App1Node1 / totalApp1
	apps[1].EndpointWeight[2] = response.App1Node2 / totalApp1

	totalApp2 := response.App2Node2 + response.App2Node3
	apps[2].EndpointWeight[2] = response.App2Node2 / totalApp2
	apps[2].EndpointWeight[3] = response.App2Node3 / totalApp2

	totalApp3 := response.App3Node1
	apps[3].EndpointWeight[1] = response.App3Node1 / totalApp3

	return apps, nil
}

func getHostCap(hostCapPerSec int, timeAtStart time.Time) float64 {
	hostCapPerMs := float64(hostCapPerSec) / 1000.0
	currTime := time.Now()
	elapsedTime := currTime.Sub(timeAtStart)
	return hostCapPerMs * float64(elapsedTime.Milliseconds())
}

func (lb *GlobalLoadBalancer) updateWeights() {

	ticker := time.NewTicker(
		time.Duration(lb.WeightUpdateIntervalMs) * time.Millisecond)

	currTime := time.Now()

	for range ticker.C {

		lb.newReqCountCh <- make(map[int]int)
		newReqCounts := <-lb.newReqCountCh

		hostCap := getHostCap(lb.HostCapPerSec, currTime)

		timeBeforeGurobi := time.Now()

		weights, err := lb.getWeightsFromGurobi(hostCap, newReqCounts)

		currTime = time.Now()
		timeTakenForUpdateMs := float64(
			currTime.Sub(timeBeforeGurobi).Microseconds()) / 1000.0

		if err != nil {
			fmt.Println(err)
			continue
		}
		newAppEndpoints, err := lb.getNewAppEndpointWeights(weights)
		if err != nil {
			fmt.Println(err)
			continue
		}
		lb.notifyNewWeightCh <- WeightNotification{
			newAppEndpoints,
			timeTakenForUpdateMs}
	}

}

func (lb *GlobalLoadBalancer) log(logMessage string) {
	fmt.Fprint(lb.logWriter, logMessage)
	lb.logWriter.Flush()
}

func (lb *GlobalLoadBalancer) handleState() {

	apps := lb.initializeApps()

	logFile, err := os.Create("lb_log.txt")
	check(err)
	defer logFile.Close()
	lb.logWriter = bufio.NewWriter(logFile)

	for {
		select {
		case <-lb.newReqCountCh:
			// collect all reqcounts in a map (appNum -> reqCount)
			newReqCounts := make(map[int]int)
			for _, appNum := range lb.Apps {
				newReqCounts[appNum] = apps[appNum].ReqCount
				apps[appNum].ReqCount = 0
			}
			lb.newReqCountCh <- newReqCounts

		case weightNotif := <-lb.notifyNewWeightCh:

			newAppEndpoints := weightNotif.apps

			// update the weights
			for _, appNum := range lb.Apps {
				apps[appNum].EndpointWeight =
					newAppEndpoints[appNum].EndpointWeight
			}

			if lb.AreStatsLogged {
				lb.log(fmt.Sprintf(
					"[%fms] Updated apps: %v\n",
					weightNotif.timeTakenForUpdateMs, apps))
			}

		case endpoints := <-lb.requestForEndpointCh:

			app := endpoints[0].App

			// increment app req count
			apps[app].ReqCount++

			// select an endpoint based on the weights
			selectedEndpoint := endpoints[0]
			randomNum := rand.Float64()
			currWeight := 0.0
			for _, endpoint := range endpoints {
				node := endpoint.Node
				currWeight += apps[app].EndpointWeight[node]
				if randomNum <= currWeight {
					selectedEndpoint = endpoint
					break
				}
			}

			// if lb.AreStatsLogged {
			// 	lb.log(fmt.Sprintf(
			// 		"%v Req from app %d going to send to node %d\n",
			// 		apps,
			// 		endpoints[0].App,
			// 		selectedEndpoint.Node))
			// }

			lb.receiveEndpointCh <- selectedEndpoint

		case node := <-lb.notifyReqCompletedCh:

			if false && lb.AreStatsLogged {
				lb.log(fmt.Sprintf(
					"%v Req completed on node %d\n",
					apps,
					node))
			}

		}
	}
}

func (lb *GlobalLoadBalancer) StartLoadBalancer() {
	lb.requestForEndpointCh = make(chan []Endpoint)
	lb.receiveEndpointCh = make(chan Endpoint)
	lb.notifyReqCompletedCh = make(chan int)
	lb.newReqCountCh = make(chan map[int]int)
	lb.notifyNewWeightCh = make(chan WeightNotification)

	go lb.handleState()

	go lb.updateWeights()
}

func (lb *GlobalLoadBalancer) GetEndpointForReq(endpoints []Endpoint) Endpoint {
	lb.requestForEndpointCh <- endpoints
	return <-lb.receiveEndpointCh
}

func (lb *GlobalLoadBalancer) NotifyReqCompleted(node int) {
	lb.notifyReqCompletedCh <- node
}

/*
Drawbacks:
- only caters to a fixed topology
- gurobi is not implemented natively in Go
	it is accessed through an external flask python server
	adding unnecessary latency
*/
