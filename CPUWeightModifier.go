package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"
)

type CPUWeightModifier struct {
	Nodes                  []int
	Apps                   []int
	HostCapPerSec          int
	AreStatsLogged         bool
	WeightUpdateIntervalMs int

	notifyReqSentCh      chan Endpoint
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

func (lb *CPUWeightModifier) initializeApps() []App {
	apps := make([]App, len(lb.Apps)+1)
	for i := 1; i <= len(lb.Apps); i++ {
		apps[i] = App{0, make(map[int]float64)}
	}
	return apps
}

func (lb *CPUWeightModifier) getWeightsFromGurobi(
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

func (lb *CPUWeightModifier) getNewAppEndpointWeights(
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

	hostCap := float64(lb.HostCapPerSec)

	fmt.Printf("response %v\n", response)

	apps[1].EndpointWeight[1] = response.App1Node1 / hostCap
	apps[1].EndpointWeight[2] = response.App1Node2 / hostCap
	apps[2].EndpointWeight[2] = response.App2Node2 / hostCap
	apps[2].EndpointWeight[3] = response.App2Node3 / hostCap
	apps[3].EndpointWeight[1] = response.App3Node1 / hostCap

	return apps, nil
}

// {'status': 2, 't00': 22.0, 't01': 42.0, 't11': 1.0, 't12': 43.0, 't20': 21.0}
// response {2 22.333333333333332 42.66666666666667 0.6666666666666643 43.333333333333336 21}
// app1-node1: 712 [pod/app1-node1 patched (no change)
// ]
// app1-node2: 1361 [pod/app1-node2 patched
// ]
// app2-node2: 1361 [pod/app2-node2 patched
// ]
// app2-node3: 10 [pod/app2-node3 patched (no change)
// ]
// app3-node1: 712 [pod/app3-node1 patched (no change)
// ]

func executeShellCommand(command string) string {
	cmd := exec.Command("bash", "-c", command)
	stdout, err := cmd.Output()
	check(err)
	return string(stdout)
}

func setCPUShareOnCluster(appNum int, nodeNum int, cpuWeight float64) {

	totalCPUs := 500.0
	cpuReqValue := int(cpuWeight * totalCPUs)
	if cpuReqValue < 1 {
		cpuReqValue = 1
	}

	command := fmt.Sprintf(
		"kubectl patch pod app%d-node%d --patch '{\"spec\":{\"containers\":[{\"name\":\"app%d-node%d\", \"resources\":{\"requests\":{\"cpu\":\"%dm\", \"memory\":\"%dMi\"}}}]}}'",
		appNum, nodeNum, appNum, nodeNum, cpuReqValue, cpuReqValue)

	output := executeShellCommand(command)
	fmt.Printf("app%d-node%d: %d [%s]\n", appNum, nodeNum, cpuReqValue, output)
}

func (lb *CPUWeightModifier) modifyCPUSharesOnCluster(newAppEndpoints []App) {

	// setCPUShareOnCluster(1, 1, 0.5) // newAppEndpoints[1].EndpointWeight[1])
	// setCPUShareOnCluster(1, 2, 1) // newAppEndpoints[1].EndpointWeight[2])

	// setCPUShareOnCluster(2, 2, 0.0) // newAppEndpoints[2].EndpointWeight[2])
	// setCPUShareOnCluster(2, 3, 1)   // newAppEndpoints[2].EndpointWeight[3])

	// setCPUShareOnCluster(3, 1, 0.5) // newAppEndpoints[3].EndpointWeight[1])

	// setCPUShareOnCluster(1, 1, newAppEndpoints[1].EndpointWeight[1])
	// setCPUShareOnCluster(1, 2, newAppEndpoints[1].EndpointWeight[2])

	// setCPUShareOnCluster(2, 2, newAppEndpoints[2].EndpointWeight[2])
	// setCPUShareOnCluster(2, 3, newAppEndpoints[2].EndpointWeight[3])

	// setCPUShareOnCluster(3, 1, newAppEndpoints[3].EndpointWeight[1])

	fmt.Printf("---------\n")
}

func (lb *CPUWeightModifier) updateWeights() {

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

		lb.modifyCPUSharesOnCluster(newAppEndpoints)

		lb.notifyNewWeightCh <- WeightNotification{
			newAppEndpoints,
			timeTakenForUpdateMs}
	}

}

func (lb *CPUWeightModifier) log(logMessage string) {
	fmt.Fprint(lb.logWriter, logMessage)
	lb.logWriter.Flush()
}

func (lb *CPUWeightModifier) handleState() {

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

			if false && lb.AreStatsLogged {
				lb.log(fmt.Sprintf(
					"[%fms] Updated apps: %v\n",
					weightNotif.timeTakenForUpdateMs, apps))
			}

		case endpoint := <-lb.notifyReqSentCh:

			app := endpoint.App

			// increment app req count
			apps[app].ReqCount++

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

func (lb *CPUWeightModifier) Start() {
	lb.notifyReqSentCh = make(chan Endpoint)
	lb.notifyReqCompletedCh = make(chan int)
	lb.newReqCountCh = make(chan map[int]int)
	lb.notifyNewWeightCh = make(chan WeightNotification)

	go lb.handleState()

	go lb.updateWeights()
}

func (lb *CPUWeightModifier) NotifyReqSent(endpoint Endpoint) {
	if lb.notifyReqSentCh == nil {
		return
	}
	lb.notifyReqSentCh <- endpoint
}

func (lb *CPUWeightModifier) NotifyReqCompleted(node int) {
	if lb.notifyReqCompletedCh == nil {
		return
	}
	lb.notifyReqCompletedCh <- node
}
