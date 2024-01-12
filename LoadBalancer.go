package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
)

type LoadBalancer struct {
	loadBalancerAlgo string // can be "NONE", "LEAST_REQUEST"
	endpoints        []Endpoint
	logWriter        *bufio.Writer

	requestForEndpointCh chan int
	receiveEndpointCh    chan Endpoint
	notifyReqCompletedCh chan int

	endpointReqCounter map[Endpoint]int
	reqEndpoint        map[int]Endpoint
}

/*
Load balancing logic:
	callbacks to both
		sendings a request
		a request is completed
	least request loadbalancing
		- keeps state for which endpoint each req is sent to,
			and a counter for each endpoint
		- sends the next request to whichever endpoint has the
			least requests pending
*/

func (lb *LoadBalancer) startLeastRequestLoadBalancer() {
	lb.requestForEndpointCh = make(chan int)
	lb.receiveEndpointCh = make(chan Endpoint)
	lb.notifyReqCompletedCh = make(chan int)

	lb.endpointReqCounter = make(map[Endpoint]int)
	for _, endpoint := range lb.endpoints {
		lb.endpointReqCounter[endpoint] = 0
	}
	lb.reqEndpoint = make(map[int]Endpoint)

	go lb.handleState()
}

func (lb *LoadBalancer) startNoneLoadBalancer() {
	// ensure that there is only one endpoint
	if len(lb.endpoints) != 1 {
		panic("Load balancer algorithm is NONE, " +
			"but there is more than one endpoint")
	}
}

func (lb *LoadBalancer) StartLoadBalancer() {
	if lb.loadBalancerAlgo == "NONE" {
		lb.startNoneLoadBalancer()
	} else if lb.loadBalancerAlgo == "LEAST_REQUEST" {
		lb.startLeastRequestLoadBalancer()
	} else {
		panic("Invalid load balancer algorithm")
	}
}

func (lb *LoadBalancer) addReqToState(reqNum int, endpoint Endpoint) {
	lb.endpointReqCounter[endpoint]++
	lb.reqEndpoint[reqNum] = endpoint
}

func (lb *LoadBalancer) removeReqFromState(reqNum int) {
	endpoint := lb.reqEndpoint[reqNum]
	lb.endpointReqCounter[endpoint]--
}

func (lb *LoadBalancer) getLeastQueuedEndpoint() Endpoint {

	// return the endpoint that has the least req count
	// 		if there is a tie, break it randomnly

	minCount := lb.endpointReqCounter[lb.endpoints[0]]
	minEndpoints := make([]Endpoint, 0)

	for endpoint, count := range lb.endpointReqCounter {
		if count < minCount {
			minCount = count
			minEndpoints = make([]Endpoint, 0)
			minEndpoints = append(minEndpoints, endpoint)
		} else if count == minCount {
			minEndpoints = append(minEndpoints, endpoint)
		}
	}

	randomMin := minEndpoints[rand.Intn(len(minEndpoints))]

	lb.log(
		fmt.Sprintf(
			"App%d: %v, %v\n",
			lb.endpoints[0].App,
			lb.endpointReqCounter,
			randomMin))

	return randomMin
}

func (lb *LoadBalancer) log(logMessage string) {
	fmt.Fprint(lb.logWriter, logMessage)
	lb.logWriter.Flush()
}

func (lb *LoadBalancer) handleState() {

	logFile, err := os.Create(fmt.Sprintf("lb_app%d.txt", lb.endpoints[0].App))
	check(err)
	defer logFile.Close()
	lb.logWriter = bufio.NewWriter(logFile)

	for {
		select {
		case reqNum := <-lb.requestForEndpointCh:
			leastQueuedEndPoint := lb.getLeastQueuedEndpoint()
			lb.addReqToState(reqNum, leastQueuedEndPoint)
			lb.receiveEndpointCh <- leastQueuedEndPoint

		case reqNum := <-lb.notifyReqCompletedCh:
			lb.removeReqFromState(reqNum)
		}
	}
}

func (lb *LoadBalancer) GetEndpointForReq(reqNum int) Endpoint {

	if lb.loadBalancerAlgo == "NONE" {
		return lb.endpoints[0]
	} else if lb.loadBalancerAlgo == "LEAST_REQUEST" {
		lb.requestForEndpointCh <- reqNum
		return <-lb.receiveEndpointCh
	} else {
		panic("Invalid load balancer algorithm")
	}
}

func (lb *LoadBalancer) NotifyReqCompleted(reqNum int) {
	if lb.loadBalancerAlgo == "NONE" {
		// do nothing
	} else if lb.loadBalancerAlgo == "LEAST_REQUEST" {
		lb.notifyReqCompletedCh <- reqNum
	} else {
		panic("Invalid load balancer algorithm")
	}
}
