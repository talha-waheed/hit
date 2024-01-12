package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
)

type NodalLoadBalancer struct {
	Nodes          []int
	AreStatsLogged bool

	requestForEndpointCh chan []Endpoint
	receiveEndpointCh    chan Endpoint
	notifyReqCompletedCh chan int

	nodeReqCounter []int

	logWriter *bufio.Writer
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

func (lb *NodalLoadBalancer) startLeastRequestLoadBalancer() {
	lb.requestForEndpointCh = make(chan []Endpoint)
	lb.receiveEndpointCh = make(chan Endpoint)
	lb.notifyReqCompletedCh = make(chan int)

	lb.nodeReqCounter = make([]int, lb.Nodes[len(lb.Nodes)-1]+1)

	go lb.handleState()
}

func (lb *NodalLoadBalancer) StartLoadBalancer() {
	lb.startLeastRequestLoadBalancer()
}

func (lb *NodalLoadBalancer) addReqToState(node int) {
	lb.nodeReqCounter[node]++
}

func (lb *NodalLoadBalancer) removeReqFromState(node int) {
	lb.nodeReqCounter[node]--
}

func (lb *NodalLoadBalancer) getLeastQueuedEndpoint(endpoints []Endpoint) Endpoint {

	// return the endpoint that has the least req count
	// 		if there is a tie, break it randomnly

	minCount := lb.nodeReqCounter[endpoints[0].Node]
	minEndpoints := make([]Endpoint, 0)

	for _, endpoint := range endpoints {
		count := lb.nodeReqCounter[endpoint.Node]
		if count < minCount {
			minCount = count
			minEndpoints = make([]Endpoint, 0)
			minEndpoints = append(minEndpoints, endpoint)
		} else if count == minCount {
			minEndpoints = append(minEndpoints, endpoint)
		}
	}

	randomMin := minEndpoints[rand.Intn(len(minEndpoints))]

	return randomMin
}

func (lb *NodalLoadBalancer) log(logMessage string) {
	fmt.Fprint(lb.logWriter, logMessage)
	lb.logWriter.Flush()
}

func (lb *NodalLoadBalancer) handleState() {
	logFile, err := os.Create("lb_log.txt")
	check(err)
	defer logFile.Close()
	lb.logWriter = bufio.NewWriter(logFile)

	for {
		select {
		case endpoints := <-lb.requestForEndpointCh:

			leastQueuedEndPoint := lb.getLeastQueuedEndpoint(endpoints)

			if lb.AreStatsLogged {
				lb.log(fmt.Sprintf(
					"%v Req from app %d going to send to node %d\n",
					lb.nodeReqCounter,
					endpoints[0].App,
					leastQueuedEndPoint.Node))
			}

			lb.addReqToState(leastQueuedEndPoint.Node)
			lb.receiveEndpointCh <- leastQueuedEndPoint

		case node := <-lb.notifyReqCompletedCh:

			if lb.AreStatsLogged {
				lb.log(fmt.Sprintf(
					"%v Req completed on node %d\n",
					lb.nodeReqCounter,
					node))
			}

			lb.removeReqFromState(node)

		}
	}
}

func (lb *NodalLoadBalancer) GetEndpointForReq(endpoints []Endpoint) Endpoint {
	lb.requestForEndpointCh <- endpoints
	return <-lb.receiveEndpointCh
}

func (lb *NodalLoadBalancer) NotifyReqCompleted(node int) {
	lb.notifyReqCompletedCh <- node
}
