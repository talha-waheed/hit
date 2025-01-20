package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type GurobiResponse struct {
	Status    int     `json:"status"`
	App1Node1 float64 `json:"t00"`
	App1Node2 float64 `json:"t01"`
	App2Node2 float64 `json:"t11"`
	App2Node3 float64 `json:"t12"`
	App3Node1 float64 `json:"t20"`
}

type Response struct {
	ReqURL      string
	ReqEndpoint Endpoint
	ReqNum      int
	IsError     bool
	ErrMsg      string
	StatusCode  int
	Body        string
	StartTimeNs int64
	LatencyNs   int64
	ReadTimeNs  int64
}

func makeRequest(reqURL string, resChan chan Response, reqNum int) {

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		errMsg := fmt.Sprintf("client: could not create request: %s", err)
		resChan <- Response{reqURL, Endpoint{},
			reqNum,
			true, errMsg,
			0, "",
			time.Now().UnixNano(), 0, 0}
		return
	}
	req.Header.Set("Connection", "close")

	startReq := time.Now()
	// client := http.Client{
	// 	Transport: &http2.Transport{
	// 		// So http2.Transport doesn't complain the URL scheme isn't 'https'
	// 		AllowHTTP: true,
	// 		// Pretend we are dialing a TLS endpoint. (Note, we ignore the passed tls.Config)
	// 		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
	// 			return net.Dial(network, addr)
	// 		},
	// 	},
	// }
	// res, err := client.Do(req)
	res, err := http.DefaultClient.Do(req)
	latency := time.Since(startReq)

	if err != nil {
		errMsg := fmt.Sprintf("client: error making http request: %s", err)
		resChan <- Response{reqURL, Endpoint{},
			reqNum,
			true, errMsg,
			0, "",
			startReq.UnixNano(), latency.Nanoseconds(), 0}
		return
	}

	startRead := time.Now()
	resBody, err := io.ReadAll(res.Body)
	readTime := time.Since(startRead)

	if err != nil {
		errMsg := fmt.Sprintf("client: could not read response body: %s", err)
		resChan <- Response{reqURL, Endpoint{},
			reqNum,
			true, errMsg,
			res.StatusCode, "",
			startReq.UnixNano(), latency.Nanoseconds(), readTime.Nanoseconds()}
		return
	}

	resChan <- Response{reqURL, Endpoint{},
		reqNum,
		false, "",
		res.StatusCode, string(resBody),
		startReq.UnixNano(), latency.Nanoseconds(), readTime.Nanoseconds()}
}

// Function to parse URL
// replace "EXP<\d+>" in the URL with a random integer distributed exponentially
//
//	with the mean as the \d+ in teh pattern
func parseURL(s string) string {
	// Define the regular expression to match "EXP<\d+>"
	re := regexp.MustCompile(`EXP<(\d+)>`)

	// Check if the pattern matches
	if match := re.FindStringSubmatch(s); len(match) > 1 {
		// If a match is found, return the original string
		// Seed the random number generator
		// rand.Seed(time.Now().UnixNano())

		// Replace pattern with a random integer from an exponential distribution
		return re.ReplaceAllStringFunc(s, func(match string) string {

			if submatch := re.FindStringSubmatch(match); len(submatch) > 1 {

				floatValue, err := strconv.ParseFloat(submatch[1], 64)
				if err != nil {
					fmt.Println("Error:", err)
					return match
				}
				expMean := floatValue

				exponentialRandom := -expMean * math.Log(1-rand.Float64())

				// Generate a random integer exponential value
				return fmt.Sprintf("%d", int(exponentialRandom))
			}

			return match
		})
	}

	return s
}

func makeReqToEndpoint(
	endpoint Endpoint,
	resChan chan Response,
	reqNum int,
	headers map[string]string) {

	reqURL := parseURL(endpoint.URL)

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		errMsg := fmt.Sprintf("client: could not create request: %s", err)
		resChan <- Response{reqURL, endpoint,
			reqNum,
			true, errMsg,
			0, "",
			time.Now().UnixNano(), 0, 0}
		return
	}

	// for key, value := range headers {
	// 	req.Header.Set(key, value)
	// }

	// endpoint.Headers is a string of json with key-value pairs
	// 	eg. {"key1": "value1", "key2": "value2"}
	// 	we need to convert this to a map[string]string
	// 	so that we can set the headers in the request
	endpointHeaders := make(map[string]string)
	json.Unmarshal([]byte(endpoint.Headers), &endpointHeaders)
	for headersKey, headersValue := range endpointHeaders {
		if headersKey == "Host" {
			req.Host = headersValue
		} else {
			req.Header.Set(headersKey, headersValue)
		}
	}
	req.Header.Set("Connection", "close")

	startReq := time.Now()
	// client := http.Client{
	// 	Transport: &http2.Transport{
	// 		// So http2.Transport doesn't complain the URL scheme isn't 'https'
	// 		AllowHTTP: true,
	// 		// Pretend we are dialing a TLS endpoint. (Note, we ignore the passed tls.Config)
	// 		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
	// 			return net.Dial(network, addr)
	// 		},
	// 	},
	// }
	// res, err := client.Do(req)
	client := &http.Client{
		Timeout: 10 * time.Second, // Set a timeout for the entire request
	}
	res, err := client.Do(req)
	latency := time.Since(startReq)

	if err != nil {
		errMsg := fmt.Sprintf("client: error making http request: %s", err)
		resChan <- Response{reqURL, endpoint,
			reqNum,
			true, errMsg,
			0, "",
			startReq.UnixNano(), latency.Nanoseconds(), 0}
		return
	}

	startRead := time.Now()
	resBody, err := io.ReadAll(res.Body)
	readTime := time.Since(startRead)

	if err != nil {
		errMsg := fmt.Sprintf("client: could not read response body: %s", err)
		resChan <- Response{reqURL, endpoint,
			reqNum,
			true, errMsg,
			res.StatusCode, "",
			startReq.UnixNano(), latency.Nanoseconds(), readTime.Nanoseconds()}
		return
	}

	resChan <- Response{reqURL, endpoint,
		reqNum,
		false, "",
		res.StatusCode, string(resBody),
		startReq.UnixNano(), latency.Nanoseconds(), readTime.Nanoseconds()}
}

type Interval struct {
	repeatIntervalMs float64
	distribution     string
	// poisson          distuv.Poisson
}

func (interval *Interval) Initialize(
	repeatIntervalMs float64, distributionName string) {

	interval.repeatIntervalMs = repeatIntervalMs
	interval.distribution = distributionName

	rand.Seed(time.Now().UnixNano())

	if interval.distribution == "poisson" {
		panic("Poisson distribution not implemented yet")
		// interval.poisson = distuv.Poisson{
		// 	Lambda: float64(interval.repeatIntervalMs),
		// 	Src:    xrand.NewSource(uint64(time.Now().UnixNano())),
		// }
	}
}

func (interval *Interval) Next() time.Duration {
	if interval.distribution == "poisson" {
		panic("Poisson distribution not implemented yet")
		// return time.Duration(
		// 	interval.poisson.Rand() * float64(time.Millisecond))
	} else if interval.distribution == "exponential" {
		mean := interval.repeatIntervalMs
		exponentialRandom := -mean * math.Log(1-rand.Float64())
		// fmt.Printf("Exponential Random: %f\n", exponentialRandom)
		return time.Duration(exponentialRandom) * time.Millisecond
	} else {
		return time.Duration(interval.repeatIntervalMs) * time.Millisecond
	}
}

func repeatRequests(
	cpuModifier *CPUWeightModifier,
	endpoints []Endpoint,
	lb *LoadBalancer,
	resChan chan Response,
	repeatIntervalMs float64,
	endTimeMs int,
	lastReqCountChan chan int,
	isReadable bool,
	distributionName string,
	headers map[string]string) {

	fmt.Printf("Making requests for %dms at %.2fms interval\n", endTimeMs,
		repeatIntervalMs)

	endTime := time.Duration(endTimeMs) * time.Millisecond
	endTicker := time.NewTicker(endTime)

	interval := Interval{}
	interval.Initialize(repeatIntervalMs, distributionName)

	lastTime := time.Now()

	reqCounter := 1
	for {
		select {
		case <-time.After(interval.Next() - time.Since(lastTime)):
			lastTime = time.Now()
			endpoint := lb.GetEndpointForReq(reqCounter)
			cpuModifier.NotifyReqSent(endpoint)
			go makeReqToEndpoint(endpoint, resChan, reqCounter, headers)
			if isReadable {
				fmt.Println("Making request ", reqCounter, " to ",
					endpoint.URL)
			}
			reqCounter += 1
		case <-endTicker.C:
			endTicker.Stop()

			lastReqCountChan <- reqCounter

			// Don't return, we need to continously send requests to avoid bias
			// return
		}
	}
}

func areRequestsFinished(resReceivedCount int, resEndCount int) bool {
	// false if we are not communicated the last rescount yet (this case is
	// 	redundant, but is there for code readability)
	if resEndCount == -1 {
		return false
	}
	// false if we are communicated the last rescount, but we havent yet
	// 	received all responses
	if resReceivedCount < resEndCount {
		return false
	}
	// true if we are communicated the last rescount, and we have received all
	//  responses
	return true
}

func logResponseStats(
	logWriter *bufio.Writer,
	resp Response,
	resReceivedCount int,
	isReadable bool) {

	defer logWriter.Flush()

	if isReadable {
		fmt.Printf("{%s} res %d [%d]: %s, latency: %fms %fms\n",
			resp.ReqURL, resp.ReqNum, resp.StatusCode, resp.Body,
			float64(resp.LatencyNs)/1000000, float64(resp.ReadTimeNs)/1000000)
	} else {
		respJSON, err := json.Marshal(resp)
		if err != nil {
			fmt.Fprintf(logWriter,
				"Err converting stats to json for url %s: req %d (res %d): %s",
				resp.ReqURL, resp.ReqNum, resReceivedCount, err)
			return
		}
		fmt.Fprintln(logWriter, string(respJSON))
	}
}

func repeatNodalRequests(
	endpoints []Endpoint,
	lb *NodalLoadBalancer,
	resChan chan Response,
	repeatIntervalMs float64,
	endTimeMs int,
	lastReqCountChan chan int,
	isReadable bool,
	distributionName string) {

	fmt.Printf("Making requests for %dms at %.2fms interval\n", endTimeMs,
		repeatIntervalMs)

	endTime := time.Duration(endTimeMs) * time.Millisecond
	endTicker := time.NewTicker(endTime)

	interval := Interval{}
	interval.Initialize(repeatIntervalMs, distributionName)

	lastTime := time.Now()

	reqCounter := 1
	for {
		select {

		case <-time.After(interval.Next() - time.Since(lastTime)):
			lastTime = time.Now()
			endpointToReq := lb.GetEndpointForReq(endpoints)
			go makeReqToEndpoint(endpointToReq, resChan, reqCounter,
				make(map[string]string))
			if isReadable {
				fmt.Printf("Making request %d to app%d-node%d\n", reqCounter,
					endpointToReq.App, endpointToReq.Node)
			}
			reqCounter += 1

		case <-endTicker.C:
			endTicker.Stop()
			lastReqCountChan <- reqCounter
			return
		}
	}
}

func repeatGlobalRequests(
	endpoints []Endpoint,
	lb *GlobalLoadBalancer,
	resChan chan Response,
	repeatIntervalMs float64,
	endTimeMs int,
	lastReqCountChan chan int,
	isReadable bool,
	distributionName string) {

	fmt.Printf("Making requests for %dms at %.2fms interval\n", endTimeMs,
		repeatIntervalMs)

	endTime := time.Duration(endTimeMs) * time.Millisecond
	endTicker := time.NewTicker(endTime)

	interval := Interval{}
	interval.Initialize(repeatIntervalMs, distributionName)

	lastTime := time.Now()

	reqCounter := 1
	for {
		select {

		case <-time.After(interval.Next() - time.Since(lastTime)):
			lastTime = time.Now()
			endpointToReq := lb.GetEndpointForReq(endpoints)
			go makeReqToEndpoint(endpointToReq, resChan, reqCounter,
				map[string]string{})
			if isReadable {
				fmt.Printf("Making request %d to app%d-node%d\n", reqCounter,
					endpointToReq.App, endpointToReq.Node)
			}
			reqCounter += 1

		case <-endTicker.C:
			endTicker.Stop()
			lastReqCountChan <- reqCounter
			return
		}
	}
}

type arrayFlags []string

func (arr *arrayFlags) String() string {
	return fmt.Sprintf("%v", *arr)
}

func (arr *arrayFlags) StringArr() []string {
	return *arr
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

type Endpoint struct {
	URL     string `json:"url"`
	Node    int    `json:"node"`
	App     int    `json:"app"`
	Headers string `json:"headers"`
}

type Config struct {
	Endpoints     []Endpoint        `json:"endpoints"`
	ReqIntervalMs float64           `json:"reqIntervalMs"`
	DurationMs    int               `json:"durationMs"`
	LogFileName   string            `json:"logFileName"`
	StallTimeMs   int               `json:"stallTimeMs"`
	Headers       map[string]string `json:"headers"`
}

func jsonToMap(jsonStr string) map[string]string {
	result := make(map[string]string)
	json.Unmarshal([]byte(jsonStr), &result)
	return result
}

func getConfigs() ([]Config, bool, string, bool, bool, bool) {

	var urls arrayFlags
	flag.Var(&urls, "url", "an endpoint's URL to send requests to")

	reqRatePerSec := flag.Float64("rps", 1.0, "Requests to make per sec")
	durationInSec := flag.Int("d", 4, "Duration in sec")
	logFileName := flag.String("l", "", "Log file name")
	headers := flag.String("headers", "", "Headers to send with the request")

	isReadable := flag.Bool("r", false, "Print readable statistics")

	useNodalLB := flag.Bool("n", false,
		"Use node-level least request load balancer")

	useGlobalLB := flag.Bool("g", false,
		"Use global-level least request load balancer")

	useCPUSharing := flag.Bool("c", false,
		"Use varying CPU weights to balance a least request load balancer")

	configFileName := flag.String("f", "", "JSON config file name")

	stallTimeMs := flag.Int("s", 0,
		"Time (in ms) to stall before starting to send requests")

	distributionName := flag.String(
		"distr",
		"none",
		"Distribution name [none|exponential] (default: none)")

	flag.Parse()

	if *configFileName != "" {

		// read config file
		file, err := os.ReadFile(*configFileName)
		if err != nil {
			panic(fmt.Sprintf("Error reading the json file: %s\n", err))
		}

		var raw_configs []Config
		json.Unmarshal(file, &raw_configs)

		fmt.Printf("Raw Configs: %v\n", raw_configs)

		configs := make([]Config, len(raw_configs))
		for i, raw_config := range raw_configs {
			configs[i] = Config{
				Endpoints:     raw_config.Endpoints,
				ReqIntervalMs: raw_config.ReqIntervalMs,
				DurationMs:    raw_config.DurationMs,
				LogFileName:   raw_config.LogFileName,
				StallTimeMs:   raw_config.StallTimeMs,
			}
		}

		fmt.Printf("Configs: %v\n", configs)

		return configs, *isReadable, *distributionName,
			*useNodalLB, *useGlobalLB, *useCPUSharing

	} else {

		if len(urls.StringArr()) == 0 {
			panic("No URLs provided")
		}

		endpoints := make([]Endpoint, len(urls.StringArr()))
		for i, url := range urls.StringArr() {
			endpoints[i] = Endpoint{
				URL:     url,
				Node:    0,
				App:     0,
				Headers: *headers,
			}
		}

		reqIntervalMs := 1000.0 / *reqRatePerSec
		if *reqRatePerSec == 0 {
			// set reqIntervalMs as infinity
			reqIntervalMs = float64((*durationInSec + 1) * 1000)
		}

		configs := []Config{{
			Endpoints:     endpoints,
			ReqIntervalMs: reqIntervalMs,
			DurationMs:    *durationInSec * 1000,
			LogFileName:   *logFileName,
			StallTimeMs:   *stallTimeMs,
			Headers:       jsonToMap(*headers),
		}}

		return configs, *isReadable, *distributionName,
			*useNodalLB, *useGlobalLB, *useCPUSharing
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func hit(
	config Config,
	isReadable bool,
	distributionName string,
	cpuModifier *CPUWeightModifier,
	hasHitEndedCh chan bool,
	okayToAbortCh chan bool,
	wg *sync.WaitGroup) {

	defer wg.Done()

	// set benchmark configs
	reqEndpoints := config.Endpoints
	reqURLs := make([]string, len(reqEndpoints))
	for i, endpoint := range reqEndpoints {
		reqURLs[i] = endpoint.URL
	}
	reqIntervalMs := config.ReqIntervalMs
	durationMs := config.DurationMs

	// set benchmark log file
	logFile, err := os.Create(config.LogFileName)
	check(err)
	defer logFile.Close()
	logWriter := bufio.NewWriter(logFile)

	// printing benchmark config
	fmt.Fprintf(logWriter,
		"Running test for %s [%.2fms req interval] [%dms duration]:\n",
		reqURLs, reqIntervalMs, durationMs)
	logWriter.Flush()

	// set load balancer algorithm
	loadBalancerAlgo := "NONE"
	if len(reqURLs) > 1 {
		loadBalancerAlgo = "LEAST_REQUEST"
	}
	fmt.Printf("Load balancer algorithm: %s\n", loadBalancerAlgo)

	// start the load balancer
	lb := LoadBalancer{
		loadBalancerAlgo: loadBalancerAlgo,
		endpoints:        config.Endpoints,
	}
	lb.StartLoadBalancer()

	// stall before starting to send requests
	time.Sleep(time.Duration(config.StallTimeMs) * time.Millisecond)

	// making channels for getting responses back
	resChan := make(chan Response)     // for getting responses
	lastReqCountChan := make(chan int) // for getting the last response num

	// repeatedly make requests,
	// send back responses in resChan,
	// and notify the number of responses sent in the lastReqCountChan
	go repeatRequests(
		cpuModifier,
		reqEndpoints,
		&lb,
		resChan,
		reqIntervalMs,
		durationMs,
		lastReqCountChan,
		isReadable,
		distributionName,
		config.Headers)

	// now, repeatedly listen for responses and the last response number,
	// 	end when the last response is received

	lastResCount := math.MaxInt // dummy value till we are informed of this
	resReceivedCount := 1
	for {

		select {

		case resp := <-resChan:
			lb.NotifyReqCompleted(resp.ReqNum)
			cpuModifier.NotifyReqCompleted(resp.ReqEndpoint.Node)

			// request should only be logged if it is
			// 	<= last request to be measured
			if resp.ReqNum <= lastResCount {
				logResponseStats(logWriter, resp, resReceivedCount, isReadable)
				resReceivedCount += 1
			}
		case lastReqCount := <-lastReqCountChan:
			lastResCount = lastReqCount
		}

		// if we have received all responses, then break
		if resReceivedCount >= lastResCount {
			break
		}
	}

	// signal that this hit has finished measurements
	hasHitEndedCh <- true
	// wait for all hits to have finished measurements
	<-okayToAbortCh

	fmt.Println("Aborted hit for ", config.Endpoints)
}

func hitNodal(
	lb *NodalLoadBalancer,
	config Config,
	isReadable bool,
	distributionName string,
	wg *sync.WaitGroup) {

	defer wg.Done()

	// set benchmark configs
	reqEndpoints := config.Endpoints
	reqURLs := make([]string, len(reqEndpoints))
	for i, endpoint := range reqEndpoints {
		reqURLs[i] = endpoint.URL
	}
	reqIntervalMs := config.ReqIntervalMs
	durationMs := config.DurationMs

	// set benchmark log file
	logFile, err := os.Create(config.LogFileName)
	check(err)
	defer logFile.Close()
	logWriter := bufio.NewWriter(logFile)

	// printing benchmark config
	fmt.Fprintf(logWriter,
		"Running test for %s [%f.2ms req interval] [%dms duration]:\n",
		reqURLs, reqIntervalMs, durationMs)
	logWriter.Flush()
	fmt.Printf("Load balancer algorithm: %s\n", "Nodal LR")

	// stall before starting to send requests
	time.Sleep(time.Duration(config.StallTimeMs) * time.Millisecond)

	// making channels for getting responses back
	resChan := make(chan Response)     // for getting responses
	lastReqCountChan := make(chan int) // for getting the last response num

	// repeatedly make requests,
	// send back responses in resChan,
	// and notify the number of responses sent in the lastReqCountChan
	go repeatNodalRequests(
		reqEndpoints,
		lb,
		resChan,
		reqIntervalMs,
		durationMs,
		lastReqCountChan,
		isReadable,
		distributionName)

	// now, repeatedly listen for responses and the last response number,
	// 	end when the last response is received

	lastResCount := -1 // dummy value till we are informed of this
	resReceivedCount := 1
	for {
		select {
		case resp := <-resChan:
			lb.NotifyReqCompleted(resp.ReqEndpoint.Node)
			logResponseStats(logWriter, resp, resReceivedCount, isReadable)
			resReceivedCount += 1
		case lastReqCount := <-lastReqCountChan:
			lastResCount = lastReqCount
		}

		if areRequestsFinished(resReceivedCount, lastResCount) {
			break
		}
	}
}

func hitGlobal(
	lb *GlobalLoadBalancer,
	config Config,
	isReadable bool,
	distributionName string,
	wg *sync.WaitGroup) {

	defer wg.Done()

	// set benchmark configs
	reqEndpoints := config.Endpoints
	reqURLs := make([]string, len(reqEndpoints))
	for i, endpoint := range reqEndpoints {
		reqURLs[i] = endpoint.URL
	}
	reqIntervalMs := config.ReqIntervalMs
	durationMs := config.DurationMs

	// set benchmark log file
	logFile, err := os.Create(config.LogFileName)
	check(err)
	defer logFile.Close()
	logWriter := bufio.NewWriter(logFile)

	// printing benchmark config
	fmt.Fprintf(logWriter,
		"Running test for %s [%.2fms req interval] [%dms duration]:\n",
		reqURLs, reqIntervalMs, durationMs)
	logWriter.Flush()
	fmt.Printf("Load balancer algorithm: %s\n", "Nodal LR")

	// stall before starting to send requests
	time.Sleep(time.Duration(config.StallTimeMs) * time.Millisecond)

	// making channels for getting responses back
	resChan := make(chan Response)     // for getting responses
	lastReqCountChan := make(chan int) // for getting the last response num

	// repeatedly make requests,
	// send back responses in resChan,
	// and notify the number of responses sent in the lastReqCountChan
	go repeatGlobalRequests(
		reqEndpoints,
		lb,
		resChan,
		reqIntervalMs,
		durationMs,
		lastReqCountChan,
		isReadable,
		distributionName)

	// now, repeatedly listen for responses and the last response number,
	// 	end when the last response is received

	lastResCount := -1 // dummy value till we are informed of this
	resReceivedCount := 1
	for {
		select {
		case resp := <-resChan:
			lb.NotifyReqCompleted(resp.ReqEndpoint.Node)
			logResponseStats(logWriter, resp, resReceivedCount, isReadable)
			resReceivedCount += 1
		case lastReqCount := <-lastReqCountChan:
			lastResCount = lastReqCount
		}

		if areRequestsFinished(resReceivedCount, lastResCount) {
			break
		}
	}
}

func coordinateAborting(numConfigs int,
	hasHitEndedCh chan bool, okayToAbortCh chan bool) {

	// wait for all the hit instances to finish
	for range numConfigs {
		<-hasHitEndedCh
	}

	// send signal to all the hit instances to finish
	for range numConfigs {
		okayToAbortCh <- true
	}
}

func main() {

	// set benchmark configs
	configs, isReadable, distributionName,
		useNodalLB, useGlobalLB, useCPUSharing := getConfigs()

	wg := new(sync.WaitGroup)

	if useGlobalLB {
		fmt.Println("Using a global load balancer")

		lb := GlobalLoadBalancer{
			Nodes:                  []int{1, 2, 3},
			Apps:                   []int{1, 2, 3},
			HostCapPerSec:          47,
			AreStatsLogged:         true,
			WeightUpdateIntervalMs: 1000,
		}
		lb.StartLoadBalancer()

		for _, config := range configs {
			wg.Add(1)
			go hitGlobal(&lb, config, isReadable, distributionName, wg)
		}

	} else if useNodalLB {
		fmt.Println("Using same nodal load balancer")

		lb := NodalLoadBalancer{
			Nodes: []int{1, 2, 3},
		}
		lb.StartLoadBalancer()

		for _, config := range configs {
			wg.Add(1)
			go hitNodal(&lb, config, isReadable, distributionName, wg)
		}
	} else {
		fmt.Println("Using Least Request load balancer")

		cpuModifier := CPUWeightModifier{
			Nodes:                  []int{1, 2, 3},
			Apps:                   []int{1, 2, 3},
			HostCapPerSec:          47,
			AreStatsLogged:         true,
			WeightUpdateIntervalMs: 1000,
		}
		if useCPUSharing {
			fmt.Println("Using CPU sharing")
			cpuModifier.Start()
		}

		okayToAbortCh, hasHitEndedCh := make(chan bool), make(chan bool)
		go coordinateAborting(len(configs), hasHitEndedCh, okayToAbortCh)

		for _, config := range configs {
			wg.Add(1)
			go hit(config, isReadable, distributionName, &cpuModifier,
				hasHitEndedCh, okayToAbortCh, wg)
		}
	}

	wg.Wait()
}

/*
To-Dos:

- add a flag to get config from a file -- done
- parse the config file -- done
- run multiple go instances of hit -- done
- log to a log file instead of printing to stdout -- done

- run an experiment to validate that everything is working correctly -- done

- find a way to have the different hit instances coordinate with each other to
	to implement a node-level least request load balancer
*/
