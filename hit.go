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
	"net/http/httptrace"
	"os"
	"regexp"
	"sort"
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
	ReqURL          string
	ReqEndpoint     Endpoint
	ReqNum          int
	IsError         bool
	ErrMsg          string
	StatusCode      int
	Body            string
	StartTimeNs     int64
	LatencyNs       int64
	WireLatencyNs   int64
	SetupOverheadNs int64
}

func makeRequest(reqURL string, resChan chan Response, reqNum int) {

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		errMsg := fmt.Sprintf("client: could not create request: %s", err)
		resChan <- Response{reqURL, Endpoint{},
			reqNum,
			true, errMsg,
			0, "",
			time.Now().UnixNano(), 0, 0, 0}
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
			startReq.UnixNano(), latency.Nanoseconds(), 0, 0}
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
			startReq.UnixNano(), latency.Nanoseconds(), readTime.Nanoseconds(), 0}
		return
	}

	resChan <- Response{reqURL, Endpoint{},
		reqNum,
		false, "",
		res.StatusCode, string(resBody),
		startReq.UnixNano(), latency.Nanoseconds(), readTime.Nanoseconds(), 0}
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
			time.Now().UnixNano(), 0, 0, 0}
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

	// --- NEW: Tracing Hooks ---
	var dnsStart, connStart, wroteRequest, firstByte time.Time
	var dnsDone, connDone time.Duration

	trace := &httptrace.ClientTrace{
		DNSStart:     func(_ httptrace.DNSStartInfo) { dnsStart = time.Now() },
		DNSDone:      func(_ httptrace.DNSDoneInfo) { dnsDone = time.Since(dnsStart) },
		ConnectStart: func(_, _ string) { connStart = time.Now() },
		ConnectDone:  func(_, _ string, _ error) { connDone = time.Since(connStart) },
		WroteRequest: func(_ httptrace.WroteRequestInfo) {
			wroteRequest = time.Now()
		},
		GotFirstResponseByte: func() {
			firstByte = time.Now()
		},
	}

	// Attach the trace to the request context
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	client := &http.Client{
		Timeout: 5 * time.Second, // Set a timeout for the entire request
	}

	startReq := time.Now()

	res, err := client.Do(req)
	totalLatency := time.Since(startReq)

	// --- ANALYSIS ---
	// This is the duration Istio actually sees (Request in flight)
	wireLatency := firstByte.Sub(wroteRequest)
	// This is the overhead of Connection: close
	setupOverhead := connDone + dnsDone

	// fmt.Printf("[%d] Setup: %v | On-Wire: %v | Total: %v\n",
	// 	reqNum, setupOverhead, wireLatency, totalLatency)

	if err != nil {
		errMsg := fmt.Sprintf("client: error making http request: %s", err)
		resChan <- Response{reqURL, endpoint,
			reqNum,
			true, errMsg,
			0, "",
			startReq.UnixNano(), totalLatency.Nanoseconds(), wireLatency.Nanoseconds(), setupOverhead.Nanoseconds()}
		return
	}

	// startRead := time.Now()
	resBody, err := io.ReadAll(res.Body)
	// readTime := time.Since(startRead)

	if err != nil {
		errMsg := fmt.Sprintf("client: could not read response body: %s", err)
		resChan <- Response{reqURL, endpoint,
			reqNum,
			true, errMsg,
			res.StatusCode, "",
			startReq.UnixNano(), totalLatency.Nanoseconds(), wireLatency.Nanoseconds(), setupOverhead.Nanoseconds()}
		return
	}

	resChan <- Response{reqURL, endpoint,
		reqNum,
		false, "",
		res.StatusCode, string(resBody),
		startReq.UnixNano(), totalLatency.Nanoseconds(), wireLatency.Nanoseconds(), setupOverhead.Nanoseconds()}
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

	if interval.distribution == "poisson" {
		panic("Poisson distribution not implemented yet")
		// interval.poisson = distuv.Poisson{
		// 	Lambda: float64(interval.repeatIntervalMs),
		// 	Src:    xrand.NewSource(uint64(time.Now().UnixNano())),
		// }
	}
}

// Pareto sample generator
func paretoSample(alpha, beta float64) float64 {
	// Generate a uniform random number in the range (0, 1)
	u := rand.Float64()

	// Generate a Pareto-distributed sample
	return alpha / math.Pow(u, 1.0/beta)
}

func paretoSampleWithMean(desiredMean float64) float64 {

	// Shape parameter
	beta := 1.5

	// Calculate alpha from the mean formula
	alpha := desiredMean * (beta - 1) / beta

	return paretoSample(alpha, beta)
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

	} else if interval.distribution == "pareto" {
		mean := interval.repeatIntervalMs
		paretoRandom := paretoSampleWithMean(mean)
		return time.Duration(paretoRandom) * time.Millisecond

	} else {
		return time.Duration(interval.repeatIntervalMs) * time.Millisecond
	}
}

type RequestIntervalUpdate struct {
	AtMs          int     `json:"atMs"`
	ReqIntervalMs float64 `json:"reqIntervalMs"`
}

func repeatRequests(
	cpuModifier *CPUWeightModifier,
	endpoints []Endpoint,
	lb *LoadBalancer,
	resChan chan Response,
	repeatIntervalMs float64,
	requestIntervalUpdates []RequestIntervalUpdate,
	endTimeMs int,
	lastReqCountChan chan int,
	isReadable bool,
	distributionName string,
	headers map[string]string) {

	// repeatRequests drives request generation for a single experiment.
	//
	// It has three independent time-based concerns:
	//  1) Request scheduling (based on Interval.Next(), possibly stochastic)
	//  2) Request-interval updates (deterministic wall-clock times since experiment start)
	//  3) End-of-measurement (after endTimeMs) – we publish the last request id to measure
	//
	// The function intentionally does NOT exit after the measurement window ends.
	// It continues sending requests so the system stays "warm" and avoids bias.

	fmt.Printf("Making requests for %dms at %.2fms interval\n", endTimeMs,
		repeatIntervalMs)

	// One-shot timer for the end of the measurement window.
	endTime := time.Duration(endTimeMs) * time.Millisecond
	endTimer := time.NewTimer(endTime)
	defer endTimer.Stop()

	// Interval controls how long to wait between requests.
	// It can be updated mid-run via requestIntervalUpdates.
	interval := Interval{}
	interval.Initialize(repeatIntervalMs, distributionName)

	// Updates may be provided out of order in JSON; apply them in time order.
	sort.Slice(requestIntervalUpdates, func(i, j int) bool {
		return requestIntervalUpdates[i].AtMs < requestIntervalUpdates[j].AtMs
	})
	updateIdx := 0                // index of the next update to apply
	experimentStart := time.Now() // time 0 for AtMs offsets

	// Applies any updates whose AtMs has already passed.
	// Multiple updates can be applied in one call if the system is behind.
	applyDueUpdates := func() {
		elapsedMs := int(time.Since(experimentStart).Milliseconds())
		for updateIdx < len(requestIntervalUpdates) && requestIntervalUpdates[updateIdx].AtMs <= elapsedMs {
			interval.Initialize(requestIntervalUpdates[updateIdx].ReqIntervalMs, distributionName)
			updateIdx++
		}
	}

	// Apply updates scheduled at or before time 0.
	applyDueUpdates()

	// resetTimer safely resets a timer even if it has already fired.
	// (If it fired, we drain the channel to avoid spurious wakeups later.)
	resetTimer := func(t *time.Timer, d time.Duration) {
		if d < 0 {
			d = 0
		}
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(d)
	}

	// lastTime is the time we last sent a request.
	// We subtract time.Since(lastTime) so that work done in the loop doesn't cause drift.
	lastTime := time.Now()

	// reqTimer triggers the next request send.
	// We reuse a single timer to avoid creating new timers every select iteration.
	reqTimer := time.NewTimer(0)
	defer reqTimer.Stop()
	resetTimer(reqTimer, interval.Next())

	// updateTimer triggers the next interval update time (if any).
	updateTimer := time.NewTimer(time.Hour)
	defer updateTimer.Stop()
	updateEnabled := false // whether updateTimer is currently armed for a real update
	if updateIdx < len(requestIntervalUpdates) {
		updateEnabled = true
		at := experimentStart.Add(time.Duration(requestIntervalUpdates[updateIdx].AtMs) * time.Millisecond)
		resetTimer(updateTimer, time.Until(at))
	}

	reqCounter := 1
	measurementEnded := false // defensive guard to avoid double-sending on lastReqCountChan
	for {
		select {
		case <-reqTimer.C:
			// Time to send the next request.
			// First, apply any interval updates that are already due.
			applyDueUpdates()
			lastTime = time.Now()
			endpoint := lb.GetEndpointForReq(reqCounter)
			cpuModifier.NotifyReqSent(endpoint)
			go makeReqToEndpoint(endpoint, resChan, reqCounter, headers)
			if isReadable {
				fmt.Println("Making request ", reqCounter, " to ",
					endpoint.URL)
			}
			reqCounter += 1
			// Schedule the next request.
			resetTimer(reqTimer, interval.Next()-time.Since(lastTime))
		case <-endTimer.C:
			// Measurement window ended. Tell the receiver the last request id to include.
			// Keep sending requests after this point to avoid bias.
			if !measurementEnded {
				measurementEnded = true
				lastReqCountChan <- reqCounter
			}
		case <-updateTimer.C:
			if updateEnabled {
				// An interval update time fired. Apply any updates due and re-arm timer.
				applyDueUpdates()
				if updateIdx < len(requestIntervalUpdates) {
					at := experimentStart.Add(time.Duration(requestIntervalUpdates[updateIdx].AtMs) * time.Millisecond)
					resetTimer(updateTimer, time.Until(at))
				} else {
					updateEnabled = false
				}
				// Make the new interval take effect immediately for the next send.
				resetTimer(reqTimer, interval.Next()-time.Since(lastTime))
			}
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
			float64(resp.LatencyNs)/1000000, float64(resp.WireLatencyNs)/1000000)
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
	Endpoints              []Endpoint              `json:"endpoints"`
	ReqIntervalMs          float64                 `json:"reqIntervalMs"`
	RequestIntervalUpdates []RequestIntervalUpdate `json:"requestIntervalUpdates"`
	DurationMs             int                     `json:"durationMs"`
	LogFileName            string                  `json:"logFileName"`
	StallTimeMs            int                     `json:"stallTimeMs"`
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

	var configFileName string
	flag.StringVar(&configFileName, "f", "", "JSON config file name")

	stallTimeMs := flag.Int("s", 0,
		"Time (in ms) to stall before starting to send requests")

	distributionName := flag.String(
		"distr",
		"none",
		"Distribution name [none|exponential|pareto] (default: none)")

	flag.Parse()

	if configFileName != "" {

		// read config file
		file, err := os.ReadFile(configFileName)
		if err != nil {
			panic(fmt.Sprintf("Error reading the json file: %s\n", err))
		}

		var raw_configs []Config
		if err := json.Unmarshal(file, &raw_configs); err != nil {
			panic(fmt.Sprintf("Error parsing the json file: %s\n", err))
		}

		fmt.Printf("Raw Configs: %v\n", raw_configs)

		configs := make([]Config, len(raw_configs))
		for i, raw_config := range raw_configs {
			configs[i] = Config{
				Endpoints:              raw_config.Endpoints,
				ReqIntervalMs:          raw_config.ReqIntervalMs,
				RequestIntervalUpdates: raw_config.RequestIntervalUpdates,
				DurationMs:             raw_config.DurationMs,
				LogFileName:            raw_config.LogFileName,
				StallTimeMs:            raw_config.StallTimeMs,
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
			Endpoints:              endpoints,
			ReqIntervalMs:          reqIntervalMs,
			RequestIntervalUpdates: nil,
			DurationMs:             *durationInSec * 1000,
			LogFileName:            *logFileName,
			StallTimeMs:            *stallTimeMs,
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
		loadBalancerAlgo = "ROUND_ROBIN"
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
		config.RequestIntervalUpdates,
		durationMs,
		lastReqCountChan,
		isReadable,
		distributionName,
		make(map[string]string))

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
