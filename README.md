# hit
The pre-built binary is built for Ubuntu.

Example use: If I want to send requests to `google.com` for 10s at 1 requests per second and store the results in a log file named `"google.log"`, I would run:
```./hit -d 10 -rps 2 -l "google.log" -headers "{'connection': 'close'}" -url "http://www.google.com"```

For more options and details on the benchmark, run `./hit -help`.