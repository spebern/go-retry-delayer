# Defer railed operations (http requests, grpc requests)

This package provides simple interface to defer failed operations.

When servers talk to each other it can occur than one is unavailbe for some time.
During the time requests that fail should be queued and retrtied into the future.

Also, sometimes a such agolang kafka tutorial queued request should be replaced by a better one.

This Package aims to provide an interface for that.

``` go
package main

import (
	"net/http"
	"time"

	"github.com/spebern/go-retry-delayer"
)

func main() {
	msgToRetry := make(map[interface{}]func(interface{}) bool)
	msgToRetry[""] = func(urlV interface{}) bool {
		url := urlV.(string)
		_, err := http.Get(url)
		if err != nil {
			// no success, will be delayed and retried again if
			// max retries has not been exhausted for this message
			return false
		}

		// successfull, won't be retried
		return true
	}

	// duration until a failed operation will be tried again
	retryAfter := 5 * time.Second

	// all messages that failed in a time frame of jitter will be retried together
	jitter := 100 * time.Millisecond

	// number of concurrent retries
	concurrentRetries := uint(8)

	// a delayed operation will be retried (with retryAfter delay) maxRetries times
	maxRetries := uint(3)

	delayer := retry_delayer.New(retryAfter, jitter, concurrentRetries, maxRetries, msgToRetry)

	url := "http://www.bing.de"

	_, err := http.Get(url)
	if err != nil {
		// the get request failed, will be retried in the future
		// the first argument will be a unique identifier for the msg (so that it can be removed or replaced)
		delayer.Delay("search engine", url)
	}

	delayer.Replace("search engine", "https://www.baidu.de")

	delayer.Remove("search engine")

	// ...
}
```
