package process

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

var stopped = false

func RunRest(stop chan bool, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		r := gin.Default()
		r.GET("/healthProcess", func(c *gin.Context) {
			if stopped {
				c.JSON(http.StatusInternalServerError, gin.H{
					"status": "fail",
				})
			} else {
				c.JSON(http.StatusOK, gin.H{
					"status": "ok",
				})
			}
		})
		consumerHealthCheckPort, ok := os.LookupEnv("CONSUMER_HEALTH_CHECK_PORT")
		if !ok {
			consumerHealthCheckPort = "8081"
		}
		_ = r.Run(fmt.Sprintf(":%s", consumerHealthCheckPort))
	}()

	consumerHealthCheckTimeout, ok := os.LookupEnv("CONSUMER_HEALTH_CHECK_TIMEOUT_MS")
	if !ok {
		consumerHealthCheckTimeout = "2000"
	}
	consumerHealthCheckTimeoutMs, _ := strconv.Atoi(consumerHealthCheckTimeout)

	for {
		select {
		case <-stop:
			stopped = true
			fmt.Println("stopped")
			return
		default:
			time.Sleep(time.Millisecond * time.Duration(consumerHealthCheckTimeoutMs))
		}
	}
}
func RecoverRunProc(stop chan bool) {
	if err := recover(); err != nil {
		stop <- true
		fmt.Println(fmt.Sprintf("process finished with error: %+v", err))
	}
}
