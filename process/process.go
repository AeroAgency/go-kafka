package process

import (
	"fmt"
	"github.com/AeroAgency/go-kafka/env"
	"github.com/gin-gonic/gin"
	"net/http"
	"sync"
	"time"
)

const (
	defaultConsumerHealthCheckPort      = "8081"
	defaultConsumerHealthCheckTimeoutMs = 2000
)

func RunRest(stop chan bool, wg *sync.WaitGroup) {
	var stopped = false
	go startServer(wg, &stopped)

	consumerHealthCheckTimeoutMs := env.GetIntOrDefault("CONSUMER_HEALTH_CHECK_TIMEOUT_MS", defaultConsumerHealthCheckTimeoutMs)

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

func startServer(wg *sync.WaitGroup, stopped *bool) {
	defer wg.Done()
	r := setupRouter(stopped)
	consumerHealthCheckPort := env.GetStringOrDefault("CONSUMER_HEALTH_CHECK_PORT", defaultConsumerHealthCheckPort)

	_ = r.Run(fmt.Sprintf(":%s", consumerHealthCheckPort))
}

func setupRouter(stopped *bool) *gin.Engine {
	r := gin.Default()
	r.GET("/healthProcess", func(c *gin.Context) {
		if *stopped {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status": "fail",
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"status": "ok",
			})
		}
	})

	return r
}

func RecoverRunProc(stop chan bool) {
	if err := recover(); err != nil {
		stop <- true
		fmt.Println(fmt.Sprintf("process finished with error: %+v", err))
	}
}
