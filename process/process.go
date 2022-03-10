package process

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"sync"
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
		r.Run(consumerHealthCheckPort)
	}()
	for {
		select {
		case <-stop:
			stopped = true
			fmt.Println("stopped")
			return
		default:
			// …
		}
	}
}
func RecoverRunProc(stop chan bool) {
	if err := recover(); err != nil {
		stop <- true
		fmt.Println(fmt.Sprintf("process finished with error: %v", err))
	}
}
