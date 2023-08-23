package process

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestRouter(t *testing.T) {
	gin.SetMode(gin.TestMode)

	type response struct {
		Status string `json:"status"`
	}

	okResponse := response{Status: "ok"}
	failResponse := response{Status: "fail"}

	testCases := []struct {
		name             string
		stopped          bool
		expectedCode     int
		expectedResponse response
	}{
		{
			name:             "Not stopped",
			stopped:          false,
			expectedCode:     http.StatusOK,
			expectedResponse: okResponse,
		},
		{
			name:             "Stopped",
			stopped:          true,
			expectedCode:     http.StatusInternalServerError,
			expectedResponse: failResponse,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			router := setupRouter(&tc.stopped)
			w := httptest.NewRecorder()
			req, _ := http.NewRequest("GET", "/healthProcess", nil)
			router.ServeHTTP(w, req)

			assert.Equal(t, tc.expectedCode, w.Code)

			var actualResp response
			err := json.Unmarshal(w.Body.Bytes(), &actualResp)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectedResponse, actualResp)
		})
	}
}

func TestRunRest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	type response struct {
		Status string `json:"status"`
	}

	stop := make(chan bool)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		RunRest(stop, wg)
	}()

	get, err := http.Get("http://localhost:8081/healthProcess")
	assert.NoError(t, err)

	body, err := io.ReadAll(get.Body)
	assert.NoError(t, err)

	expectedResponse := response{Status: "ok"}

	var actualResponse response
	err = json.Unmarshal(body, &actualResponse)
	assert.NoError(t, err)

	assert.Equal(t, expectedResponse, actualResponse)

	stop <- true

	get, err = http.Get("http://localhost:8081/healthProcess")
	assert.NoError(t, err)

	body, err = io.ReadAll(get.Body)
	assert.NoError(t, err)

	expectedResponse = response{Status: "fail"}

	err = json.Unmarshal(body, &actualResponse)
	assert.NoError(t, err)

	assert.Equal(t, expectedResponse, actualResponse)
}

func TestRecoverRunProc(t *testing.T) {
	stop := make(chan bool)
	go testRecoverRunProc(stop)
	v := <-stop
	assert.True(t, v)
}

func testRecoverRunProc(stop chan bool) {
	defer RecoverRunProc(stop)
	panic("")
}
