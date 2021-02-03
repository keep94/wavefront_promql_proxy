package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/keep94/toolbox/http_util"
)

func main() {
	wavefrontUrlStr := "https://" + os.Getenv("WAVEFRONT_ADDRESS") + "/api/v2/chart/api"
	wavefrontUrl, err := url.Parse(wavefrontUrlStr)
	if err != nil {
		log.Fatal(err)
	}
	http.Handle("/api/v1/query_range", &queryHandler{
		WavefrontUrl: wavefrontUrl,
		Token:        os.Getenv("WAVEFRONT_TOKEN"),
	})
	if err := http.ListenAndServe(":9090", http.DefaultServeMux); err != nil {
		fmt.Println(err)
	}
}

type queryHandler struct {
	WavefrontUrl *url.URL
	Token        string
	client       http.Client
}

func (h *queryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http_util.Error(w, http.StatusMethodNotAllowed)
		return
	}
	r.ParseForm()
	promQL, err := extractPromQL(r)
	if err != nil {
		writeError(w, err)
		return
	}
	wavefrontQuery, err := convertToWavefront(promQL)
	if err != nil {
		writeError(w, err)
		return
	}
	responseStr, httpStatusCode, err := h.SendToWavefront(wavefrontQuery)
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(httpStatusCode)

	// TODO: convert from wavefront response to promQL response
	io.Copy(w, strings.NewReader(responseStr))
}

func (h *queryHandler) SendToWavefront(query *wavefrontQuery) (
	response string, httpStatusCode int, err error) {
	wavefrontUrl := http_util.AppendParams(
		h.WavefrontUrl,
		"q", query.Q,
		"s", query.S,
		"e", query.E,
		"g", query.G,
		"strict", "true")
	header := make(http.Header)
	header.Add("Authorization", fmt.Sprintf("Bearer %s", h.Token))
	req := &http.Request{
		Method: "GET",
		URL:    wavefrontUrl,
		Header: header,
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	var builder strings.Builder
	io.Copy(&builder, resp.Body)
	return builder.String(), resp.StatusCode, nil
}

func extractPromQL(r *http.Request) (*promQLQuery, error) {
	startStr := r.Form.Get("start")
	start, err := strconv.ParseFloat(startStr, 64)
	if err != nil {
		return nil, newBadDataPromQLError(
			fmt.Sprintf("invalid parameter 'start': cannot parse \"%s\" to a valid timestamp", startStr))
	}
	endStr := r.Form.Get("end")
	end, err := strconv.ParseFloat(endStr, 64)
	if err != nil {
		return nil, newBadDataPromQLError(
			fmt.Sprintf("invalid parameter 'end': cannot parse \"%s\" to a valid timestamp", endStr))
	}
	stepStr := r.Form.Get("step")
	step, err := strconv.ParseFloat(stepStr, 64)
	if err != nil {
		return nil, newBadDataPromQLError(
			fmt.Sprintf("invalid parameter 'step': cannot parse \"%s\" to a valid duration", stepStr))
	}
	return &promQLQuery{
		Start: start,
		End:   end,
		Step:  step,
		Query: r.Form.Get("query"),
	}, nil
}

func newBadDataPromQLError(str string) *promQLError {
	return &promQLError{
		Status:    "error",
		ErrorType: "bad_data",
		Err:       str,
	}
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(400)
	io.Copy(w, strings.NewReader(err.Error()))
}

func convertToWavefront(query *promQLQuery) (*wavefrontQuery, error) {
	s := strconv.FormatInt(int64(query.Start*1000), 10)
	e := strconv.FormatInt(int64(query.End*1000), 10)
	return &wavefrontQuery{
		Q: query.Query,
		S: s,
		E: e,
		G: "s",
	}, nil
}

type promQLQuery struct {
	Start float64
	End   float64
	Step  float64
	Query string
}

type wavefrontQuery struct {
	Q string
	S string
	E string
	G string
}

type promQLError struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Err       string `json:"error"`
}

func (p *promQLError) Error() string {
	jsonBytes, err := json.Marshal(p)
	if err != nil {
		return err.Error()
	}
	return string(jsonBytes)
}
