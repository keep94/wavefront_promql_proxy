package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/WavefrontHQ/go-wavefront-management-api"
	"github.com/keep94/toolbox/http_util"
)

func main() {
	client, err := wavefront.NewClient(
		&wavefront.Config{
			Address: os.Getenv("WAVEFRONT_ADDRESS"),
			Token:   os.Getenv("WAVEFRONT_TOKEN"),
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	http.Handle("/api/v1/query_range", &queryHandler{
		client: client,
	})
	if err := http.ListenAndServe(":9090", http.DefaultServeMux); err != nil {
		fmt.Println(err)
	}
}

type queryHandler struct {
	client *wavefront.Client
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
	wavefrontResult, err := h.SendToWavefront(wavefrontQuery)
	if err != nil {
		writeError(w, err)
		return
	}
	promQLResult, err := convertFromWavefront(wavefrontResult)
	if err != nil {
		writeError(w, err)
		return
	}
	encoder := json.NewEncoder(w)
	encoder.Encode(&promQLResult)
}

func (h *queryHandler) SendToWavefront(query *wavefrontQuery) (
	*wavefront.QueryResponse, error) {
	qp := wavefront.NewQueryParams(query.Q)
	qp.StartTime = query.S
	qp.EndTime = query.E
	qp.Granularity = query.G
	q := h.client.NewQuery(qp)
	return q.Execute()
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

func convertFromWavefront(response *wavefront.QueryResponse) (
	*promQLResponse, error) {
	if response.ErrType != "" {
		return nil, newBadDataPromQLError(response.ErrMessage)
	}
	var result promQLResponse
	result.Status = "success"
	result.Data.ResultType = "matrix"
	result.Data.Result = make([]promQLTimeSeries, len(response.TimeSeries))
	for i := range response.TimeSeries {
		result.Data.Result[i].Metric = extractPromQLMetric(&response.TimeSeries[i])
		result.Data.Result[i].Values = extractPromQLData(response.TimeSeries[i].DataPoints)
	}
	return &result, nil
}

func extractPromQLMetric(t *wavefront.TimeSeries) map[string]string {
	result := make(map[string]string)
	if t.Label != "" {
		result["__name__"] = t.Label
	}
	if t.Host != "" {
		// TODO: If there is a "host" tag, this will get clobbered
		result["host"] = t.Host
	}
	for k, v := range t.Tags {
		result[k] = v
	}
	return result
}

func extractPromQLData(data []wavefront.DataPoint) [][2]interface{} {
	result := make([][2]interface{}, len(data))
	for i := range data {
		result[i][0] = data[i][0]
		result[i][1] = strconv.FormatFloat(data[i][1], 'g', -1, 64)
	}
	return result
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

type promQLResponse struct {
	Data   promQLData `json:"data"`
	Status string     `json:"status"`
}

type promQLData struct {
	Result     []promQLTimeSeries `json:"result"`
	ResultType string             `json:"resultType"`
}

type promQLTimeSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][2]interface{}  `json:"values"`
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
