package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/WavefrontHQ/go-wavefront-management-api"
	"github.com/keep94/toolbox/http_util"
)

var (
	fPort string
	fSkew time.Duration
)

func main() {
	flag.Parse()
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
		skew:   fSkew,
	})
	if err := http.ListenAndServe(fPort, http.DefaultServeMux); err != nil {
		fmt.Println(err)
	}
}

type queryHandler struct {
	client *wavefront.Client
	skew   time.Duration
}

func (h *queryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "POST" {
		http_util.Error(w, http.StatusMethodNotAllowed)
		return
	}
	r.ParseForm()
	promQL, err := extractPromQL(r)
	if err != nil {
		writeError(w, err)
		return
	}
	wavefrontQuery, err := h.convertToWavefrontAndSkewEarlier(promQL)
	if err != nil {
		writeError(w, err)
		return
	}
	wavefrontResult, err := h.sendToWavefrontAndSkewLater(wavefrontQuery)
	if err != nil {
		writeError(w, err)
		return
	}
	promQLResult, err := convertFromWavefront(wavefrontResult, promQL)
	if err != nil {
		writeError(w, err)
		return
	}
	encoder := json.NewEncoder(w)
	encoder.Encode(&promQLResult)
}

func (h *queryHandler) convertToWavefrontAndSkewEarlier(
	query *promQLQuery) (*wavefrontQuery, error) {

	skew := float64(h.skew) / float64(time.Second)

	// We set the wavefront start time to be 15s before the promQL start time.
	// We do this because otherwise, the first Wavefront data point may be
	// after start time, and we won't get the correct value for start time.
	// This isn't perfect as there is no guarantee that going 15s back is
	// sufficient.
	s := strconv.FormatInt(int64((query.Start-15.0-skew)*1000), 10)

	// In promQL, end time is inclusive, but in Wavefront it is exclusive.
	// In wavefront times have to be at 1000ms less than end time.
	e := strconv.FormatInt(int64((query.End+1.0-skew)*1000), 10)

	// Here we set g=s to get a step of one second from wavefront. Later
	// we will apply the step parameter from promQL when converting the
	// response back to promQL.
	return &wavefrontQuery{
		Q: query.Query,
		S: s,
		E: e,
		G: "s",
	}, nil
}

func (h *queryHandler) sendToWavefrontAndSkewLater(query *wavefrontQuery) (
	*wavefront.QueryResponse, error) {
	qp := wavefront.NewQueryParams(query.Q)
	qp.StartTime = query.S
	qp.EndTime = query.E
	qp.Granularity = query.G
	q := h.client.NewQuery(qp)
	response, err := q.Execute()
	if err != nil {
		return nil, err
	}
	return h.skewLater(response), nil
}

func (h *queryHandler) skewLater(
	response *wavefront.QueryResponse) *wavefront.QueryResponse {

	skew := float64(h.skew) / float64(time.Second)

	for i := range response.TimeSeries {
		for j := range response.TimeSeries[i].DataPoints {
			response.TimeSeries[i].DataPoints[j][0] += skew
		}
	}
	return response
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
	if step <= 0.0 {
		return nil, newBadDataPromQLError(
			"zero or negative query resolution step widths are not accepted. Try a positive integer")
	}
	if end < start {
		return nil, newBadDataPromQLError(
			"end timestamp must not be before start time")
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

func convertFromWavefront(
	response *wavefront.QueryResponse, query *promQLQuery) (
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
		result.Data.Result[i].Values = extractPromQLData(
			response.TimeSeries[i].DataPoints, query)
	}
	sortTimeSeriesInPlace(result.Data.Result)
	return &result, nil
}

func extractPromQLMetric(t *wavefront.TimeSeries) map[string]string {
	result := make(map[string]string)
	if t.Label != "" {
		result["__name__"] = t.Label
	}
	if t.Host != "" {
		// TODO: If there is a "instance" tag, this will get clobbered
		result["instance"] = t.Host
	}
	for k, v := range t.Tags {
		result[k] = v
	}
	return result
}

func floatToString(x float64) string {
	return strconv.FormatFloat(x, 'g', -1, 64)
}

// Here we are trying to simulate the step functionality of promQL. While
// this code works most of the time, it is not perfect because the
// wavefront data itself has granularity of 1s, 5s, or whatever. It really
// isn't possible to tell what the value is at an arbitrary time. What we
// do here, is we just assume that the last reported data value is correct,
// but this may or may not be the case.
func extractPromQLData(
	data []wavefront.DataPoint, query *promQLQuery) [][2]interface{} {
	if len(data) == 0 {
		return make([][2]interface{}, 0)
	}
	resultSize := int((query.End-query.Start)/query.Step) + 1
	var result [][2]interface{}
	indexPlus1 := 1
	for i := 0; i < resultSize; i++ {
		timestamp := query.Start + float64(i)*query.Step
		for indexPlus1 < len(data) && data[indexPlus1][0] <= timestamp {
			indexPlus1++
		}
		timestampdiff := timestamp - data[indexPlus1-1][0]
		if timestampdiff >= 0 && timestampdiff < query.Step {
			result = append(result, [2]interface{}{
				timestamp, floatToString(data[indexPlus1-1][1])})
		}
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

func sortTimeSeriesInPlace(timeSeries []promQLTimeSeries) {
	sorter := promQLTimeSeriesSorter{timeSeries: timeSeries}
	sorter.initialize()
	sort.Sort(&sorter)
}

type promQLTimeSeriesSorter struct {
	timeSeries      []promQLTimeSeries
	metricKeyValues [][]string
}

func (p *promQLTimeSeriesSorter) initialize() {
	p.metricKeyValues = make([][]string, len(p.timeSeries))
	for i := range p.timeSeries {
		p.metricKeyValues[i] = metricMapToSlice(p.timeSeries[i].Metric)
	}
}

func (p *promQLTimeSeriesSorter) Less(i, j int) bool {
	return sliceLess(p.metricKeyValues[i], p.metricKeyValues[j])
}

func (p *promQLTimeSeriesSorter) Swap(i, j int) {
	p.metricKeyValues[i], p.metricKeyValues[j] = p.metricKeyValues[j], p.metricKeyValues[i]
	p.timeSeries[i], p.timeSeries[j] = p.timeSeries[j], p.timeSeries[i]
}

func (p *promQLTimeSeriesSorter) Len() int {
	return len(p.timeSeries)
}

func metricMapToSlice(metric map[string]string) []string {
	keys := make([]string, 0, len(metric))
	for key := range metric {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, 2*len(metric))
	for _, key := range keys {
		result = append(result, key, metric[key])
	}
	return result
}

func sliceLess(lhs, rhs []string) bool {
	i := 0
	for i < len(lhs) && i < len(rhs) {
		if lhs[i] < rhs[i] {
			return true
		}
		if lhs[i] > rhs[i] {
			return false
		}
		i++
	}
	return len(lhs) < len(rhs)
}

func init() {
	flag.StringVar(&fPort, "http", ":9090", "Port to bind")
	flag.DurationVar(&fSkew, "skew", 0, "Amount of time wavefront is earlier")
}
