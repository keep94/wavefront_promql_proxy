wavefront_promql_proxy
======================

A proxy server that sits between a Wavefront server and a PromQL client.

To build the tool:
```sh
$ go build ./cmd/wavefront_promql_proxy
```

To use the proxy, set WAVEFRONT_ADDRESS environment variable to the address
of the Wavefront server, e.g test.wavefront.com. Set the WAVEFRONT_TOKEN
environment variable to the API token. Then run the proxy server. The proxy
server listens on port 9090. Currently it only handles query_range queries
e.g (/api/v1/query_range).

Simply send PromQL query_range requests to
http://localhost:9090/api/v1/query_range

![go CI](https://github.com/keep94/wavefront_promql_proxy/actions/workflows/go.yml/badge.svg)
