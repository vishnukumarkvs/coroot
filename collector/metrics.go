package collector

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	promModel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"k8s.io/klog"
)

var (
	secureClient = &http.Client{Transport: &http.Transport{
		TLSHandshakeTimeout: 10 * time.Second,
	}}
	insecureClient = &http.Client{Transport: &http.Transport{
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
	}}
)

func addLabelsIfNeeded(r *http.Request, body []byte, extraLabels map[string]string) ([]byte, error) {
	if len(extraLabels) == 0 {
		return body, nil
	}
	req, err := parseMetricsRequestBody(r, body)
	if err != nil {
		return nil, err
	}
	for i := range req.Timeseries {
		for k, v := range extraLabels {
			req.Timeseries[i].Labels = append(req.Timeseries[i].Labels, prompb.Label{Name: k, Value: v})
		}
	}
	decompressed, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, decompressed), nil
}

func (c *Collector) Metrics(w http.ResponseWriter, r *http.Request) {
	project, err := c.getProject(r.Header.Get(ApiKeyHeader))
	if err != nil {
		klog.Errorln(err)
		if errors.Is(err, ErrProjectNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	cfg := project.PrometheusConfig(c.globalPrometheus)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		klog.Errorln(err)
		http.Error(w, "", http.StatusBadRequest)
	}
	if cfg.UseClickHouse {
		req, err := parseMetricsRequestBody(r, body)
		if err != nil {
			klog.Errorln(err)
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		c.getMetricsBatch(project).Add(req)
		return
	}

	var u *url.URL
	if cfg.RemoteWriteUrl == "" {
		u, err = url.Parse(cfg.Url)
		if err != nil {
			klog.Errorln(err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
		u = u.JoinPath("/api/v1/write")
	} else {
		u, err = url.Parse(cfg.RemoteWriteUrl)
		if err != nil {
			klog.Errorln(err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
	}

	if cfg.BasicAuth != nil {
		u.User = url.UserPassword(cfg.BasicAuth.User, cfg.BasicAuth.Password)
	}
	body, err = addLabelsIfNeeded(r, body, cfg.ExtraLabels)
	if err != nil {
		klog.Errorln(err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, u.String(), bytes.NewReader(body))
	if err != nil {
		klog.Errorln(err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	for _, h := range cfg.CustomHeaders {
		req.Header.Add(h.Key, h.Value)
	}
	for k, vs := range r.Header {
		if k == ApiKeyHeader {
			continue
		}
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	httpClient := secureClient
	if cfg.TlsSkipVerify {
		httpClient = insecureClient
	}
	res, err := httpClient.Do(req)
	if err != nil {
		klog.Errorln(err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	defer func() {
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}()
	for k, vs := range res.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	if res.StatusCode == http.StatusBadRequest {
		scanner := bufio.NewScanner(io.LimitReader(res.Body, 1024))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		klog.Errorf("failed to write: got %d (%s) from prometheus, responding to the agent with 200 (to prevent retry)", res.StatusCode, line)
		w.WriteHeader(http.StatusOK)
		return
	} else if res.StatusCode > 400 {
		klog.Errorf("failed to write: got %d from prometheus", res.StatusCode)
	}
	w.WriteHeader(res.StatusCode)
	_, _ = io.Copy(w, res.Body)
}

func parseMetricsRequestBody(r *http.Request, body []byte) (*prompb.WriteRequest, error) {
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		return nil, fmt.Errorf("expected application/x-protobuf content-type")
	}
	if r.Header.Get("Content-Encoding") != "snappy" {
		return nil, fmt.Errorf("expected snappy content-encoding")
	}

	decompressed, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, err
	}

	var req prompb.WriteRequest
	if err = proto.Unmarshal(decompressed, &req); err != nil {
		return nil, err
	}

	return &req, nil
}

type columnBatch struct {
	Timestamp        *chproto.ColDateTime64
	MetricHash       *chproto.ColUInt64
	Value            *chproto.ColFloat64
	MetricName       *chproto.ColLowCardinality[string]
	Labels           *chproto.ColMap[string, string]
	MetricFamilyName *chproto.ColLowCardinality[string]
	Type             *chproto.ColLowCardinality[string]
	Help             *chproto.ColStr
	Unit             *chproto.ColLowCardinality[string]
}

type MetricsBatch struct {
	limit int
	exec  func(query ch.Query) error

	lock sync.Mutex
	done chan struct{}

	Timestamp  *chproto.ColDateTime64
	MetricHash *chproto.ColUInt64
	Value      *chproto.ColFloat64
	MetricName *chproto.ColLowCardinality[string]
	Labels     *chproto.ColMap[string, string]

	MetricFamilyName *chproto.ColLowCardinality[string]
	Type             *chproto.ColLowCardinality[string]
	Help             *chproto.ColStr
	Unit             *chproto.ColLowCardinality[string]
}

func NewMetricsBatch(limit int, timeout time.Duration, exec func(query ch.Query) error) *MetricsBatch {
	b := &MetricsBatch{
		limit: limit,
		exec:  exec,
		done:  make(chan struct{}),

		Timestamp:  new(chproto.ColDateTime64).WithPrecision(chproto.PrecisionMilli),
		MetricHash: new(chproto.ColUInt64),
		Value:      new(chproto.ColFloat64),
		MetricName: new(chproto.ColStr).LowCardinality(),
		Labels:     chproto.NewMap[string, string](new(chproto.ColStr).LowCardinality(), new(chproto.ColStr)),

		MetricFamilyName: new(chproto.ColStr).LowCardinality(),
		Type:             new(chproto.ColStr).LowCardinality(),
		Help:             new(chproto.ColStr),
		Unit:             new(chproto.ColStr).LowCardinality(),
	}

	go func() {
		ticker := time.NewTicker(timeout)
		defer ticker.Stop()
		for {
			select {
			case <-b.done:
				b.lock.Lock()
				if b.Timestamp.Rows() > 0 {
					batch := b.snapshot()
					b.lock.Unlock()
					b.saveBatch(batch)
				} else {
					b.lock.Unlock()
				}
				return
			case <-ticker.C:
				b.lock.Lock()
				if b.Timestamp.Rows() == 0 {
					b.lock.Unlock()
					continue
				}
				batch := b.snapshot()
				b.lock.Unlock()
				b.saveBatch(batch)
			}
		}
	}()

	return b
}

func (b *MetricsBatch) snapshot() *columnBatch {
	batch := &columnBatch{
		Timestamp:        b.Timestamp,
		MetricHash:       b.MetricHash,
		Value:            b.Value,
		MetricName:       b.MetricName,
		Labels:           b.Labels,
		MetricFamilyName: b.MetricFamilyName,
		Type:             b.Type,
		Help:             b.Help,
		Unit:             b.Unit,
	}
	b.Timestamp = new(chproto.ColDateTime64).WithPrecision(chproto.PrecisionMilli)
	b.MetricHash = new(chproto.ColUInt64)
	b.Value = new(chproto.ColFloat64)
	b.MetricName = new(chproto.ColStr).LowCardinality()
	b.Labels = chproto.NewMap[string, string](new(chproto.ColStr).LowCardinality(), new(chproto.ColStr))
	b.MetricFamilyName = new(chproto.ColStr).LowCardinality()
	b.Type = new(chproto.ColStr).LowCardinality()
	b.Help = new(chproto.ColStr)
	b.Unit = new(chproto.ColStr).LowCardinality()
	return batch
}

func (b *MetricsBatch) Close() {
	close(b.done)
}

func (b *MetricsBatch) Add(req *prompb.WriteRequest) {
	type preprocessedSample struct {
		timestamp int64
		value     float64
	}
	type preprocessedTimeseries struct {
		metricName string
		labels     []chproto.KV[string, string]
		hash       uint64
		samples    []preprocessedSample
	}

	metadataFamilyNames := make([]string, 0, len(req.Metadata))
	metadataTypes := make([]string, 0, len(req.Metadata))
	metadataHelps := make([]string, 0, len(req.Metadata))
	metadataUnits := make([]string, 0, len(req.Metadata))
	for _, md := range req.GetMetadata() {
		metadataFamilyNames = append(metadataFamilyNames, md.GetMetricFamilyName())
		metadataTypes = append(metadataTypes, md.GetType().String())
		metadataHelps = append(metadataHelps, md.GetHelp())
		metadataUnits = append(metadataUnits, md.GetUnit())
	}

	preprocessedList := make([]preprocessedTimeseries, 0, len(req.Timeseries))
	for _, ts := range req.GetTimeseries() {
		pp := preprocessedTimeseries{
			labels:  make([]chproto.KV[string, string], 0, len(ts.Labels)),
			samples: make([]preprocessedSample, 0, len(ts.Samples)),
		}
		for _, l := range ts.Labels {
			if l.Name == promModel.MetricNameLabel {
				pp.metricName = l.Value
			} else {
				pp.labels = append(pp.labels, chproto.KV[string, string]{Key: l.Name, Value: l.Value})
			}
		}
		sort.Slice(pp.labels, func(i, j int) bool {
			return pp.labels[i].Key < pp.labels[j].Key
		})
		h := fnv.New64a()
		h.Write([]byte(pp.metricName))
		h.Write([]byte{0})
		for _, kv := range pp.labels {
			h.Write([]byte(kv.Key))
			h.Write([]byte{0})
			h.Write([]byte(kv.Value))
			h.Write([]byte{0})
		}
		pp.hash = h.Sum64()
		for _, s := range ts.Samples {
			pp.samples = append(pp.samples, preprocessedSample{timestamp: s.Timestamp, value: s.Value})
		}
		preprocessedList = append(preprocessedList, pp)
	}

	var batch *columnBatch
	b.lock.Lock()

	for i := range metadataFamilyNames {
		b.MetricFamilyName.Append(metadataFamilyNames[i])
		b.Type.Append(metadataTypes[i])
		b.Help.Append(metadataHelps[i])
		b.Unit.Append(metadataUnits[i])
	}

	for _, pp := range preprocessedList {
		for _, s := range pp.samples {
			b.MetricName.Append(pp.metricName)
			b.Labels.AppendKV(pp.labels)
			b.Timestamp.Append(time.Unix(s.timestamp/1000, 0))
			b.MetricHash.Append(pp.hash)
			b.Value.Append(s.value)
		}
	}

	if b.Timestamp.Rows() >= b.limit {
		batch = b.snapshot()
	}
	b.lock.Unlock()

	if batch != nil {
		b.saveBatch(batch)
	}
}

func (b *MetricsBatch) saveBatch(batch *columnBatch) {
	if batch.Timestamp.Rows() == 0 {
		return
	}

	input := chproto.Input{
		chproto.InputColumn{Name: "MetricName", Data: batch.MetricName},
		chproto.InputColumn{Name: "Labels", Data: batch.Labels},
		chproto.InputColumn{Name: "Timestamp", Data: batch.Timestamp},
		chproto.InputColumn{Name: "MetricHash", Data: batch.MetricHash},
		chproto.InputColumn{Name: "Value", Data: batch.Value},
	}
	if err := b.exec(ch.Query{Body: input.Into("@@table_metrics@@"), Input: input}); err != nil {
		klog.Errorln("failed to insert metrics:", err)
		return
	}

	if batch.MetricFamilyName.Rows() == 0 {
		return
	}
	input = chproto.Input{
		chproto.InputColumn{Name: "MetricFamilyName", Data: batch.MetricFamilyName},
		chproto.InputColumn{Name: "Type", Data: batch.Type},
		chproto.InputColumn{Name: "Help", Data: batch.Help},
		chproto.InputColumn{Name: "Unit", Data: batch.Unit},
	}
	if err := b.exec(ch.Query{Body: input.Into("@@table_metrics_metadata@@"), Input: input}); err != nil {
		klog.Errorln("failed to insert metrics metadata:", err)
	}
}
