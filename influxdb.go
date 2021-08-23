package influxdb

import (
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/rcrowley/go-metrics"
	"time"
)

type reporter struct {
	reg      metrics.Registry
	interval time.Duration

	url    string
	token  string
	org    string
	bucket string
	tags   map[string]string

	client influxdb2.Client
	wapi   api.WriteAPI
}

func InfluxDB(r metrics.Registry, d time.Duration, url, token, org, bucket string) {
	InfluxDBWithTags(r, d, url, token, org, bucket, nil)
}

func InfluxDBWithTags(r metrics.Registry, d time.Duration, url, token, org, bucket string, tags map[string]string) {
	rp := &reporter{
		reg:      r,
		interval: d,
		url:      url,
		token:    token,
		org:      org,
		bucket:   bucket,
		tags:     tags,
	}

	rp.makeClient()
	rp.run()
}

func (r *reporter) makeClient() {
	opt := influxdb2.DefaultOptions()
	// opt.SetLogLevel(log.DebugLevel)
	r.client = influxdb2.NewClientWithOptions(r.url, r.token, opt)
	r.wapi = r.client.WriteAPI(r.org, r.bucket)
}

func (r *reporter) run() {
	intervalTk := time.NewTicker(r.interval)
	defer intervalTk.Stop()

	for {
		select {
		case <-intervalTk.C:
			r.send()

		case <-r.wapi.Errors():
			r.makeClient()
		}
	}
}

func (r *reporter) send() {
	r.reg.Each(func(name string, i interface{}) {
		now := time.Now()

		switch metric := i.(type) {
		case metrics.Counter:
			ms := metric.Snapshot()
			r.wapi.WritePoint(influxdb2.NewPoint(fmt.Sprintf("%s.count", name),
				r.tags,
				map[string]interface{}{
					"value": ms.Count(),
				},
				now,
			))

		case metrics.Gauge:
			ms := metric.Snapshot()
			r.wapi.WritePoint(influxdb2.NewPoint(fmt.Sprintf("%s.gauge", name),
				r.tags,
				map[string]interface{}{
					"value": ms.Value(),
				},
				now,
			))

		case metrics.GaugeFloat64:
			ms := metric.Snapshot()
			r.wapi.WritePoint(influxdb2.NewPoint(fmt.Sprintf("%s.gauge", name),
				r.tags,
				map[string]interface{}{
					"value": ms.Value(),
				},
				now,
			))

		case metrics.Histogram:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			r.wapi.WritePoint(influxdb2.NewPoint(fmt.Sprintf("%s.histogram", name),
				r.tags,
				map[string]interface{}{
					"count":    ms.Count(),
					"max":      ms.Max(),
					"mean":     ms.Mean(),
					"min":      ms.Min(),
					"stddev":   ms.StdDev(),
					"variance": ms.Variance(),
					"p50":      ps[0],
					"p75":      ps[1],
					"p95":      ps[2],
					"p99":      ps[3],
					"p999":     ps[4],
					"p9999":    ps[5],
				},
				now,
			))

		case metrics.Meter:
			ms := metric.Snapshot()
			r.wapi.WritePoint(influxdb2.NewPoint(fmt.Sprintf("%s.meter", name),
				r.tags,
				map[string]interface{}{
					"count": ms.Count(),
					"m1":    ms.Rate1(),
					"m5":    ms.Rate5(),
					"m15":   ms.Rate15(),
					"mean":  ms.RateMean(),
				},
				now,
			))

		case metrics.Timer:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			r.wapi.WritePoint(influxdb2.NewPoint(fmt.Sprintf("%s.timer", name),
				r.tags,
				map[string]interface{}{
					"count":    ms.Count(),
					"max":      ms.Max(),
					"mean":     ms.Mean(),
					"min":      ms.Min(),
					"stddev":   ms.StdDev(),
					"variance": ms.Variance(),
					"p50":      ps[0],
					"p75":      ps[1],
					"p95":      ps[2],
					"p99":      ps[3],
					"p999":     ps[4],
					"p9999":    ps[5],
					"m1":       ms.Rate1(),
					"m5":       ms.Rate5(),
					"m15":      ms.Rate15(),
					"meanrate": ms.RateMean(),
				},
				now,
			))

		}
	})

	r.wapi.Flush()
}
