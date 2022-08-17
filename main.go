package main

import (
	`bytes`
	`context`
	"flag"
	`fmt`
	`net/http`
	"os"
	`os/signal`
	`syscall`
	"time"

	`github.com/ambientsound/aidon-ams-prometheus-exporter/pkg/protocol`
	"github.com/goburrow/serial"
	`github.com/lvdlvd/go-hdlc`
	`github.com/prometheus/client_golang/prometheus`
	`github.com/prometheus/client_golang/prometheus/promhttp`
	log "github.com/sirupsen/logrus"
)

var (
	address  string
	baudrate int
	databits int
	stopbits int
	parity   string
	verbose  bool
	listen   string
)

func main() {
	flag.StringVar(&address, "a", "/dev/ttyUSB0", "address")
	flag.IntVar(&baudrate, "b", 2400, "baud rate")
	flag.IntVar(&databits, "d", 8, "data bits")
	flag.IntVar(&stopbits, "s", 1, "stop bits")
	flag.StringVar(&parity, "p", "E", "parity (N/E/O)")
	flag.BoolVar(&verbose, "v", false, "verbose output")
	flag.StringVar(&listen, "l", "0.0.0.0:8080", "listen address")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	})

	log.Infof("Aidon AMS reader V1.0")

	serialPort, err := openSerial()
	if err != nil {
		log.Fatalf("open serial port: %s", err)
	}
	defer serialPort.Close()

	log.Infof("Serial port opened")

	// Set up Prometheus metrics
	for k := range gauges {
		prometheus.MustRegister(gauges[k])
	}
	msgCounter := counter("messages_processed", "Total number of messages processed")
	resyncCounter := counter("hdlc_frame_resync", "Total number of HDLC frame re-synchronizations")
	abortCounter := counter("hdlc_frame_aborted", "Total number of HDLC frame aborts")
	parseErrorCounter := counter("parse_errors", "Total number of messages dropped due to parsing errors")
	prometheus.MustRegister(msgCounter, resyncCounter, abortCounter, parseErrorCounter)
	go func() {
		log.Infof("Started HTTP server on %s", listen)
		err := http.ListenAndServe(listen, promhttp.Handler())
		if err != nil {
			log.Errorf("HTTP server: %s", err)
			cancel()
		}
	}()

	// Input stream
	buf := make([]byte, 1024)
	unf := hdlc.Unframe(serialPort)
	packets := make(chan map[string]any, 32)

	go func() {
		for ctx.Err() == nil {
			_, err := unf.Read(buf)
			switch err {
			case hdlc.ErrResynced:
				resyncCounter.Inc()
				log.Debugf("HDLC frame re-synced")
			case hdlc.ErrAbort:
				abortCounter.Inc()
				log.Errorf("HDLC frame aborted")
			case nil:
				r := bytes.NewReader(buf[17:])
				packet, err := protocol.ParseFlattened(r)
				if err != nil {
					log.Errorf("Parse data structure: %s", err)
					parseErrorCounter.Inc()
					continue
				}
				msgCounter.Inc()
				packets <- packet
			}
		}
		log.Infof("Serial packet reading stopped")
	}()

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	for ctx.Err() == nil {
		select {
		case packet := <-packets:
			for k := range packet {
				g, ok := gauges[k]
				if !ok {
					continue
				}
				val, err := anytoint(packet[k])
				if err == nil {
					g.Set(float64(val))
				}
			}
		case sig := <-signals:
			log.Infof("Received signal %s", sig)
			cancel()
		}
	}

	log.Infof("Terminating")
}

var gauges = map[string]prometheus.Gauge{
	"1-0:1.7.0.255":  gauge("active_positive_instantaneous_value", "Active- Instantaneous value"),
	"1-0:2.7.0.255":  gauge("active_negative_instantaneous_value", "Active- Instantaneous value"),
	"1-0:3.7.0.255":  gauge("reactive_positive_instantaneous_value", "Reactive+ Instantaneous value"),
	"1-0:4.7.0.255":  gauge("reactive_negative_instantaneous_value", "Reactive- Instantaneous value"),
	"1-0:31.7.0.255": gauge("l1_current_instantaneous_value", "L1 Current Instantaneous value"),
	"1-0:51.7.0.255": gauge("l2_current_instantaneous_value", "L2 Current Instantaneous value"),
	"1-0:71.7.0.255": gauge("l3_current_instantaneous_value", "L3 Current Instantaneous value"),
	"1-0:32.7.0.255": gauge("l1_voltage_instantaneous_value", "L1 Voltage Instantaneous value"),
	"1-0:52.7.0.255": gauge("l2_voltage_instantaneous_value", "L2 Voltage Instantaneous value"),
	"1-0:72.7.0.255": gauge("l3_voltage_instantaneous_value", "L3 Voltage Instantaneous value"),
	"1-0:1.8.0.255":  gauge("active_positive_energy", "Active+ Energy"),
	"1-0:2.8.0.255":  gauge("active_negative_energy", "Active- Energy"),
	"1-0:3.8.0.255":  gauge("reactive_positive_energy", "Reactive+ Energy"),
	"1-0:4.8.0.255":  gauge("reactive_negative_energy", "Reactive- Energy"),
}

// The type system is where Golang really _shines_...
// Is there a better way to do this using generics?
func anytoint(i any) (int, error) {
	switch x := i.(type) {
	case int8:
		return int(x), nil
	case int16:
		return int(x), nil
	case int32:
		return int(x), nil
	case int64:
		return int(x), nil
	case uint8:
		return int(x), nil
	case uint16:
		return int(x), nil
	case uint32:
		return int(x), nil
	case uint64:
		return int(x), nil
	default:
		return 0, fmt.Errorf("not a number")
	}
}

func openSerial() (serial.Port, error) {
	config := serial.Config{
		Address:  address,
		BaudRate: baudrate,
		DataBits: databits,
		StopBits: stopbits,
		Parity:   parity,
		Timeout:  1 * time.Second,
	}

	log.Debugf("Serial port parameters: %+v\n", config)

	return serial.Open(&config)
}

func counter(key, description string) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ams",
		Name:      key,
		Help:      description,
	})
}

func gauge(key, description string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "ams",
		Name:      key,
		Help:      description,
	})
}
