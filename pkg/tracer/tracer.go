// Package tracer provides OpenTelemetry tracing configuration with Jaeger
package tracer

import (
	"context"
	"golang-sarama-poc/pkg/config"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
)

// InitTracer initializes and configures the OpenTelemetry tracer with Jaeger exporter
// It sets up the global tracer provider and trace context propagation
func InitTracer(jaegerConfig config.JaegerConfig) *trace.TracerProvider {
	// Configure HTTP headers for OTLP exporter
	headers := map[string]string{
		"content-type": "application/json",
	}

	// Create OTLP exporter with HTTP transport to Jaeger
	exporter, err := otlptrace.New(
		context.Background(),
		otlptracehttp.NewClient(
			otlptracehttp.WithEndpoint(jaegerConfig.URL), // Jaeger collector endpoint
			otlptracehttp.WithHeaders(headers),           // HTTP headers for requests
			otlptracehttp.WithInsecure(),                 // Use insecure connection (no TLS)
		),
	)
	if err != nil {
		zap.L().Fatal("Failed to create stdout exporter", zap.Error(err))
	}

	// Create and configure the tracer provider
	tp := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()), // Sample all traces
		trace.WithBatcher(exporter),             // Use the OTLP exporter
		trace.WithResource( // Configure resource attributes
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("golang-sarama-poc"),
			)),
	)

	// Set the global tracer provider and propagator
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, // W3C Trace Context propagation
		propagation.Baggage{},      // W3C Baggage propagation
	))
	return tp
}
