// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package otel

import (
	"context"
	"fmt"
	"time"

	"github.com/absmach/fluxmq/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// InitProvider initializes OpenTelemetry SDK with OTLP exporters.
// Returns a shutdown function that should be called on application exit.
func InitProvider(cfg config.ServerConfig, nodeID string) (func(context.Context) error, error) {
	ctx := context.Background()

	// Create resource with service information
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.OtelServiceName),
			semconv.ServiceVersionKey.String(cfg.OtelServiceVersion),
			semconv.ServiceInstanceIDKey.String(nodeID),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var shutdownFuncs []func(context.Context) error

	// Initialize TracerProvider
	if cfg.OtelTracesEnabled {
		traceShutdown, err := initTracerProvider(ctx, cfg, res)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize tracer provider: %w", err)
		}
		shutdownFuncs = append(shutdownFuncs, traceShutdown)
	} else {
		// Use noop tracer provider for zero overhead
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
	}

	// Initialize MeterProvider (always enabled if metrics are enabled)
	if cfg.OtelMetricsEnabled {
		meterShutdown, err := initMeterProvider(ctx, cfg, res)
		if err != nil {
			// Cleanup trace provider if meter fails
			for _, fn := range shutdownFuncs {
				_ = fn(ctx)
			}
			return nil, fmt.Errorf("failed to initialize meter provider: %w", err)
		}
		shutdownFuncs = append(shutdownFuncs, meterShutdown)
	}

	// Return combined shutdown function
	return func(ctx context.Context) error {
		var errs []error
		for _, fn := range shutdownFuncs {
			if err := fn(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("shutdown errors: %v", errs)
		}
		return nil
	}, nil
}

// initTracerProvider creates and registers a TracerProvider with OTLP exporter.
func initTracerProvider(ctx context.Context, cfg config.ServerConfig, res *resource.Resource) (func(context.Context) error, error) {
	// Create OTLP trace exporter
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.MetricsAddr),
		otlptracegrpc.WithInsecure(), // TODO: Add TLS support via config
		otlptracegrpc.WithTimeout(30*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// Create sampler based on sample rate
	sampler := trace.ParentBased(trace.TraceIDRatioBased(cfg.OtelTraceSampleRate))

	// Create TracerProvider with batch span processor
	tp := trace.NewTracerProvider(
		trace.WithResource(res),
		trace.WithSampler(sampler),
		trace.WithBatcher(exporter,
			trace.WithMaxExportBatchSize(512),
			trace.WithBatchTimeout(5*time.Second),
		),
	)

	// Register as global tracer provider
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// initMeterProvider creates and registers a MeterProvider with OTLP exporter.
func initMeterProvider(ctx context.Context, cfg config.ServerConfig, res *resource.Resource) (func(context.Context) error, error) {
	// Create OTLP metric exporter
	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(cfg.MetricsAddr),
		otlpmetricgrpc.WithInsecure(), // TODO: Add TLS support via config
		otlpmetricgrpc.WithTimeout(30*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}

	// Create MeterProvider with periodic reader
	mp := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(exporter,
			metric.WithInterval(10*time.Second),
		)),
	)

	// Register as global meter provider
	otel.SetMeterProvider(mp)

	return mp.Shutdown, nil
}
