// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package fluxmq holds identity shared by every FluxMQ binary and library
// consumer.
package fluxmq

// Version is the build version, injected at link time with
// -X github.com/absmach/fluxmq.Version. Builds that do not set it report "dev".
var Version = "dev"
