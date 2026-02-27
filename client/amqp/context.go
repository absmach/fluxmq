// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import "context"

func runWithContext(ctx context.Context, fn func() error) error {
	if ctx == nil {
		return fn()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
