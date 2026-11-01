// Package health probes the service's runtime dependencies.
//
// Readiness must tell the truth: /readyz only reports 200 when every
// registered dependency answers. This package owns that probe logic so the
// app layer only wires checkers in and reads the result map out.
package health

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Checker is a single named dependency probe.
type Checker interface {
	// Name identifies the dependency in readiness responses and logs.
	Name() string
	// Check returns nil when the dependency is usable, and an error
	// describing what is wrong otherwise. Implementations must honour ctx.
	Check(ctx context.Context) error
}

// Registry runs a set of checkers concurrently and returns one result per
// dependency, keyed by checker name.
type Registry struct {
	checkers []Checker
	// timeout bounds each individual check, so one slow dependency cannot
	// stall the readiness endpoint.
	timeout time.Duration
}

// NewRegistry builds a registry. Checkers are copied, so the caller may
// reuse the slice afterwards.
func NewRegistry(timeout time.Duration, checkers ...Checker) *Registry {
	cs := make([]Checker, len(checkers))
	copy(cs, checkers)
	return &Registry{checkers: cs, timeout: timeout}
}

// Names lists the registered dependency names in registration order.
func (r *Registry) Names() []string {
	names := make([]string, len(r.checkers))
	for i, c := range r.checkers {
		names[i] = c.Name()
	}
	return names
}

// Check runs every checker concurrently, each under its own timeout, and
// returns a map of dependency name to error. A nil value means the
// dependency passed. The map has one entry per registered checker, even
// when the parent ctx is already cancelled.
func (r *Registry) Check(ctx context.Context) map[string]error {
	results := make(map[string]error, len(r.checkers))
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, c := range r.checkers {
		wg.Add(1)
		go func(c Checker) {
			defer wg.Done()
			checkCtx, cancel := context.WithTimeout(ctx, r.timeout)
			defer cancel()
			err := c.Check(checkCtx)
			mu.Lock()
			results[c.Name()] = err
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	return results
}

// Failed returns the names of dependencies whose checks errored, sorted.
func Failed(results map[string]error) []string {
	var failed []string
	for name, err := range results {
		if err != nil {
			failed = append(failed, name)
		}
	}
	sort.Strings(failed)
	return failed
}
