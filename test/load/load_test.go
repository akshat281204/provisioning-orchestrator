//go:build integration

// load_test.go — the load profile. Creates environments at a fixed rate,
// observes each one until it reaches a verdict, and reports the
// time-to-ready distribution, the error rate, and the duplicate-resource
// count. The duplicate count is asserted to be zero: two databases or two
// roles for one environment is the failure the lease/idempotence design
// exists to prevent, and the load run is where that claim is tested.
//
// How duplicates are actually detected: a database or role name is derived
// deterministically from the environment id and both catalog columns are
// UNIQUE, so a per-name count can never exceed one and could never fail.
// Instead the run snapshots every provisioned-prefixed resource
// (`px_%` databases, `pu_%` roles) before and after, and requires the delta
// to equal the number of resources attributed to the run's environments —
// one database and one role each. An extra, orphaned, or unexplained
// resource makes the assertion fail even though it cannot share a name with
// a counted one.
package load

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/domain"
)

// Profile knobs (env-overridable, all with documented defaults).
func envCount() int { return envInt("LOAD_ENVS", 16) }

func creationRate() float64 { return float64(envInt("LOAD_RATE_PER_S", 4)) }

func workerCount() int { return envInt("LOAD_WORKERS", 4) }

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// sample is one environment's load observation.
type sample struct {
	EnvID     string `json:"environmentId"`
	CreatedAt time.Time
	ReadyAt   time.Time
	Ready     bool   `json:"ready"`
	Error     string `json:"error,omitempty"`

	TimeToReady time.Duration `json:"-"`
	// JSON form of the duration (ms) for the machine-readable report.
	TimeToReadyMs float64 `json:"timeToReadyMs,omitempty"`

	DatabaseRows int `json:"databaseRows"`
	RoleRows     int `json:"roleRows"`
}

// report is the machine-readable summary written by scripts/benchmark.sh.
type report struct {
	StartedAt     string  `json:"startedAt"`
	Envs          int     `json:"environments"`
	RatePerSecond float64 `json:"ratePerSecond"`
	Workers       int     `json:"workers"`
	ReadyCount    int     `json:"readyCount"`
	ErrorRate     float64 `json:"errorRate"`

	P50 float64 `json:"timeToReadyP50ms"`
	P95 float64 `json:"timeToReadyP95ms"`
	P99 float64 `json:"timeToReadyP99ms"`
	Max float64 `json:"timeToReadyMaxMs"`

	// External accounting over the provisioned prefixes: how many px_/pu_
	// resources appeared during the run, and how many of those are not
	// attributable to one of the run's environments (an orphan — always a
	// failure). DuplicateResources counts per-environment extras plus
	// unattributed resources, and must be zero.
	CreatedDatabases      int `json:"createdDatabases"`
	CreatedRoles          int `json:"createdRoles"`
	UnattributedResources int `json:"unattributedResources"`
	DuplicateResources    int `json:"duplicateResources"`
}

// TestLoadProfileConcurrentCreation runs the profile end to end and asserts
// its two hard guarantees: every environment becomes ready, and no
// environment ever ends up with duplicate external resources.
func TestLoadProfileConcurrentCreation(t *testing.T) {
	rig := newLoadRig(t)
	n, rate, workers := envCount(), creationRate(), workerCount()

	// Capacity must not choke the run: size the limit to the reservations
	// already on the shared stack plus this run's environments.
	rig.capacity.Limit = rig.totalReservations(t) + n

	// Snapshot the provisioned prefixes before anything runs: the duplicate
	// check needs a before/after delta, not per-name counts (those can
	// structurally never exceed one).
	beforeDatabases := rig.prefixDatabases(t)
	beforeRoles := rig.prefixRoles(t)

	rig.startPool(t, workers)

	// Create at the documented rate and observe every environment until it
	// reaches a verdict, recording the ready instant per environment.
	started := time.Now().UTC()
	samples := make([]sample, n)
	var wg sync.WaitGroup
	var mu sync.Mutex
	deadline := time.Now().Add(4 * time.Minute)
	for i := 0; i < n; i++ {
		if i > 0 {
			time.Sleep(time.Duration(float64(time.Second) / rate))
		}
		created := time.Now()
		envID := rig.createEnv(t, fmt.Sprintf("load-%d", i))
		samples[i] = sample{EnvID: envID, CreatedAt: created}
		wg.Add(1)
		go func(s *sample) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				state := rig.envState(t, s.EnvID)
				if state == "ready" {
					mu.Lock()
					s.Ready = true
					s.ReadyAt = time.Now()
					mu.Unlock()
					return
				}
				if isTerminal(state) {
					mu.Lock()
					s.Error = "terminated without ready: " + string(state)
					mu.Unlock()
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			mu.Lock()
			s.Error = "did not converge within the deadline (last state " + string(rig.envState(t, s.EnvID)) + ")"
			mu.Unlock()
		}(&samples[i])
	}
	wg.Wait()

	// External truth: count the databases and roles every environment left
	// behind, then snapshot the prefixes again — the delta is the number of
	// resources the run created, and every one of them must belong to one of
	// the run's environments.
	for i := range samples {
		samples[i].DatabaseRows = rig.countDatabase(t, rig.databaseName(samples[i].EnvID))
		samples[i].RoleRows = rig.countRole(t, rig.roleName(samples[i].EnvID))
		if samples[i].Ready {
			samples[i].TimeToReady = samples[i].ReadyAt.Sub(samples[i].CreatedAt)
			samples[i].TimeToReadyMs = ms(samples[i].TimeToReady)
		}
	}
	createdDatabases := rig.prefixDatabases(t) - beforeDatabases
	createdRoles := rig.prefixRoles(t) - beforeRoles

	rep := summarize(started, n, rate, workers, samples, createdDatabases, createdRoles)
	printReport(t, rep, samples)

	// The guarantees.
	if rep.ReadyCount != n {
		t.Fatalf("only %d of %d environments became ready", rep.ReadyCount, n)
	}
	if rep.UnattributedResources != 0 {
		t.Fatalf("%d external resources appeared during the run without belonging to one of "+
			"the run's environments (created %d databases / %d roles, environments hold %d / %d) — "+
			"an orphaned resource is a leak, never acceptable",
			rep.UnattributedResources, rep.CreatedDatabases, rep.CreatedRoles,
			sumRows(samples, true), sumRows(samples, false))
	}
	if rep.DuplicateResources != 0 {
		t.Fatalf("the load run produced %d duplicate external resources — never acceptable",
			rep.DuplicateResources)
	}

	if path := os.Getenv("LOAD_RESULTS_FILE"); path != "" {
		writeJSONReport(t, path, rep, samples)
	}
}

// isTerminal reports whether an environment state ends the observation.
func isTerminal(state domain.EnvState) bool {
	switch state {
	case "failed", "rolled_back", "cancelled", "deleted":
		return true
	}
	return false
}

// summarize computes the percentiles and error rate over the samples, and
// reconciles the per-environment resource counts against the run's
// before/after prefix deltas. A database or role the run created but no run
// environment holds is unattributed (an orphan); per-environment counts
// above one are duplicates. Both must be zero.
func summarize(started time.Time, n int, rate float64, workers int, samples []sample,
	createdDatabases, createdRoles int) report {
	rep := report{
		StartedAt: started.Format(time.RFC3339),
		Envs:      n, RatePerSecond: rate, Workers: workers,
		CreatedDatabases: createdDatabases, CreatedRoles: createdRoles,
	}
	durs := make([]float64, 0, len(samples))
	for i := range samples {
		if samples[i].Ready {
			rep.ReadyCount++
			durs = append(durs, samples[i].TimeToReadyMs)
		}
		if samples[i].DatabaseRows > 1 {
			rep.DuplicateResources += samples[i].DatabaseRows - 1
		}
		if samples[i].RoleRows > 1 {
			rep.DuplicateResources += samples[i].RoleRows - 1
		}
	}
	// Attribution: every resource the run created must be held by a run
	// environment. A negative delta is equally wrong — a pre-existing
	// resource matching a run environment's name means the deterministic
	// naming collided with something outside the run.
	rep.UnattributedResources = (createdDatabases - sumRows(samples, true)) +
		(createdRoles - sumRows(samples, false))
	rep.DuplicateResources += max(rep.UnattributedResources, 0)
	rep.ErrorRate = float64(n-rep.ReadyCount) / float64(n)
	sort.Float64s(durs)
	if len(durs) > 0 {
		rep.P50 = percentile(durs, 0.50)
		rep.P95 = percentile(durs, 0.95)
		rep.P99 = percentile(durs, 0.99)
		rep.Max = durs[len(durs)-1]
	}
	return rep
}

// sumRows totals the per-environment catalog counts (databases when
// databases is true, roles otherwise).
func sumRows(samples []sample, databases bool) int {
	total := 0
	for i := range samples {
		if databases {
			total += samples[i].DatabaseRows
		} else {
			total += samples[i].RoleRows
		}
	}
	return total
}

// percentile returns the q-quantile of a sorted slice.
func percentile(sorted []float64, q float64) float64 {
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

// ms renders a duration in whole milliseconds.
func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// printReport renders the human-readable load report.
func printReport(t *testing.T, rep report, samples []sample) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "\n=== load profile report ===\n")
	fmt.Fprintf(&b, "profile:        %d environments at %.1f/s, %d workers\n",
		rep.Envs, rep.RatePerSecond, rep.Workers)
	fmt.Fprintf(&b, "ready rate:     %d/%d (error rate %.1f%%)\n",
		rep.ReadyCount, rep.Envs, rep.ErrorRate*100)
	fmt.Fprintf(&b, "time-to-ready p50/p95/p99/max: %.0fms / %.0fms / %.0fms / %.0fms\n",
		rep.P50, rep.P95, rep.P99, rep.Max)
	fmt.Fprintf(&b, "external accounting: created %d databases / %d roles; %d unattributed\n",
		rep.CreatedDatabases, rep.CreatedRoles, rep.UnattributedResources)
	fmt.Fprintf(&b, "duplicate resources: %d (must be 0)\n", rep.DuplicateResources)
	for _, s := range samples {
		status := fmt.Sprintf("ready in %.0fms", s.TimeToReadyMs)
		if !s.Ready {
			status = "NOT READY: " + s.Error
		}
		fmt.Fprintf(&b, "  %s  databases=%d roles=%d  %s\n",
			s.EnvID, s.DatabaseRows, s.RoleRows, status)
	}
	fmt.Fprintln(&b, "===========================")
	t.Log(b.String())
}

// writeJSONReport writes the machine-readable report and per-env samples.
func writeJSONReport(t *testing.T, path string, rep report, samples []sample) {
	t.Helper()
	doc := struct {
		Report  report   `json:"report"`
		Samples []sample `json:"samples"`
	}{rep, samples}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("marshal load report for %s: %v", path, err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write load report to %s: %v", path, err)
	}
}
