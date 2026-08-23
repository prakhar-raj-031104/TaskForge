package handler

import (
	"net/http"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
	"github.com/anjani-kr-singh-ai/taskforge/internal/job"
)

// Workers serves the fleet inventory endpoint.
type Workers struct {
	registry job.WorkerRegistry
	// staleAfter is how long without a heartbeat before a worker is presumed
	// dead. It is computed by the caller from the heartbeat interval rather
	// than hard-coded, so changing the interval cannot silently make every
	// worker look dead.
	staleAfter time.Duration
}

// NewWorkers builds the handler.
func NewWorkers(registry job.WorkerRegistry, staleAfter time.Duration) *Workers {
	return &Workers{registry: registry, staleAfter: staleAfter}
}

// workerResponse is the wire shape of a worker.
type workerResponse struct {
	ID          string `json:"id"`
	Hostname    string `json:"hostname"`
	PID         int    `json:"pid"`
	Status      string `json:"status"`
	Concurrency int    `json:"concurrency"`
	ActiveJobs  int    `json:"active_jobs"`

	// Alive is DERIVED from heartbeat freshness, never stored. A process that
	// is killed has no chance to write "dead", so any stored flag would be
	// wrong exactly when it mattered.
	Alive bool `json:"alive"`
	// SecondsSinceHeartbeat is the raw number behind Alive, so an operator can
	// see "9 seconds ago" rather than only a boolean.
	SecondsSinceHeartbeat float64 `json:"seconds_since_heartbeat"`

	StartedAt     time.Time `json:"started_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

type listWorkersResponse struct {
	Workers []workerResponse `json:"workers"`
	Count   int              `json:"count"`
	// Summary is what an operator actually looks at first.
	Summary workerSummary `json:"summary"`
}

type workerSummary struct {
	Alive int `json:"alive"`
	Dead  int `json:"dead"`
	// TotalCapacity is the sum of concurrency over LIVE workers only: the
	// number of jobs the fleet can run simultaneously right now.
	TotalCapacity int `json:"total_capacity"`
	ActiveJobs    int `json:"active_jobs"`
}

// List handles GET /api/v1/workers.
func (h *Workers) List(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	workers, err := h.registry.ListWorkers(ctx)
	if err != nil {
		writeServiceError(ctx, w, err)
		return
	}

	now := time.Now()
	items := make([]workerResponse, 0, len(workers))
	var summary workerSummary

	for _, wk := range workers {
		alive := wk.Alive(now, h.staleAfter)
		if alive {
			summary.Alive++
			summary.TotalCapacity += wk.Concurrency
			summary.ActiveJobs += wk.ActiveJobs
		} else {
			summary.Dead++
		}

		items = append(items, workerResponse{
			ID:                    wk.ID,
			Hostname:              wk.Hostname,
			PID:                   wk.PID,
			Status:                wk.Status.String(),
			Concurrency:           wk.Concurrency,
			ActiveJobs:            wk.ActiveJobs,
			Alive:                 alive,
			SecondsSinceHeartbeat: now.Sub(wk.LastHeartbeat).Seconds(),
			StartedAt:             wk.StartedAt.UTC(),
			LastHeartbeat:         wk.LastHeartbeat.UTC(),
		})
	}

	httpx.WriteJSON(w, http.StatusOK, listWorkersResponse{
		Workers: items,
		Count:   len(items),
		Summary: summary,
	})
}
