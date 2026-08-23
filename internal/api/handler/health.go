package handler

import (
	"net/http"
	"time"

	"github.com/anjani-kr-singh-ai/taskforge/internal/database"
	"github.com/anjani-kr-singh-ai/taskforge/internal/httpx"
	"github.com/anjani-kr-singh-ai/taskforge/internal/observability/logging"
)

// readinessTimeout bounds the dependency check.
//
// Short on purpose: an orchestrator polls readiness every few seconds, so a
// check that can hang for 30s turns a slow database into a pile of stuck
// probes.
const readinessTimeout = 2 * time.Second

// Health serves the liveness and readiness endpoints.
//
// It depends on database.Pinger — a one-method interface — rather than on
// *pgxpool.Pool, so the readiness test needs a three-line fake instead of a
// database.
type Health struct {
	db database.Pinger
}

// NewHealth builds the handler.
func NewHealth(db database.Pinger) *Health {
	return &Health{db: db}
}

// Live handles GET /api/v1/health.
//
// LIVENESS: "is this process alive and able to serve?" It deliberately checks
// nothing external. If liveness checked the database, a database outage would
// make every orchestrator kill every API replica — turning a recoverable
// dependency failure into a total outage with no capacity left to recover into.
func (h *Health) Live(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

// Ready handles GET /api/v1/ready.
//
// READINESS: "should this instance receive traffic right now?" Here checking
// the database is correct: an instance that cannot reach PostgreSQL can serve
// nothing useful and should be pulled from the load balancer — without being
// restarted, so it can rejoin the moment the dependency recovers.
func (h *Health) Ready(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if err := database.Check(ctx, h.db, readinessTimeout); err != nil {
		logging.FromContext(ctx).WarnContext(ctx, "readiness check failed", "error", err)

		httpx.WriteJSON(w, http.StatusServiceUnavailable, healthResponse{
			Status: "unavailable",
			// The check name, never the error text: a connection error can
			// contain the host, port and user.
			Checks: map[string]string{"database": "unreachable"},
		})
		return
	}

	httpx.WriteJSON(w, http.StatusOK, healthResponse{
		Status: "ok",
		Checks: map[string]string{"database": "ok"},
	})
}
