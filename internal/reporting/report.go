// Package reporting generates structured experiment result reports for
// ChaosExperiment runs. Reports are written as JSON + optionally posted
// to Slack and stored in a ConfigMap for historical audit.
package reporting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// ExperimentReport is the full structured output of a completed experiment.
type ExperimentReport struct {
	ExperimentName string    `json:"experiment_name"`
	Namespace      string    `json:"namespace"`
	FaultType      string    `json:"fault_type"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at"`
	DurationSec    float64   `json:"duration_seconds"`

	Outcome     string `json:"outcome"` // Completed | Aborted | Failed
	AbortReason string `json:"abort_reason,omitempty"`

	TargetCount    int      `json:"target_count"`
	TargetNames    []string `json:"target_names"`
	BlastRadiusPct int      `json:"blast_radius_pct"`

	SafetyGates []GateSnapshot `json:"safety_gates"`

	// SLO impact observed during the experiment
	SLOImpact SLOSnapshot `json:"slo_impact"`

	// Free-form findings from manual/automated analysis
	Observations []string `json:"observations,omitempty"`
}

// GateSnapshot records the state of a safety gate at a point in time.
type GateSnapshot struct {
	Name      string    `json:"name"`
	Query     string    `json:"query"`
	Threshold string    `json:"threshold"`
	Observed  string    `json:"observed"`
	Passed    bool      `json:"passed"`
	CheckedAt time.Time `json:"checked_at"`
}

// SLOSnapshot captures key SLO metrics sampled during experiment window.
type SLOSnapshot struct {
	ErrorRatePct      float64 `json:"error_rate_pct"`
	AvailabilityPct   float64 `json:"availability_pct"`
	P99LatencyMs      float64 `json:"p99_latency_ms,omitempty"`
	BurnRateMultiple  float64 `json:"burn_rate_multiple,omitempty"`
	ErrorBudgetUsed   float64 `json:"error_budget_used_pct,omitempty"`
}

// Reporter produces and distributes ExperimentReports.
type Reporter struct {
	client     client.Client
	httpClient *http.Client
}

// New creates a Reporter backed by the given controller-runtime client.
func New(c client.Client) *Reporter {
	return &Reporter{
		client:     c,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// BuildReport assembles a report from a completed ChaosExperiment.
func BuildReport(exp *chaosv1alpha1.ChaosExperiment, gates []GateSnapshot, slo SLOSnapshot) *ExperimentReport {
	startedAt := exp.Status.StartedAt.Time
	completedAt := time.Now()
	if !exp.Status.CompletedAt.IsZero() {
		completedAt = exp.Status.CompletedAt.Time
	}

	return &ExperimentReport{
		ExperimentName: exp.Name,
		Namespace:      exp.Namespace,
		FaultType:      string(exp.Spec.FaultType),
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		DurationSec:    completedAt.Sub(startedAt).Seconds(),
		Outcome:        string(exp.Status.Phase),
		AbortReason:    exp.Status.AbortReason,
		TargetCount:    len(exp.Status.TargetPods),
		TargetNames:    exp.Status.TargetPods,
		BlastRadiusPct: exp.Spec.BlastRadius.Percentage,
		SafetyGates:    gates,
		SLOImpact:      slo,
	}
}

// SaveToConfigMap persists the report as a ConfigMap in the experiment's namespace.
// This creates an auditable history of all experiments across controller restarts.
func (r *Reporter) SaveToConfigMap(ctx context.Context, report *ExperimentReport) error {
	log := log.FromContext(ctx)

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}

	cmName := fmt.Sprintf("chaos-report-%s-%d",
		sanitizeName(report.ExperimentName),
		report.StartedAt.Unix(),
	)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: report.Namespace,
			Labels: map[string]string{
				"chaos.k8s-ai-ops.io/type":       "experiment-report",
				"chaos.k8s-ai-ops.io/experiment": report.ExperimentName,
				"chaos.k8s-ai-ops.io/outcome":    report.Outcome,
				"chaos.k8s-ai-ops.io/fault-type": report.FaultType,
			},
			Annotations: map[string]string{
				"chaos.k8s-ai-ops.io/started-at":   report.StartedAt.Format(time.RFC3339),
				"chaos.k8s-ai-ops.io/completed-at": report.CompletedAt.Format(time.RFC3339),
			},
		},
		Data: map[string]string{
			"report.json": string(data),
		},
	}

	if err := r.client.Create(ctx, cm); err != nil {
		return fmt.Errorf("create report configmap: %w", err)
	}

	log.Info("experiment report saved", "configmap", cmName, "outcome", report.Outcome)
	return nil
}

// PostToSlack sends a formatted experiment summary to a Slack webhook.
// The webhook URL is read from the CHAOS_SLACK_WEBHOOK env var.
func (r *Reporter) PostToSlack(ctx context.Context, report *ExperimentReport) error {
	webhookURL := os.Getenv("CHAOS_SLACK_WEBHOOK")
	if webhookURL == "" {
		return nil // Slack not configured — silently skip
	}

	color := "#36a64f" // green = success
	if report.Outcome == "Aborted" {
		color = "#ff9900" // orange = aborted by safety gate
	} else if report.Outcome == "Failed" {
		color = "#ff0000" // red = unexpected failure
	}

	outcomeEmoji := map[string]string{
		"Completed": ":white_check_mark:",
		"Aborted":   ":warning:",
		"Failed":    ":x:",
	}

	gateLines := ""
	for _, g := range report.SafetyGates {
		icon := ":white_check_mark:"
		if !g.Passed {
			icon = ":x:"
		}
		gateLines += fmt.Sprintf("  %s `%s` — observed `%s` (threshold `%s`)\n", icon, g.Name, g.Observed, g.Threshold)
	}

	payload := map[string]interface{}{
		"attachments": []map[string]interface{}{
			{
				"color": color,
				"blocks": []map[string]interface{}{
					{
						"type": "header",
						"text": map[string]interface{}{
							"type": "plain_text",
							"text": fmt.Sprintf("%s Chaos Experiment: %s",
								outcomeEmoji[report.Outcome], report.ExperimentName),
						},
					},
					{
						"type": "section",
						"fields": []map[string]interface{}{
							{"type": "mrkdwn", "text": fmt.Sprintf("*Fault Type*\n%s", report.FaultType)},
							{"type": "mrkdwn", "text": fmt.Sprintf("*Outcome*\n%s", report.Outcome)},
							{"type": "mrkdwn", "text": fmt.Sprintf("*Duration*\n%.0fs", report.DurationSec)},
							{"type": "mrkdwn", "text": fmt.Sprintf("*Blast Radius*\n%d%% (%d targets)", report.BlastRadiusPct, report.TargetCount)},
							{"type": "mrkdwn", "text": fmt.Sprintf("*Error Rate*\n%.2f%%", report.SLOImpact.ErrorRatePct)},
							{"type": "mrkdwn", "text": fmt.Sprintf("*Availability*\n%.3f%%", report.SLOImpact.AvailabilityPct)},
						},
					},
					{
						"type": "section",
						"text": map[string]interface{}{
							"type": "mrkdwn",
							"text": fmt.Sprintf("*Safety Gates*\n%s", gateLines),
						},
					},
				},
			},
		},
	}

	if report.AbortReason != "" {
		abortBlock := map[string]interface{}{
			"type": "section",
			"text": map[string]interface{}{
				"type": "mrkdwn",
				"text": fmt.Sprintf(":rotating_light: *Abort Reason*: %s", report.AbortReason),
			},
		}
		blocks := payload["attachments"].([]map[string]interface{})[0]["blocks"].([]map[string]interface{})
		payload["attachments"].([]map[string]interface{})[0]["blocks"] = append(blocks, abortBlock)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post to slack: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("slack returned %d: %s", resp.StatusCode, string(b))
	}

	return nil
}

// ListReports returns all experiment reports from a namespace.
func (r *Reporter) ListReports(ctx context.Context, namespace string, experimentName string) ([]*ExperimentReport, error) {
	cmList := &corev1.ConfigMapList{}
	opts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels{"chaos.k8s-ai-ops.io/type": "experiment-report"},
	}
	if experimentName != "" {
		opts = append(opts, client.MatchingLabels{
			"chaos.k8s-ai-ops.io/experiment": experimentName,
		})
	}

	if err := r.client.List(ctx, cmList, opts...); err != nil {
		return nil, fmt.Errorf("list report configmaps: %w", err)
	}

	var reports []*ExperimentReport
	for _, cm := range cmList.Items {
		jsonStr, ok := cm.Data["report.json"]
		if !ok {
			continue
		}
		var rep ExperimentReport
		if err := json.Unmarshal([]byte(jsonStr), &rep); err != nil {
			continue
		}
		reports = append(reports, &rep)
	}

	return reports, nil
}

func sanitizeName(name string) string {
	out := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			out = append(out, c)
		} else if c >= 'A' && c <= 'Z' {
			out = append(out, c+32) // to lower
		} else {
			out = append(out, '-')
		}
	}
	return string(out)
}
