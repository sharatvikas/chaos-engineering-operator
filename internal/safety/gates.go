package safety

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// GateChecker evaluates safety gates against live Prometheus metrics
type GateChecker struct {
	prometheusURL string
	client        *http.Client
}

// NewGateChecker creates a gate checker using the given Prometheus URL
func NewGateChecker(prometheusURL string) *GateChecker {
	return &GateChecker{
		prometheusURL: strings.TrimRight(prometheusURL, "/"),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Check evaluates a single safety gate.
// Returns (safe, reason, error). If safe=false, the experiment should abort.
func (g *GateChecker) Check(
	ctx context.Context,
	gate chaosv1alpha1.SafetyGate,
) (safe bool, reason string, err error) {
	switch gate.Type {
	case "SLOBurnRate":
		return g.checkSLOBurnRate(ctx, gate)
	case "ErrorRate":
		return g.checkErrorRate(ctx, gate)
	case "Availability":
		return g.checkAvailability(ctx, gate)
	default:
		if gate.PrometheusQuery != "" {
			return g.checkCustomQuery(ctx, gate)
		}
		return true, "", fmt.Errorf("unknown gate type: %s", gate.Type)
	}
}

func (g *GateChecker) checkSLOBurnRate(
	ctx context.Context, gate chaosv1alpha1.SafetyGate,
) (bool, string, error) {
	// Parse threshold like "5x" → 5.0
	thresholdStr := strings.TrimSuffix(gate.Threshold, "x")
	threshold, err := strconv.ParseFloat(thresholdStr, 64)
	if err != nil {
		return true, "", fmt.Errorf("invalid burn rate threshold %q: %w", gate.Threshold, err)
	}

	// SLO burn rate = (error_rate / (1 - slo_target))
	// For 99.9% SLO: burn_rate = error_rate / 0.001
	query := fmt.Sprintf(
		`job:request_error_rate:ratio_rate1h{job="%s"} / 0.001`,
		gate.Service,
	)

	value, err := g.instantQuery(ctx, query)
	if err != nil {
		// If Prometheus is unreachable, fail safe (don't abort experiment)
		return true, "", fmt.Errorf("prometheus query failed: %w", err)
	}

	if value >= threshold {
		return false, fmt.Sprintf(
			"SLO burn rate %.1fx exceeds threshold %.1fx for service %s",
			value, threshold, gate.Service,
		), nil
	}

	return true, fmt.Sprintf("SLO burn rate %.1fx is within threshold %.1fx", value, threshold), nil
}

func (g *GateChecker) checkErrorRate(
	ctx context.Context, gate chaosv1alpha1.SafetyGate,
) (bool, string, error) {
	threshold, err := strconv.ParseFloat(gate.Threshold, 64)
	if err != nil {
		return true, "", fmt.Errorf("invalid error rate threshold %q: %w", gate.Threshold, err)
	}

	var query string
	if gate.Service != "" {
		query = fmt.Sprintf(
			`sum(rate(http_requests_total{status=~"5..",job="%s"}[5m])) / sum(rate(http_requests_total{job="%s"}[5m])) * 100`,
			gate.Service, gate.Service,
		)
	} else if gate.PrometheusQuery != "" {
		query = gate.PrometheusQuery
	} else {
		return true, "", fmt.Errorf("ErrorRate gate requires service or prometheusQuery")
	}

	value, err := g.instantQuery(ctx, query)
	if err != nil {
		return true, "", err
	}

	if value >= threshold {
		return false, fmt.Sprintf(
			"Error rate %.2f%% exceeds threshold %.2f%% for service %s",
			value, threshold, gate.Service,
		), nil
	}

	return true, fmt.Sprintf("Error rate %.2f%% within threshold %.2f%%", value, threshold), nil
}

func (g *GateChecker) checkAvailability(
	ctx context.Context, gate chaosv1alpha1.SafetyGate,
) (bool, string, error) {
	// Availability = 1 - error_rate
	// Threshold like "99.5" means availability must be >= 99.5%
	threshold, err := strconv.ParseFloat(gate.Threshold, 64)
	if err != nil {
		return true, "", fmt.Errorf("invalid availability threshold %q", gate.Threshold)
	}

	query := fmt.Sprintf(
		`(1 - sum(rate(http_requests_total{status=~"5..",job="%s"}[5m])) / sum(rate(http_requests_total{job="%s"}[5m]))) * 100`,
		gate.Service, gate.Service,
	)

	value, err := g.instantQuery(ctx, query)
	if err != nil {
		return true, "", err
	}

	if value < threshold {
		return false, fmt.Sprintf(
			"Availability %.2f%% below threshold %.2f%% for service %s",
			value, threshold, gate.Service,
		), nil
	}

	return true, fmt.Sprintf("Availability %.2f%% meets threshold %.2f%%", value, threshold), nil
}

func (g *GateChecker) checkCustomQuery(
	ctx context.Context, gate chaosv1alpha1.SafetyGate,
) (bool, string, error) {
	threshold, err := strconv.ParseFloat(gate.Threshold, 64)
	if err != nil {
		return true, "", fmt.Errorf("invalid threshold %q for custom query", gate.Threshold)
	}

	value, err := g.instantQuery(ctx, gate.PrometheusQuery)
	if err != nil {
		return true, "", err
	}

	if value >= threshold {
		return false, fmt.Sprintf("Custom gate: value %.4f exceeds threshold %.4f", value, threshold), nil
	}

	return true, fmt.Sprintf("Custom gate: value %.4f within threshold %.4f", value, threshold), nil
}

// instantQuery runs a Prometheus instant query and returns the first scalar value
func (g *GateChecker) instantQuery(ctx context.Context, query string) (float64, error) {
	endpoint := fmt.Sprintf("%s/api/v1/query", g.prometheusURL)
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return 0, err
	}

	q := url.Values{}
	q.Set("query", query)
	req.URL.RawQuery = q.Encode()

	resp, err := g.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("prometheus request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus returned status %d", resp.StatusCode)
	}

	var result struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]interface{}    `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode prometheus response: %w", err)
	}

	if result.Status != "success" || len(result.Data.Result) == 0 {
		return 0, nil // No data = metric doesn't exist, treat as safe
	}

	valueStr, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("unexpected value type in prometheus response")
	}

	return strconv.ParseFloat(valueStr, 64)
}
