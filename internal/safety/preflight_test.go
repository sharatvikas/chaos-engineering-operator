package safety

import "testing"

func TestCheckNamespace(t *testing.T) {
	pf := PreflightChecker{NamespaceDenylist: DefaultNamespaceDenylist()}

	tests := []struct {
		name      string
		namespace string
		wantOK    bool
	}{
		{"allowed app namespace", "staging", true},
		{"allowed production", "production", true},
		{"denied kube-system", "kube-system", false},
		{"denied case-insensitive", "Kube-System", false},
		{"denied chaos-system", "chaos-system", false},
		{"empty namespace", "", false},
		{"whitespace namespace", "   ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, reason := pf.CheckNamespace(tt.namespace)
			if ok != tt.wantOK {
				t.Errorf("CheckNamespace(%q) = %v, want %v (reason: %s)", tt.namespace, ok, tt.wantOK, reason)
			}
			if !ok && reason == "" {
				t.Error("expected a non-empty reason when namespace is blocked")
			}
		})
	}
}

func TestCheckMinHealthyReplicas(t *testing.T) {
	pf := PreflightChecker{}

	tests := []struct {
		name       string
		total      int
		affected   int
		minHealthy int
		wantOK     bool
	}{
		{"guard disabled", 3, 3, 0, true},
		{"leaves enough", 5, 2, 2, true},
		{"exactly enough", 4, 2, 2, true},
		{"one short", 3, 2, 2, false},
		{"kills everything", 3, 3, 1, false},
		{"negative guard disabled", 1, 1, -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, reason := pf.CheckMinHealthyReplicas(tt.total, tt.affected, tt.minHealthy)
			if ok != tt.wantOK {
				t.Errorf("CheckMinHealthyReplicas(%d,%d,%d) = %v, want %v",
					tt.total, tt.affected, tt.minHealthy, ok, tt.wantOK)
			}
			if !ok && reason == "" {
				t.Error("expected a non-empty reason when guard blocks")
			}
		})
	}
}
