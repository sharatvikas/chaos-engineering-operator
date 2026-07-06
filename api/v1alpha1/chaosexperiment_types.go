package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FaultType defines the type of chaos fault to inject
// +kubebuilder:validation:Enum=PodKill;ContainerKill;NetworkDelay;NetworkPartition;CPUStress;MemoryStress;DNSFailure;DiskFill;NodeDrain
type FaultType string

const (
	FaultPodKill          FaultType = "PodKill"
	FaultContainerKill    FaultType = "ContainerKill"
	FaultNetworkDelay     FaultType = "NetworkDelay"
	FaultNetworkPartition FaultType = "NetworkPartition"
	FaultCPUStress        FaultType = "CPUStress"
	FaultMemoryStress     FaultType = "MemoryStress"
	FaultDNSFailure       FaultType = "DNSFailure"
	FaultDiskFill         FaultType = "DiskFill"
	FaultNodeDrain        FaultType = "NodeDrain"
)

// ExperimentPhase represents the current phase of the experiment
// +kubebuilder:validation:Enum=Pending;PreflightCheck;Running;Completed;Aborted;Failed
type ExperimentPhase string

const (
	PhasePending        ExperimentPhase = "Pending"
	PhasePreflightCheck ExperimentPhase = "PreflightCheck"
	PhaseRunning        ExperimentPhase = "Running"
	PhaseCompleted      ExperimentPhase = "Completed"
	PhaseAborted        ExperimentPhase = "Aborted"
	PhaseFailed         ExperimentPhase = "Failed"
)

// ChaosExperimentSpec defines the desired state of ChaosExperiment
type ChaosExperimentSpec struct {
	// Target defines which pods/nodes to apply the fault to
	Target TargetSpec `json:"target"`

	// Fault defines what kind of failure to inject
	Fault FaultSpec `json:"fault"`

	// Duration is how long to run the experiment (e.g. "10m", "1h")
	// +kubebuilder:validation:Pattern=`^[0-9]+(s|m|h)$`
	Duration string `json:"duration"`

	// Cooldown is how long to wait after experiment before marking complete
	// +optional
	// +kubebuilder:default="2m"
	Cooldown string `json:"cooldown,omitempty"`

	// SafetyGates define conditions that will abort the experiment
	// +optional
	SafetyGates []SafetyGate `json:"safetyGates,omitempty"`

	// Schedule defines when to run this experiment (cron syntax)
	// +optional
	Schedule *ScheduleSpec `json:"schedule,omitempty"`

	// Notifications configures where to send experiment events
	// +optional
	Notifications *NotificationSpec `json:"notifications,omitempty"`

	// DryRun if true will plan the experiment without executing it
	// +optional
	// +kubebuilder:default=false
	DryRun bool `json:"dryRun,omitempty"`
}

// TargetSpec defines which resources to target
type TargetSpec struct {
	// Selector specifies which pods to target
	Selector metav1.LabelSelector `json:"selector"`

	// Namespace of the target pods
	// +kubebuilder:default="default"
	Namespace string `json:"namespace"`

	// Percentage of matching pods to affect (1-100)
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=50
	Percentage int `json:"percentage,omitempty"`

	// Mode controls how targets are selected: Random, All, Fixed
	// +kubebuilder:validation:Enum=Random;All;Fixed
	// +kubebuilder:default=Random
	Mode string `json:"mode,omitempty"`

	// MinHealthyReplicas is a preflight safety guard: the experiment is
	// blocked if injecting the fault would leave fewer than this many
	// running, unaffected pods behind. Set to 0 (default) to disable.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinHealthyReplicas int `json:"minHealthyReplicas,omitempty"`
}

// FaultSpec defines the type and parameters of the fault
type FaultSpec struct {
	// Type is the kind of fault to inject
	Type FaultType `json:"type"`

	// PodKill parameters (used when Type=PodKill)
	// +optional
	PodKill *PodKillParams `json:"podKill,omitempty"`

	// ContainerKill parameters (used when Type=ContainerKill)
	// +optional
	ContainerKill *ContainerKillParams `json:"containerKill,omitempty"`

	// NetworkDelay parameters (used when Type=NetworkDelay)
	// +optional
	NetworkDelay *NetworkDelayParams `json:"networkDelay,omitempty"`

	// NetworkPartition parameters (used when Type=NetworkPartition)
	// +optional
	NetworkPartition *NetworkPartitionParams `json:"networkPartition,omitempty"`

	// CPUStress parameters (used when Type=CPUStress)
	// +optional
	CPUStress *CPUStressParams `json:"cpuStress,omitempty"`

	// MemoryStress parameters (used when Type=MemoryStress)
	// +optional
	MemoryStress *MemoryStressParams `json:"memoryStress,omitempty"`
}

// PodKillParams configures pod deletion
type PodKillParams struct {
	// GracePeriodSeconds is the termination grace period for the killed pod.
	// 0 (default) is a hard kill that simulates an abrupt node/process failure.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=0
	GracePeriodSeconds *int64 `json:"gracePeriodSeconds,omitempty"`
}

// ContainerKillParams configures killing a single container inside a pod
// (rather than deleting the whole pod). The kubelet restarts the container,
// which exercises restart handling, readiness gating, and warm-cache loss.
type ContainerKillParams struct {
	// ContainerName is the container to kill. Empty selects the first
	// container in the pod spec.
	// +optional
	ContainerName string `json:"containerName,omitempty"`

	// Signal is the signal delivered to the container's PID 1.
	// +optional
	// +kubebuilder:validation:Enum=SIGKILL;SIGTERM
	// +kubebuilder:default=SIGKILL
	Signal string `json:"signal,omitempty"`
}

// NetworkPartitionParams configures a full network partition, dropping all
// traffic (not just delaying it) between the target pods and their peers.
type NetworkPartitionParams struct {
	// Direction controls which traffic is dropped relative to the target pod.
	// +optional
	// +kubebuilder:validation:Enum=Egress;Ingress;Both
	// +kubebuilder:default=Both
	Direction string `json:"direction,omitempty"`

	// PeerCIDRs optionally restricts the partition to traffic to/from these
	// CIDR blocks (e.g. the database subnet). When empty, ALL traffic is
	// dropped, fully isolating the target pods.
	// +optional
	PeerCIDRs []string `json:"peerCIDRs,omitempty"`
}

// NetworkDelayParams configures network latency injection
type NetworkDelayParams struct {
	// LatencyMs is the delay to add in milliseconds
	// +kubebuilder:validation:Minimum=1
	LatencyMs int `json:"latencyMs"`

	// JitterMs is the jitter to add to the latency
	// +optional
	JitterMs int `json:"jitterMs,omitempty"`

	// CorrelationPct is the correlation percentage for jitter (0-100)
	// +optional
	CorrelationPct int `json:"correlationPct,omitempty"`
}

// CPUStressParams configures CPU stress
type CPUStressParams struct {
	// Cores is the number of CPU cores to stress
	// +kubebuilder:validation:Minimum=1
	Cores int `json:"cores"`

	// Load is the CPU load percentage to apply (1-100)
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	Load int `json:"load"`
}

// MemoryStressParams configures memory pressure
type MemoryStressParams struct {
	// MemoryMB is the amount of memory to consume in MB
	// +kubebuilder:validation:Minimum=64
	MemoryMB int `json:"memoryMB"`

	// FillPercentage fills this % of available memory instead of a fixed amount
	// +optional
	// +kubebuilder:validation:Maximum=95
	FillPercentage int `json:"fillPercentage,omitempty"`
}

// SafetyGate defines a condition that will abort the experiment
type SafetyGate struct {
	// Type of gate: SLOBurnRate, ErrorRate, Availability
	// +kubebuilder:validation:Enum=SLOBurnRate;ErrorRate;Availability
	Type string `json:"type"`

	// Service name to check (for SLO-based gates)
	// +optional
	Service string `json:"service,omitempty"`

	// Threshold value that triggers abort (interpretation depends on Type)
	// e.g. "5x" for burn rate, "10" for error rate percentage
	Threshold string `json:"threshold"`

	// PrometheusQuery is a custom PromQL expression for custom gates
	// +optional
	PrometheusQuery string `json:"prometheusQuery,omitempty"`
}

// ScheduleSpec defines when to run the experiment
type ScheduleSpec struct {
	// Cron expression for scheduling
	Cron string `json:"cron"`

	// Timezone for the cron schedule
	// +kubebuilder:default="UTC"
	Timezone string `json:"timezone,omitempty"`

	// Suspend temporarily disables the schedule
	// +optional
	Suspend bool `json:"suspend,omitempty"`
}

// NotificationSpec configures experiment notifications
type NotificationSpec struct {
	// Slack webhook URL or channel name
	// +optional
	Slack string `json:"slack,omitempty"`

	// PagerDuty will create an incident if set to true
	// +optional
	PagerDuty bool `json:"pagerduty,omitempty"`
}

// ChaosExperimentStatus defines the observed state of ChaosExperiment
type ChaosExperimentStatus struct {
	// Phase is the current lifecycle phase
	Phase ExperimentPhase `json:"phase,omitempty"`

	// StartTime is when the experiment started
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// EndTime is when the experiment completed or was aborted
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`

	// TargetedPods is the list of pods that were/are being affected
	TargetedPods []string `json:"targetedPods,omitempty"`

	// AbortReason explains why the experiment was aborted (if applicable)
	// +optional
	AbortReason string `json:"abortReason,omitempty"`

	// Conditions represents the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Report contains the experiment summary report
	// +optional
	Report string `json:"report,omitempty"`

	// LastScheduledTime is the last time the schedule triggered a run
	// +optional
	LastScheduledTime *metav1.Time `json:"lastScheduledTime,omitempty"`

	// BlockedReason describes why a dry-run was blocked by safety gates
	// +optional
	BlockedReason string `json:"blockedReason,omitempty"`

	// DryRunReport contains the dry-run simulation summary
	// +optional
	DryRunReport string `json:"dryRunReport,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Fault",type=string,JSONPath=`.spec.fault.type`
// +kubebuilder:printcolumn:name="Duration",type=string,JSONPath=`.spec.duration`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ChaosExperiment is the Schema for the chaosexperiments API
type ChaosExperiment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ChaosExperimentSpec   `json:"spec,omitempty"`
	Status ChaosExperimentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ChaosExperimentList contains a list of ChaosExperiment
type ChaosExperimentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ChaosExperiment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ChaosExperiment{}, &ChaosExperimentList{})
}
