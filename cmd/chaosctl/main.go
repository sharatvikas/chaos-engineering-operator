// chaosctl — CLI for managing ChaosExperiment resources.
//
// chaosctl list                    — list all experiments with status
// chaosctl describe <name>         — show full experiment details
// chaosctl history [--namespace n] — show run history from report ConfigMaps
// chaosctl next-runs <name>        — show next N scheduled run times
// chaosctl trigger <name>          — manually trigger a pending experiment
// chaosctl abort <name>            — abort a running experiment
// chaosctl dryrun <name>           — annotate experiment to run in dry-run mode
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	namespace  string
	allNs      bool
	outputJSON bool
	numRuns    int
)

var chaosExperimentGVR = schema.GroupVersionResource{
	Group:    "chaos.sharatvikas.io",
	Version:  "v1alpha1",
	Resource: "chaosexperiments",
}

var rootCmd = &cobra.Command{
	Use:   "chaosctl",
	Short: "Manage ChaosExperiment resources in Kubernetes",
	Long: `chaosctl provides a CLI interface for inspecting, triggering, and managing
ChaosExperiment custom resources created by the chaos-engineering-operator.`,
}

func main() {
	rootCmd.PersistentFlags().StringVarP(&namespace, "namespace", "n", "default", "Target namespace")
	rootCmd.PersistentFlags().BoolVarP(&allNs, "all-namespaces", "A", false, "List across all namespaces")

	rootCmd.AddCommand(listCmd())
	rootCmd.AddCommand(describeCmd())
	rootCmd.AddCommand(historyCmd())
	rootCmd.AddCommand(nextRunsCmd())
	rootCmd.AddCommand(triggerCmd())
	rootCmd.AddCommand(abortCmd())
	rootCmd.AddCommand(dryrunCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// ── list ──────────────────────────────────────────────────────────────────────

func listCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List ChaosExperiment resources",
		RunE: func(cmd *cobra.Command, _ []string) error {
			dc, err := dynamicClient()
			if err != nil {
				return err
			}
			ctx := context.Background()
			ns := resolveNamespace()

			var list *metav1.List
			if ns == "" {
				result, err := dc.Resource(chaosExperimentGVR).Namespace("").List(ctx, metav1.ListOptions{})
				if err != nil {
					return fmt.Errorf("list experiments: %w", err)
				}
				list = &metav1.List{Items: result.Items}
			} else {
				result, err := dc.Resource(chaosExperimentGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
				if err != nil {
					return fmt.Errorf("list experiments: %w", err)
				}
				list = &metav1.List{Items: result.Items}
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "NAMESPACE\tNAME\tPHASE\tFAULT\tDURATION\tSCHEDULE\tLAST RUN\tAGE")

			for _, item := range list.Items {
				obj := item.Object

				meta := getMap(obj, "metadata")
				spec := getMap(obj, "spec")
				status := getMap(obj, "status")
				fault := getMap(spec, "fault")
				scheduleMap := getMap(spec, "schedule")

				itemNS := getString(meta, "namespace")
				name := getString(meta, "name")
				phase := getString(status, "phase")
				faultType := getString(fault, "type")
				duration := getString(spec, "duration")
				creationTimestamp := getString(meta, "creationTimestamp")

				schedule := ""
				if len(scheduleMap) > 0 {
					cronExpr := getString(scheduleMap, "cron")
					if cronExpr != "" {
						schedule = cronExpr
						if getBool(scheduleMap, "suspend") {
							schedule += " (suspended)"
						}
					}
				}

				lastRun := "<never>"
				if lrt := getString(status, "lastScheduledTime"); lrt != "" {
					if t, err := time.Parse(time.RFC3339, lrt); err == nil {
						lastRun = humanDuration(time.Since(t)) + " ago"
					}
				}

				age := "<unknown>"
				if ct, err := time.Parse(time.RFC3339, creationTimestamp); err == nil {
					age = humanDuration(time.Since(ct))
				}

				phaseColor := phaseColorCode(phase)
				fmt.Fprintf(w, "%s\t%s\t%s%s\033[0m\t%s\t%s\t%s\t%s\t%s\n",
					itemNS, name, phaseColor, phase, faultType, duration, schedule, lastRun, age)
			}
			return w.Flush()
		},
	}
	cmd.Flags().BoolVar(&outputJSON, "json", false, "Output as JSON")
	return cmd
}

// ── describe ──────────────────────────────────────────────────────────────────

func describeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe <name>",
		Short: "Show detailed information about a ChaosExperiment",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			dc, err := dynamicClient()
			if err != nil {
				return err
			}
			result, err := dc.Resource(chaosExperimentGVR).Namespace(namespace).Get(
				context.Background(), args[0], metav1.GetOptions{},
			)
			if err != nil {
				return fmt.Errorf("get experiment: %w", err)
			}

			obj := result.Object
			meta := getMap(obj, "metadata")
			spec := getMap(obj, "spec")
			status := getMap(obj, "status")
			fault := getMap(spec, "fault")
			scheduleMap := getMap(spec, "schedule")
			target := getMap(spec, "target")

			fmt.Printf("Name:       %s\n", getString(meta, "name"))
			fmt.Printf("Namespace:  %s\n", getString(meta, "namespace"))
			fmt.Printf("Created:    %s\n", getString(meta, "creationTimestamp"))
			fmt.Printf("\nSpec:\n")
			fmt.Printf("  Fault:    %s\n", getString(fault, "type"))
			fmt.Printf("  Duration: %s\n", getString(spec, "duration"))
			fmt.Printf("  Cooldown: %s\n", getString(spec, "cooldown"))
			fmt.Printf("  DryRun:   %v\n", getBool(spec, "dryRun"))
			fmt.Printf("  Target:\n")
			fmt.Printf("    Namespace:  %s\n", getString(target, "namespace"))
			fmt.Printf("    Percentage: %v%%\n", getInt(target, "percentage"))
			fmt.Printf("    Mode:       %s\n", getString(target, "mode"))

			if len(scheduleMap) > 0 {
				fmt.Printf("\nSchedule:\n")
				fmt.Printf("  Cron:      %s\n", getString(scheduleMap, "cron"))
				fmt.Printf("  Timezone:  %s\n", getString(scheduleMap, "timezone"))
				fmt.Printf("  Suspended: %v\n", getBool(scheduleMap, "suspend"))
			}

			fmt.Printf("\nStatus:\n")
			fmt.Printf("  Phase:   %s%s\033[0m\n", phaseColorCode(getString(status, "phase")), getString(status, "phase"))
			if st := getString(status, "startTime"); st != "" {
				fmt.Printf("  Started: %s\n", st)
			}
			if et := getString(status, "endTime"); et != "" {
				fmt.Printf("  Ended:   %s\n", et)
			}
			if lrt := getString(status, "lastScheduledTime"); lrt != "" {
				fmt.Printf("  Last Scheduled: %s\n", lrt)
			}
			if ar := getString(status, "abortReason"); ar != "" {
				fmt.Printf("  \033[31mAbort Reason: %s\033[0m\n", ar)
			}
			if br := getString(status, "blockedReason"); br != "" {
				fmt.Printf("  \033[33mBlocked: %s\033[0m\n", br)
			}

			if pods, ok := obj["status"].(map[string]interface{})["targetedPods"]; ok {
				if podList, ok := pods.([]interface{}); ok && len(podList) > 0 {
					fmt.Printf("\nTargeted Pods (%d):\n", len(podList))
					for _, p := range podList {
						fmt.Printf("  - %v\n", p)
					}
				}
			}

			if report := getString(status, "report"); report != "" {
				fmt.Printf("\nReport:\n%s\n", report)
			}
			if dryReport := getString(status, "dryRunReport"); dryReport != "" {
				fmt.Printf("\nDry-Run Report:\n%s\n", dryReport)
			}

			return nil
		},
	}
}

// ── history ───────────────────────────────────────────────────────────────────

func historyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "history [experiment-name]",
		Short: "Show experiment run history from audit ConfigMaps",
		RunE: func(_ *cobra.Command, args []string) error {
			cs, err := k8sClient()
			if err != nil {
				return err
			}
			ctx := context.Background()
			ns := resolveNamespace()
			if ns == "" {
				ns = metav1.NamespaceAll
			}

			labelSelector := "chaos.sharatvikas.io/report=true"
			if len(args) > 0 {
				labelSelector += fmt.Sprintf(",chaos.sharatvikas.io/experiment=%s", args[0])
			}

			cms, err := cs.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{
				LabelSelector: labelSelector,
			})
			if err != nil {
				return fmt.Errorf("list report ConfigMaps: %w", err)
			}

			if len(cms.Items) == 0 {
				fmt.Println("No experiment run history found.")
				return nil
			}

			// Sort by creation time desc
			sort.Slice(cms.Items, func(i, j int) bool {
				return cms.Items[i].CreationTimestamp.After(cms.Items[j].CreationTimestamp.Time)
			})

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "EXPERIMENT\tNAMESPACE\tFAULT\tOUTCOME\tSTARTED\tDURATION\tTARGETED PODS")

			for _, cm := range cms.Items {
				labels := cm.Labels
				expName := labels["chaos.sharatvikas.io/experiment"]
				faultType := labels["chaos.sharatvikas.io/fault-type"]
				outcome := cm.Data["outcome"]
				startedAt := cm.Data["start_time"]
				endedAt := cm.Data["end_time"]
				targetedPods := cm.Data["targeted_pods"]

				duration := ""
				if startedAt != "" && endedAt != "" {
					if st, err := time.Parse(time.RFC3339, startedAt); err == nil {
						if et, err := time.Parse(time.RFC3339, endedAt); err == nil {
							duration = humanDuration(et.Sub(st))
						}
					}
				}
				if startedAt == "" {
					startedAt = cm.CreationTimestamp.Format(time.RFC3339)
				}

				outcomeColor := "\033[32m"
				if strings.ToLower(outcome) == "aborted" || strings.ToLower(outcome) == "failed" {
					outcomeColor = "\033[31m"
				}

				fmt.Fprintf(w, "%s\t%s\t%s\t%s%s\033[0m\t%s\t%s\t%s\n",
					expName, cm.Namespace, faultType,
					outcomeColor, outcome,
					startedAt, duration, targetedPods,
				)
			}
			return w.Flush()
		},
	}
}

// ── next-runs ─────────────────────────────────────────────────────────────────

func nextRunsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "next-runs <experiment-name>",
		Short: "Show the next N scheduled run times for an experiment",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			dc, err := dynamicClient()
			if err != nil {
				return err
			}
			result, err := dc.Resource(chaosExperimentGVR).Namespace(namespace).Get(
				context.Background(), args[0], metav1.GetOptions{},
			)
			if err != nil {
				return fmt.Errorf("get experiment: %w", err)
			}

			obj := result.Object
			spec := getMap(obj, "spec")
			scheduleMap := getMap(spec, "schedule")

			if len(scheduleMap) == 0 {
				return fmt.Errorf("experiment %q has no schedule defined", args[0])
			}

			cronExpr := getString(scheduleMap, "cron")
			tz := getString(scheduleMap, "timezone")
			suspended := getBool(scheduleMap, "suspend")

			if suspended {
				fmt.Printf("⚠️  Schedule for %q is suspended.\n", args[0])
				return nil
			}

			loc := time.UTC
			if tz != "" {
				if l, err := time.LoadLocation(tz); err == nil {
					loc = l
				}
			}

			parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
			schedule, err := parser.Parse(cronExpr)
			if err != nil {
				return fmt.Errorf("parse cron %q: %w", cronExpr, err)
			}

			fmt.Printf("Next %d runs for %q (cron: %s, tz: %s):\n\n", numRuns, args[0], cronExpr, loc)

			t := time.Now().In(loc)
			for i := 0; i < numRuns; i++ {
				t = schedule.Next(t)
				if t.IsZero() {
					break
				}
				fmt.Printf("  %2d. %s  (%s from now)\n",
					i+1,
					t.Format("2006-01-02 15:04:05 MST"),
					humanDuration(time.Until(t)),
				)
			}
			return nil
		},
	}
	cmd.Flags().IntVarP(&numRuns, "count", "c", 10, "Number of upcoming runs to show")
	return cmd
}

// ── trigger ───────────────────────────────────────────────────────────────────

func triggerCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "trigger <experiment-name>",
		Short: "Manually trigger a ChaosExperiment by setting its phase to Pending",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			dc, err := dynamicClient()
			if err != nil {
				return err
			}
			patch := []byte(`{"status":{"phase":"Pending","message":"Manually triggered via chaosctl"}}`)
			_, err = dc.Resource(chaosExperimentGVR).Namespace(namespace).Patch(
				context.Background(), args[0],
				types.MergePatchType, patch, metav1.PatchOptions{}, "status",
			)
			if err != nil {
				return fmt.Errorf("trigger experiment: %w", err)
			}
			fmt.Printf("✓ Experiment %q set to Pending — operator will pick it up shortly.\n", args[0])
			return nil
		},
	}
}

// ── abort ─────────────────────────────────────────────────────────────────────

func abortCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "abort <experiment-name>",
		Short: "Abort a running ChaosExperiment",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			dc, err := dynamicClient()
			if err != nil {
				return err
			}

			reason := fmt.Sprintf("Manually aborted via chaosctl at %s", time.Now().Format(time.RFC3339))
			patchData, _ := json.Marshal(map[string]interface{}{
				"status": map[string]interface{}{
					"phase":       "Aborted",
					"abortReason": reason,
				},
			})
			_, err = dc.Resource(chaosExperimentGVR).Namespace(namespace).Patch(
				context.Background(), args[0],
				types.MergePatchType, patchData, metav1.PatchOptions{}, "status",
			)
			if err != nil {
				return fmt.Errorf("abort experiment: %w", err)
			}
			fmt.Printf("✓ Experiment %q marked as Aborted.\n", args[0])
			return nil
		},
	}
}

// ── dryrun ────────────────────────────────────────────────────────────────────

func dryrunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dryrun <experiment-name>",
		Short: "Enable dry-run mode on an experiment and set it to Pending",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			dc, err := dynamicClient()
			if err != nil {
				return err
			}

			// Enable dryRun on spec
			specPatch, _ := json.Marshal(map[string]interface{}{
				"spec": map[string]interface{}{"dryRun": true},
			})
			_, err = dc.Resource(chaosExperimentGVR).Namespace(namespace).Patch(
				context.Background(), args[0],
				types.MergePatchType, specPatch, metav1.PatchOptions{},
			)
			if err != nil {
				return fmt.Errorf("patch spec.dryRun: %w", err)
			}

			// Set status to Pending to trigger reconcile
			statusPatch, _ := json.Marshal(map[string]interface{}{
				"status": map[string]interface{}{
					"phase":   "Pending",
					"message": "Dry-run initiated via chaosctl",
				},
			})
			_, err = dc.Resource(chaosExperimentGVR).Namespace(namespace).Patch(
				context.Background(), args[0],
				types.MergePatchType, statusPatch, metav1.PatchOptions{}, "status",
			)
			if err != nil {
				return fmt.Errorf("patch status: %w", err)
			}

			fmt.Printf("✓ Experiment %q set to dry-run mode and Pending.\n", args[0])
			fmt.Printf("  The operator will simulate the experiment and write results to status.dryRunReport.\n")
			fmt.Printf("  Check progress with: chaosctl describe %s -n %s\n", args[0], namespace)
			return nil
		},
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func dynamicClient() (dynamic.Interface, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules, &clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("build kubeconfig: %w", err)
	}
	return dynamic.NewForConfig(cfg)
}

func k8sClient() (*kubernetes.Clientset, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules, &clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("build kubeconfig: %w", err)
	}
	return kubernetes.NewForConfig(cfg)
}

func resolveNamespace() string {
	if allNs {
		return ""
	}
	return namespace
}

func getMap(obj map[string]interface{}, key string) map[string]interface{} {
	if v, ok := obj[key]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			return m
		}
	}
	return map[string]interface{}{}
}

func getString(obj map[string]interface{}, key string) string {
	if v, ok := obj[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getBool(obj map[string]interface{}, key string) bool {
	if v, ok := obj[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func getInt(obj map[string]interface{}, key string) int {
	if v, ok := obj[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		case float64:
			return int(n)
		}
	}
	return 0
}

func phaseColorCode(phase string) string {
	switch phase {
	case "Running":
		return "\033[34m" // blue
	case "Completed":
		return "\033[32m" // green
	case "Aborted", "Failed":
		return "\033[31m" // red
	case "Pending", "PreflightCheck":
		return "\033[33m" // yellow
	default:
		return ""
	}
}

func humanDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%dh", int(d.Hours()/24), int(d.Hours())%24)
	}
}

// keep runtime import used by dynamic client
var _ = runtime.DefaultUnstructuredConverter
