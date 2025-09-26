/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	kbatch "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	batchv1 "tutorial.kubebuilder.io/project/api/v1"
	"tutorial.kubebuilder.io/project/internal/controller/utils"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock  clock.Clock
}

// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.1/pkg/reconcile
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// TODO(user): your logic here
	fmt.Println("Reconciling CronJob")
	fmt.Printf("Name: %s\n", req.Name)
	fmt.Printf("Namespace: %s\n", req.Namespace)

	// log.Info("Reconciling CronJob")
	// log.Info("Name: ", "value", req.Name)
	// log.Info("Namespace: ", "value", req.Namespace)

	// 	The basic logic of our CronJob controller is this:

	// Load the named CronJob
	fmt.Println("📋 Step 1: Loading CronJob resource...")
	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		fmt.Printf("❌ Error fetching CronJob: %v\n", err)
		log.Error(err, "unable to fetch CronJob")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	fmt.Printf("✅ CronJob loaded: %s (Schedule: %s)\n", cronJob.Name, cronJob.Spec.Schedule)

	// List all active jobs, and update the status
	fmt.Println("📝 Step 2: Listing child Jobs...")
	var childJobs kbatch.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		fmt.Printf("❌ Error listing child Jobs: %v\n", err)
		log.Error(err, "unable to list child Jobs")
		return ctrl.Result{}, err
	}
	fmt.Printf("📊 Found %d child Jobs\n", len(childJobs.Items))

	// find the active list of jobs
	var activeJobs []*kbatch.Job
	var successfulJobs []*kbatch.Job
	var failedJobs []*kbatch.Job
	var mostRecentTime *time.Time // find the last run so we can update the status

	fmt.Println("🔍 Step 3: Categorizing Jobs by status...")
	for i, job := range childJobs.Items {
		_, finishedType := utils.IsJobFinished(&job)
		switch finishedType {
		case "": // ongoing
			activeJobs = append(activeJobs, &childJobs.Items[i])
			fmt.Printf("⏳ Active Job: %s\n", job.Name)
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
			fmt.Printf("❌ Failed Job: %s\n", job.Name)
		case kbatch.JobComplete:
			successfulJobs = append(successfulJobs, &childJobs.Items[i])
			fmt.Printf("✅ Successful Job: %s\n", job.Name)
		}

		// We'll store the launch time in an annotation, so we'll reconstitute that from
		// the active jobs themselves.
		scheduledTimeForJob, err := utils.GetScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for child job", "job", &job)
			continue
		}
		if scheduledTimeForJob != nil {
			if mostRecentTime == nil || mostRecentTime.Before(*scheduledTimeForJob) {
				mostRecentTime = scheduledTimeForJob
			}
		}
	}

	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}
	cronJob.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference to active job", "job", activeJob)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	fmt.Printf("📈 Job Summary - Active: %d, Successful: %d, Failed: %d\n", len(activeJobs), len(successfulJobs), len(failedJobs))
	log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	fmt.Println("💾 Step 4: Updating CronJob status...")
	if err := r.Status().Update(ctx, &cronJob); err != nil {
		fmt.Printf("❌ Error updating CronJob status: %v\n", err)
		log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}
	fmt.Println("✅ Status updated successfully")

	// Clean up old jobs according to the history limits
	// NB: deleting these are "best effort" -- if we fail on a particular one,
	// we won't requeue just to finish the deleting.
	fmt.Println("🧹 Step 5: Cleaning up old jobs...")
	if cronJob.Spec.FailedJobsHistoryLimit != nil {
		fmt.Printf("🗑️  Cleaning failed jobs (limit: %d)\n", *cronJob.Spec.FailedJobsHistoryLimit)
		sort.Slice(failedJobs, func(i, j int) bool {
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})
		for i, job := range failedJobs {
			if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				fmt.Printf("❌ Failed to delete old failed job %s: %v\n", job.Name, err)
				log.Error(err, "unable to delete old failed job", "job", job)
			} else {
				fmt.Printf("🗑️  Deleted old failed job: %s\n", job.Name)
				log.V(0).Info("deleted old failed job", "job", job)
			}
		}
	}

	if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
		fmt.Printf("🗑️  Cleaning successful jobs (limit: %d)\n", *cronJob.Spec.SuccessfulJobsHistoryLimit)
		sort.Slice(successfulJobs, func(i, j int) bool {
			if successfulJobs[i].Status.StartTime == nil {
				return successfulJobs[j].Status.StartTime != nil
			}
			return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
		})
		for i, job := range successfulJobs {
			if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SuccessfulJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
				fmt.Printf("❌ Failed to delete old successful job %s: %v\n", job.Name, err)
				log.Error(err, "unable to delete old successful job", "job", job)
			} else {
				fmt.Printf("🗑️  Deleted old successful job: %s\n", job.Name)
				log.V(0).Info("deleted old successful job", "job", job)
			}
		}
	}
	// Check if we’re suspended (and don’t do anything else if we are)

	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	// figure out the next times that we need to create
	// jobs at (or anything we missed).
	fmt.Println("⏰ Step 7: Calculating schedule...")
	missedRun, nextRun, err := utils.GetNextSchedule(&cronJob, r.Clock.Now())
	if err != nil {
		fmt.Printf("❌ Error calculating schedule: %v\n", err)
		log.Error(err, "unable to figure out CronJob schedule")
		// we don't really care about requeuing until we get an update that
		// fixes the schedule, so don't return an error
		return ctrl.Result{}, nil
	}
	fmt.Printf("⏰ Current time: %v, Next run: %v\n", r.Clock.Now(), nextRun)
	if !missedRun.IsZero() {
		fmt.Printf("⚠️  Missed run detected: %v\n", missedRun)
	}

	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Clock.Now())} // save this so we can re-use it elsewhere
	log = log.WithValues("now", r.Clock.Now(), "next run", nextRun)

	// Run a new job if it’s on schedule, not past the deadline, and not blocked by our concurrency policy

	if missedRun.IsZero() {
		fmt.Printf("😴 No missed runs, sleeping until next run at %v\n", nextRun)
		log.V(1).Info("no upcoming scheduled times, sleeping until next")
		return scheduledResult, nil
	}

	// make sure we're not too late to start the run
	log = log.WithValues("current run", missedRun)
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).Before(r.Clock.Now())
	}
	if tooLate {
		fmt.Println("⏰ Too late to start missed run, skipping")
		log.V(1).Info("missed starting deadline for last run, sleeping till next")
		// TODO(directxman12): events
		return scheduledResult, nil
	}
	fmt.Println("✅ Missed run is within deadline, proceeding...")

	// figure out how to run this job -- concurrency policy might forbid us from running
	// multiple at the same time...
	fmt.Printf("🔄 Step 8: Checking concurrency policy (%s)...\n", cronJob.Spec.ConcurrencyPolicy)
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		fmt.Printf("🚫 Concurrency policy FORBID blocks run (%d active jobs)\n", len(activeJobs))
		log.V(1).Info("concurrency policy blocks concurrent runs, skipping", "num active", len(activeJobs))
		return scheduledResult, nil
	}

	// ...or instruct us to replace existing ones...
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		fmt.Printf("🔄 Concurrency policy REPLACE: deleting %d active jobs\n", len(activeJobs))
		for _, activeJob := range activeJobs {
			// we don't care if the job was already deleted
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				fmt.Printf("❌ Failed to delete active job %s: %v\n", activeJob.Name, err)
				log.Error(err, "unable to delete active job", "job", activeJob)
				return ctrl.Result{}, err
			} else {
				fmt.Printf("🗑️  Deleted active job for replacement: %s\n", activeJob.Name)
			}
		}
	} else if len(activeJobs) > 0 {
		fmt.Printf("✅ Concurrency policy ALLOW: %d active jobs will continue\n", len(activeJobs))
	}

	// actually make the job...
	fmt.Println("🏗️  Step 9: Creating new Job...")
	job, err := utils.ConstructJobForCronJob(&cronJob, missedRun, r.Scheme)
	if err != nil {
		fmt.Printf("❌ Error constructing job: %v\n", err)
		log.Error(err, "unable to construct job from template")
		// don't bother requeuing until we get a change to the spec
		return scheduledResult, nil
	}
	fmt.Printf("🏗️  Job constructed: %s\n", job.Name)

	// ...and create it on the cluster
	fmt.Println("🚀 Creating Job on cluster...")
	if err := r.Create(ctx, job); err != nil {
		fmt.Printf("❌ Error creating Job: %v\n", err)
		log.Error(err, "unable to create Job for CronJob", "job", job)
		return ctrl.Result{}, err
	}

	fmt.Printf("🎉 Successfully created Job: %s\n", job.Name)
	log.V(1).Info("created Job for CronJob run", "job", job)

	// Requeue when we either see a running job (done automatically) or it’s time for the next scheduled run.
	// we'll requeue once we see the running job, and update our status
	return scheduledResult, nil
}

var (
	jobOwnerKey = ".metadata.controller"
	apiGVStr    = batchv1.GroupVersion.String()
)

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// set up a real clock, since we're not in a test
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatch.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		// grab the job object, extract the owner...
		job := rawObj.(*kbatch.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		// ...make sure it's a CronJob...
		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatch.Job{}).
		Named("cronjob").
		Complete(r)
}
