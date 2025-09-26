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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"reflect"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"

	cronjobv1 "tutorial.kubebuilder.io/project/api/v1"
)

var _ = Describe("CronJob Controller", func() {

	// Define utility constants for object names and testing timeouts/durations and intervals.
	const (
		CronjobName      = "test-cronjob"
		CronjobNamespace = "default"
		JobName          = "test-job"

		timeout  = time.Second * 10
		duration = time.Second * 10
		interval = time.Millisecond * 250
	)

	It("Should increase CronJob Status.Active count when new Jobs are created", func() {
		By("By creating a new CronJob")
		ctx := context.Background()
		cronJob := &cronjobv1.CronJob{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "batch.tutorial.kubebuilder.io/v1",
				Kind:       "CronJob",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      CronjobName,
				Namespace: CronjobNamespace,
			},
			Spec: cronjobv1.CronJobSpec{
				Schedule: "1 * * * *",
				JobTemplate: batchv1.JobTemplateSpec{
					Spec: batchv1.JobSpec{
						// For simplicity, we only fill out the required fields.
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								// For simplicity, we only fill out the required fields.
								Containers: []v1.Container{
									{
										Name:  "test-container",
										Image: "test-image",
									},
								},
								RestartPolicy: v1.RestartPolicyOnFailure,
							},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, cronJob)).To(Succeed())

	})
	cronjobLookupKey := types.NamespacedName{Name: CronjobName, Namespace: CronjobNamespace}
	createdCronjob := &cronjobv1.CronJob{}

	// We'll need to retry getting this newly created CronJob, given that creation may not immediately happen.
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, cronjobLookupKey, createdCronjob)).To(Succeed())
	}, timeout, interval).Should(Succeed())
	// Let's make sure our Schedule string value was properly converted/handled.
	Expect(createdCronjob.Spec.Schedule).To(Equal("1 * * * *"))

	By("By checking the CronJob has zero active Jobs")
	Consistently(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, cronjobLookupKey, createdCronjob)).To(Succeed())
		g.Expect(createdCronjob.Status.Active).To(BeEmpty())
	}, duration, interval).Should(Succeed())

	By("By creating a new Job")
	testJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      JobName,
			Namespace: CronjobNamespace,
		},
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					// For simplicity, we only fill out the required fields.
					Containers: []v1.Container{
						{
							Name:  "test-container",
							Image: "test-image",
						},
					},
					RestartPolicy: v1.RestartPolicyOnFailure,
				},
			},
		},
	}

	By("By checking that the CronJob has one active Job")
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, cronjobLookupKey, createdCronjob)).To(Succeed(), "should GET the CronJob")
		g.Expect(createdCronjob.Status.Active).To(HaveLen(1), "should have exactly one active job")
		g.Expect(createdCronjob.Status.Active[0].Name).To(Equal(JobName), "the wrong job is active")
	}, timeout, interval).Should(Succeed(), "should list our active job %s in the active jobs list in status", JobName)

	// Note that your CronJob’s GroupVersionKind is required to set up this owner reference.
	kind := reflect.TypeOf(cronjobv1.CronJob{}).Name()
	gvk := cronjobv1.GroupVersion.WithKind(kind)

	controllerRef := metav1.NewControllerRef(createdCronjob, gvk)
	testJob.SetOwnerReferences([]metav1.OwnerReference{*controllerRef})
	Expect(k8sClient.Create(ctx, testJob)).To(Succeed())
	// Note that you can not manage the status values while creating the resource.
	// The status field is managed separately to reflect the current state of the resource.
	// Therefore, it should be updated using a PATCH or PUT operation after the resource has been created.
	// Additionally, it is recommended to use StatusConditions to manage the status. For further information see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	testJob.Status.Active = 2
	Expect(k8sClient.Status().Update(ctx, testJob)).To(Succeed())

	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed

		}
		cronjob := &cronjobv1.CronJob{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind CronJob")
			err := k8sClient.Get(ctx, typeNamespacedName, cronjob)
			if err != nil && errors.IsNotFound(err) {
				resource := &cronjobv1.CronJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &cronjobv1.CronJob{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance CronJob")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &CronJobReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})
