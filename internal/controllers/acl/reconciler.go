/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package acl

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cskafkav1alpha1 "github.com/sap/kafka-operator/api/v1alpha1"
	"github.com/sap/kafka-operator/internal/connection"
	"github.com/sap/kafka-operator/internal/kafka"
	"github.com/sap/kafka-operator/internal/kafka/franz"
	"github.com/sap/kafka-operator/internal/util"
	"github.com/sap/kafka-operator/pkg/types"
)

type Reconciler struct {
	name          string
	finalizer     string
	fieldOwner    string
	client        client.Client
	eventRecorder record.EventRecorder
}

var _ reconcile.Reconciler = &Reconciler{}

func NewReconciler(name string, clnt client.Client, eventRecorder record.EventRecorder) *Reconciler {
	finalizer := fmt.Sprintf("%s/finalize", name)
	fieldOwner := name

	return &Reconciler{
		name:          name,
		finalizer:     finalizer,
		fieldOwner:    fieldOwner,
		client:        client.WithFieldOwner(clnt, fieldOwner),
		eventRecorder: eventRecorder,
	}
}

func (r *Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (result reconcile.Result, err error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(1).Info("Running reconcile")

	now := metav1.Now()

	// read requested acl
	acl := &cskafkav1alpha1.Acl{}
	if err := r.client.Get(ctx, request.NamespacedName, acl); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("not found; ignoring")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("unexpected get error: %w", err)
	}
	savedAcl := acl.DeepCopy()

	// deferred status handling
	skipStatusUpdate := false
	defer func() {
		// recover panic and re-panic; this is needed to avoid the logic in this function to be executed in panic cases
		if r := recover(); r != nil {
			log.Error(fmt.Errorf("panic occurred during reconcile"), "panic", r)
			// re-panic in order skip the remaining steps
			panic(r)
		}

		// in error cases, always set state and Ready condition
		if err != nil {
			if retriableError, ok := errors.AsType[types.RetriableError](err); ok {
				acl.Status.State = cskafkav1alpha1.AclStatePending
				acl.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.AclReadyReasonRetrying, util.Capitalize(retriableError.Error()))
				result.RequeueAfter = retriableError.RetryAfter()
				err = nil
			} else {
				acl.Status.State = cskafkav1alpha1.AclStateFailed
				acl.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.AclReadyReasonError, util.Capitalize(err.Error()))
			}
		}

		// set some Ready condition if there is none;
		// this helps observers such as kstatus to understand things right
		if acl.Status.GetReadyCondition() == nil {
			acl.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.AclReadyReasonUnknown, "Acl state cannot be determined")
		}

		// set observed generations
		acl.Status.ObservedGeneration = acl.Generation
		for i := 0; i < len(acl.Status.Conditions); i++ {
			acl.Status.Conditions[i].ObservedGeneration = acl.Generation
		}

		// write log
		log.V(1).Info("reconcile done", "withError", err != nil, "requeue", result.Requeue || result.RequeueAfter > 0, "requeueAfter", result.RequeueAfter.String())

		// see if status has changed so far; this must be done before updating the timestamps
		statusChanged := !reflect.DeepEqual(acl.Status, savedAcl.Status)

		// update timestamps
		acl.Status.LastObservedAt = &now
		for i := 0; i < len(acl.Status.Conditions); i++ {
			cond := &acl.Status.Conditions[i]
			if savedCond := savedAcl.Status.GetCondition(cond.Type); savedCond == nil || cond.Status != savedCond.Status {
				cond.LastTransitionTime = now
			}
		}

		// update status (unless suppressed)
		if !skipStatusUpdate {
			if updateErr := r.client.Status().Update(ctx, acl); updateErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to update status for acl %s/%s: %w", acl.Namespace, acl.Name, updateErr))
				result = ctrl.Result{}
			}
		}

		// send events; we do it only if something essentially changed, in order to avoid eventing noise,
		// and to avoid running into events being dropped by the event broadcaster because of rate limits
		if statusChanged {
			readyCondition := acl.Status.GetReadyCondition()
			if acl.Status.State == cskafkav1alpha1.AclStateFailed {
				r.eventRecorder.Event(acl, corev1.EventTypeWarning, readyCondition.Reason, readyCondition.Message)
			} else {
				r.eventRecorder.Event(acl, corev1.EventTypeNormal, readyCondition.Reason, readyCondition.Message)
			}
		}
	}()

	// set a first status and requeue immediately
	if acl.Status.State == "" {
		acl.Status.State = cskafkav1alpha1.AclStateInProgress
		acl.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.AclReadyReasonFirstSeen, "First seen")
		return ctrl.Result{Requeue: true}, nil
	}

	// compute desired ACLs (canonical low-level form)
	desired := toKafkaAcls(acl.Spec.ExpandEntries())

	// load references
	if err := acl.LoadReferences(ctx, r.client); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to load references for acl %s/%s: %w", acl.Namespace, acl.Name, err)
	}

	// parse and validate connection details
	connectionDetails, err := connection.ParseConnectionDetails(acl.Spec.ConnectionSecretRef.Value())
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to parse connection details for acl %s/%s: %w", acl.Namespace, acl.Name, err)
	}

	// get kafka client
	cli, err := franz.NewClient(r.name, connectionDetails)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// read existing ACLs for this principal
	existing, err := cli.ListAcls(ctx, acl.Spec.Principal)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error reading ACLs for principal %s: %w", acl.Spec.Principal, err)
	}

	if acl.DeletionTimestamp.IsZero() {
		// apply case
		if controllerutil.AddFinalizer(acl, r.finalizer) {
			if err := r.client.Update(ctx, acl); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to add finalizer to acl %s/%s: %w", acl.Namespace, acl.Name, err)
			}
		}

		existingSet := toSet(existing)
		desiredSet := toSet(desired)

		toCreate := difference(desiredSet, existingSet)
		toDelete := difference(existingSet, desiredSet)

		// apply one kind of change per reconcile, then requeue (mirrors the topic controller)
		if len(toCreate) > 0 {
			if err := cli.CreateAcls(ctx, acl.Spec.Principal, toCreate); err != nil {
				return ctrl.Result{}, fmt.Errorf("error creating ACLs for principal %s: %w", acl.Spec.Principal, err)
			}
			acl.Status.State = cskafkav1alpha1.AclStateInProgress
			acl.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.AclReadyReasonProcessing, "Acls created successfully")
			return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
		}

		if len(toDelete) > 0 {
			if err := cli.DeleteAcls(ctx, acl.Spec.Principal, toDelete); err != nil {
				return ctrl.Result{}, fmt.Errorf("error deleting ACLs for principal %s: %w", acl.Spec.Principal, err)
			}
			acl.Status.State = cskafkav1alpha1.AclStateInProgress
			acl.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.AclReadyReasonProcessing, "Acls updated successfully")
			return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
		}

		acl.Status.Details = toDetails(existing)
		acl.Status.State = cskafkav1alpha1.AclStateReady
		acl.Status.SetReadyCondition(metav1.ConditionTrue, cskafkav1alpha1.AclReadyReasonSynced, "Acls synchronized successfully")
		return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
	} else {
		// delete case: remove all ACLs for this principal, then drop the finalizer
		if len(existing) > 0 {
			if err := cli.DeleteAcls(ctx, acl.Spec.Principal, existing); err != nil {
				return ctrl.Result{}, fmt.Errorf("error deleting ACLs for principal %s: %w", acl.Spec.Principal, err)
			}
			acl.Status.State = cskafkav1alpha1.AclStateDeleting
			acl.Status.SetReadyCondition(metav1.ConditionFalse, cskafkav1alpha1.AclReadyReasonDeleting, "Acls deleted successfully")
			return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
		} else {
			if controllerutil.RemoveFinalizer(acl, r.finalizer) {
				if err := r.client.Update(ctx, acl); err != nil {
					return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from acl %s/%s: %w", acl.Namespace, acl.Name, err)
				}
			}
			skipStatusUpdate = true
			return ctrl.Result{}, nil
		}
	}
}

// toKafkaAcls maps canonical low-level rules to the neutral kafka.Acl form.
func toKafkaAcls(rules []cskafkav1alpha1.AclRule) []kafka.Acl {
	acls := make([]kafka.Acl, 0, len(rules))
	for _, rule := range rules {
		resourceName := rule.ResourceName
		if rule.ResourceType == cskafkav1alpha1.AclResourceTypeCluster {
			// Cluster has a fixed broker-side name; normalize so diffing is stable.
			resourceName = ""
		}
		acls = append(acls, kafka.Acl{
			ResourceType:   string(rule.ResourceType),
			ResourceName:   resourceName,
			PatternType:    string(rule.PatternType),
			Operation:      string(rule.Operation),
			PermissionType: string(rule.PermissionType),
			Host:           rule.Host,
		})
	}
	return acls
}

func toDetails(acls []kafka.Acl) []cskafkav1alpha1.AclDetail {
	if len(acls) == 0 {
		return nil
	}
	details := make([]cskafkav1alpha1.AclDetail, 0, len(acls))
	for _, acl := range acls {
		details = append(details, cskafkav1alpha1.AclDetail{
			ResourceType:   cskafkav1alpha1.AclResourceType(acl.ResourceType),
			ResourceName:   acl.ResourceName,
			PatternType:    cskafkav1alpha1.AclPatternType(acl.PatternType),
			Operation:      cskafkav1alpha1.AclOperation(acl.Operation),
			PermissionType: cskafkav1alpha1.AclPermissionType(acl.PermissionType),
			Host:           acl.Host,
		})
	}
	return details
}

func toSet(acls []kafka.Acl) map[kafka.Acl]struct{} {
	set := make(map[kafka.Acl]struct{}, len(acls))
	for _, acl := range acls {
		set[acl] = struct{}{}
	}
	return set
}

// difference returns the ACLs in a that are not in b.
func difference(a, b map[kafka.Acl]struct{}) []kafka.Acl {
	var out []kafka.Acl
	for acl := range a {
		if _, ok := b[acl]; !ok {
			out = append(out, acl)
		}
	}
	return out
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cskafkav1alpha1.Acl{}, builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, predicate.AnnotationChangedPredicate{}))).
		Complete(r)
}
