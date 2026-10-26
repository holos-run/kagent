package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kagent-dev/kagent/go/api/definition"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/scheduling"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type ScheduledRunReconciliation struct {
	Resource   *v1alpha3.ScheduledRun
	Conditions []metav1.Condition
	Failure    string
}

func (s ScheduledRunReconciliation) ResourceName() string {
	return s.Resource.Namespace + "/" + s.Resource.Name
}
func (s ScheduledRunReconciliation) Equals(other ScheduledRunReconciliation) bool {
	return s.Resource.UID == other.Resource.UID && s.Resource.Generation == other.Resource.Generation &&
		s.Resource.DeletionTimestamp.Equal(other.Resource.DeletionTimestamp) &&
		reflect.DeepEqual(s.Resource.Spec, other.Resource.Spec) &&
		reflect.DeepEqual(s.Conditions, other.Conditions) && s.Failure == other.Failure
}

func newScheduledRunReconciliations(resources krt.Collection[*v1alpha3.ScheduledRun], revisions krt.Collection[AgentReconciliation], opts krt.OptionsBuilder) krt.Collection[ScheduledRunReconciliation] {
	return krt.NewCollection(resources, func(ctx krt.HandlerContext, resource *v1alpha3.ScheduledRun) *ScheduledRunReconciliation {
		state := &ScheduledRunReconciliation{Resource: resource}
		if !resource.DeletionTimestamp.IsZero() {
			return state
		}
		if err := scheduling.Validate(resource.Spec); err != nil {
			state.Failure = err.Error()
			return state
		}
		source := definition.Source{Kind: definition.ScheduledRun, Namespace: resource.Namespace, Name: resource.Name, UID: string(resource.UID)}
		compiled := krt.FetchOne(ctx, revisions, krt.FilterKey(source.Key()))
		if compiled == nil || compiled.Source != source {
			return state
		}
		status := statusForAgent(*compiled, resource.Generation, "")
		state.Conditions = status.Conditions
		return state
	}, opts.WithName("ScheduledRunReconciliations")...)
}

type scheduleProjectionStore interface {
	GetLatestSuccessfulRevision(context.Context, definition.Source) (string, error)
	SyncScheduledRun(context.Context, *database.ScheduledRun) (*database.ScheduledRun, error)
	GetActiveScheduledRunID(context.Context, types.NamespacedName) (uuid.UUID, error)
	RetireScheduledRun(context.Context, uuid.UUID) error
	DisableScheduledRun(context.Context, uuid.UUID, int64) error
}

// ScheduledRunReconciler is the sole writer of desired scheduling projections.
// Its KRT dependency on compiled inputs keeps scheduling and runtime readiness aligned.
type ScheduledRunReconciler struct {
	states  krt.Collection[ScheduledRunReconciliation]
	store   scheduleProjectionStore
	kube    client.Client
	reader  client.Reader
	caller  auth.Session
	queue   controllers.Queue
	handler krt.HandlerRegistration
	ctx     context.Context
}

func NewScheduledRunReconciler(states krt.Collection[ScheduledRunReconciliation], store scheduleProjectionStore, kube client.Client, reader client.Reader, caller auth.Session) *ScheduledRunReconciler {
	r := &ScheduledRunReconciler{states: states, store: store, kube: kube, reader: reader, caller: caller}
	r.queue = newReconciliationQueue("scheduled-runs", func(key any) error { return r.reconcile(r.ctx, key.(string)) })
	r.handler = states.Register(func(event krt.Event[ScheduledRunReconciliation]) {
		if event.New != nil {
			r.queue.Add(event.New.ResourceName())
		} else if event.Old != nil {
			r.queue.Add(event.Old.ResourceName())
		}
	})
	return r
}

func (*ScheduledRunReconciler) NeedLeaderElection() bool { return true }
func (r *ScheduledRunReconciler) Start(ctx context.Context) error {
	r.ctx = ctx
	if !r.handler.WaitUntilSynced(ctx.Done()) {
		r.queue.ShutDownEarly()
		return nil
	}
	r.queue.Run(ctx.Done())
	return nil
}

func (r *ScheduledRunReconciler) reconcile(ctx context.Context, key string) error {
	namespace, name, _ := strings.Cut(key, "/")
	ref := types.NamespacedName{Namespace: namespace, Name: name}
	// Read the projection before the live resource. Retiring only this observed
	// UID cannot accidentally retire a concurrently reconciled replacement.
	previousID, err := r.store.GetActiveScheduledRunID(ctx, ref)
	if err != nil && !errors.Is(err, database.ErrNotFound) {
		return err
	}
	resource := &v1alpha3.ScheduledRun{}
	if err := r.reader.Get(ctx, ref, resource); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		if previousID != uuid.Nil {
			return r.store.RetireScheduledRun(ctx, previousID)
		}
		return nil
	}
	id, err := uuid.Parse(string(resource.UID))
	if err != nil {
		return fmt.Errorf("invalid ScheduledRun UID: %w", err)
	}
	if previousID != uuid.Nil && previousID != id {
		if err := r.store.RetireScheduledRun(ctx, previousID); err != nil {
			return err
		}
	}
	if !resource.DeletionTimestamp.IsZero() {
		if err := r.store.RetireScheduledRun(ctx, id); err != nil {
			return err
		}
		return nil
	}
	state := r.states.GetKey(key)
	if state == nil || state.Resource.UID != resource.UID || state.Resource.Generation != resource.Generation {
		// KRT will enqueue the authoritative resource when its watch catches up.
		return nil
	}

	condition := metav1.Condition{Type: v1alpha3.ScheduledRunConditionReady, Status: metav1.ConditionTrue, Reason: "Reconciled", Message: "Schedule configuration is synchronized", ObservedGeneration: resource.Generation}
	failure := state.Failure
	if failure == "" {
		revision, err := r.store.GetLatestSuccessfulRevision(ctx, definition.Source{Kind: definition.ScheduledRun, Namespace: resource.Namespace, Name: resource.Name, UID: string(resource.UID)})
		if err != nil {
			return err
		}
		if revision == "" {
			failure = "Agent runtime has no successful revision yet"
			condition.Reason = "RuntimeNotReady"
		}
	} else {
		condition.Reason = "InvalidConfiguration"
	}
	if r.caller == nil {
		condition.Reason = "SchedulerNotConfigured"
		failure = "Scheduler service-account authentication is not configured"
	}
	if failure != "" {
		if err := r.store.DisableScheduledRun(ctx, id, resource.Generation); err != nil {
			return err
		}
		condition.Status, condition.Message = metav1.ConditionFalse, failure
	} else {
		_, err := r.store.SyncScheduledRun(ctx, &database.ScheduledRun{
			ID: id.String(), Creator: r.caller.Principal().ID,
			Ref:        types.NamespacedName{Namespace: resource.Namespace, Name: resource.Name},
			Generation: resource.Generation, ResourceVersion: resource.ResourceVersion, Spec: resource.Spec,
		})
		if err != nil {
			return err
		}
	}
	previous := resource.Status.DeepCopy()
	resource.Status.ObservedGeneration = resource.Generation
	resource.Status.Conditions = conditionsWithTransitionTimes(state.Conditions, previous.Conditions)
	meta.SetStatusCondition(&resource.Status.Conditions, condition)
	if reflect.DeepEqual(*previous, resource.Status) {
		return nil
	}
	if err := r.kube.Status().Update(ctx, resource); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
