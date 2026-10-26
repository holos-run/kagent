package controller

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"istio.io/istio/pkg/kube/krt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kagent-dev/kagent/go/api/definition"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/translator"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

type scheduleProjectionRecorder struct {
	projected *database.ScheduledRun
	revision  string
	disabled  bool
	retired   bool
	failure   error
}

func (s *scheduleProjectionRecorder) GetLatestSuccessfulRevision(context.Context, definition.Source) (string, error) {
	return s.revision, s.failure
}
func (s *scheduleProjectionRecorder) SyncScheduledRun(_ context.Context, run *database.ScheduledRun) (*database.ScheduledRun, error) {
	s.projected = run
	return run, s.failure
}
func (s *scheduleProjectionRecorder) GetActiveScheduledRunID(context.Context, types.NamespacedName) (uuid.UUID, error) {
	if s.projected == nil || s.retired {
		return uuid.Nil, database.ErrNotFound
	}
	return uuid.Parse(s.projected.ID)
}
func (s *scheduleProjectionRecorder) DisableScheduledRun(context.Context, uuid.UUID, int64) error {
	s.disabled = true
	return s.failure
}
func (s *scheduleProjectionRecorder) RetireScheduledRun(context.Context, uuid.UUID) error {
	s.retired = true
	return s.failure
}

func TestScheduledRunReconciliationLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		opts := krt.NewOptionsBuilder(ctx.Done(), "schedule-test", nil)
		resource := &v1alpha3.ScheduledRun{ObjectMeta: metav1.ObjectMeta{Name: "daily", Namespace: "team", UID: types.UID(uuid.NewString()), Generation: 1}, Spec: v1alpha3.ScheduledRunSpec{Agent: v1alpha3.AgentSpec{HarnessRef: corev1.LocalObjectReference{Name: "runtime"}, Template: v1alpha3.AgentTemplateSource{Ref: &corev1.LocalObjectReference{Name: "agent"}}}, Schedule: "0 9 * * *", Prompt: "hello"}}
		compiled := AgentReconciliation{Source: definition.Source{Kind: definition.ScheduledRun, Namespace: resource.Namespace, Name: resource.Name, UID: string(resource.UID)},
			Target:                &compiledTarget{RevisionID: translator.RevisionID{1}},
			ObservedActorTemplate: &ateapipb.ActorTemplate{Status: &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{GoldenTag: &ateapipb.ObjectRef{Name: "golden"}}}},
		}
		resources := krt.NewStaticCollection(nil, []*v1alpha3.ScheduledRun{resource}, opts.WithName("resources")...)
		revisions := krt.NewStaticCollection(nil, []AgentReconciliation{compiled}, opts.WithName("compiled")...)
		states := newScheduledRunReconciliations(resources, revisions, opts)
		synctest.Wait()
		scheme := runtime.NewScheme()
		require.NoError(t, v1alpha3.AddToScheme(scheme))
		kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha3.ScheduledRun{}).WithObjects(resource).Build()
		store := &scheduleProjectionRecorder{revision: "last-successful"}
		reconciler := &ScheduledRunReconciler{states: states, store: store, kube: kube, reader: kube, caller: auth.NewCredentialSession(auth.Principal{ID: "scheduler"}, nil)}
		key := client.ObjectKeyFromObject(resource)
		refresh := func() {
			require.NoError(t, kube.Get(ctx, key, resource))
			resources.UpdateObject(resource.DeepCopy())
			synctest.Wait()
		}

		require.NoError(t, reconciler.reconcile(ctx, "team/daily"))
		refresh()
		require.Empty(t, resource.Finalizers)
		require.Equal(t, "scheduler", store.projected.Creator)
		require.True(t, meta.IsStatusConditionTrue(resource.Status.Conditions, v1alpha3.ScheduledRunConditionReady))
		// Missing compilation does not withdraw the last successful runtime.
		revisions.DeleteObject(compiled.ResourceName())
		synctest.Wait()
		require.NoError(t, reconciler.reconcile(ctx, "team/daily"))
		refresh()
		require.False(t, store.disabled)
		require.True(t, meta.IsStatusConditionTrue(resource.Status.Conditions, v1alpha3.ScheduledRunConditionReady))
		store.revision = ""
		require.NoError(t, reconciler.reconcile(ctx, "team/daily"))
		refresh()
		require.True(t, store.disabled)
		require.Equal(t, "RuntimeNotReady", meta.FindStatusCondition(resource.Status.Conditions, v1alpha3.ScheduledRunConditionReady).Reason)
		require.NoError(t, kube.Delete(ctx, resource))
		store.failure = errors.New("database unavailable")
		require.Error(t, reconciler.reconcile(ctx, "team/daily"))
		store.failure = nil
		require.NoError(t, reconciler.reconcile(ctx, "team/daily"))
		require.True(t, store.retired)
		require.Error(t, kube.Get(ctx, key, &v1alpha3.ScheduledRun{}))
	})
}

func TestScheduledRunDeleteEventRetiresProjectionWithoutFinalizer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		resource := &v1alpha3.ScheduledRun{ObjectMeta: metav1.ObjectMeta{
			Name: "daily", Namespace: "team", UID: types.UID(uuid.NewString()), Generation: 1,
		}, Spec: v1alpha3.ScheduledRunSpec{Agent: v1alpha3.AgentSpec{HarnessRef: corev1.LocalObjectReference{Name: "runtime"}, Template: v1alpha3.AgentTemplateSource{Ref: &corev1.LocalObjectReference{Name: "agent"}}}, Schedule: "0 9 * * *", Prompt: "hello"}}
		opts := krt.NewOptionsBuilder(ctx.Done(), "delete-event", nil)
		states := krt.NewStaticCollection(nil, []ScheduledRunReconciliation{{Resource: resource}}, opts.WithName("states")...)
		scheme := runtime.NewScheme()
		require.NoError(t, v1alpha3.AddToScheme(scheme))
		kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha3.ScheduledRun{}).WithObjects(resource).Build()
		store := &scheduleProjectionRecorder{revision: "last-successful"}
		reconciler := NewScheduledRunReconciler(states, store, kube, kube, auth.NewCredentialSession(auth.Principal{ID: "scheduler"}, nil))
		done := make(chan error, 1)
		go func() { done <- reconciler.Start(ctx) }()
		synctest.Wait()
		require.NotNil(t, store.projected)
		require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(resource), resource))
		resource.Finalizers = nil
		require.NoError(t, kube.Update(ctx, resource))
		require.NoError(t, kube.Delete(ctx, resource))
		states.DeleteObject("team/daily")
		synctest.Wait()
		require.True(t, store.retired, "a delete event must retire the durable projection even when the finalizer was removed")
		cancel()
		require.NoError(t, <-done)
	})
}

func TestScheduledRunEqualityIgnoresStatusWrites(t *testing.T) {
	before := ScheduledRunReconciliation{Resource: &v1alpha3.ScheduledRun{ObjectMeta: metav1.ObjectMeta{Name: "daily", Namespace: "team", UID: "uid", Generation: 1}}}
	after := before
	after.Resource = before.Resource.DeepCopy()
	after.Resource.ResourceVersion = "2"
	after.Resource.Status.ObservedGeneration = 1
	require.True(t, before.Equals(after))
	after.Resource.Spec.Paused = new(true)
	require.False(t, before.Equals(after))
}
