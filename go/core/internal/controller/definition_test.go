package controller

import (
	"istio.io/istio/pkg/kube/krt"

	"github.com/kagent-dev/kagent/go/api/definition"
	configv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/config/v1"
	v1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/translator"
)

func agentSource(agent *v1alpha3.Agent) definition.Source {
	return definition.Source{Kind: definition.Agent, Namespace: agent.Namespace, Name: agent.Name, UID: string(agent.UID)}
}

func testAgentReconciliations(agents krt.Collection[*v1alpha3.Agent], collections translator.Collections, observations krt.Collection[AgentRuntimeObservation], opts krt.OptionsBuilder, settings *configv1.RuntimeDefaultsConfig, images *configv1.HarnessImagesConfig) krt.Collection[AgentReconciliation] {
	sources := newDefinitionInputs(agents, krt.NewStaticCollection[*v1alpha3.ScheduledRun](nil, nil, opts.WithName("test-schedules")...), opts)
	return newAgentReconciliations(sources, collections, observations, opts, settings, images)
}
