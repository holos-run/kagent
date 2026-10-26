// Package definition identifies the Kubernetes source of compiled agent behavior.
package definition

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
)

type Kind string

const (
	Agent        Kind = "Agent"
	ScheduledRun Kind = "ScheduledRun"
)

// Source identifies the resource whose AgentSpec produced a runtime revision.
type Source struct {
	Kind      Kind   `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

func (s Source) Key() string { return string(s.Kind) + "/" + s.Namespace + "/" + s.Name }

func (s Source) Validate() error {
	if s.Kind != Agent && s.Kind != ScheduledRun {
		return fmt.Errorf("unsupported definition source kind %q", s.Kind)
	}
	if s.Namespace == "" || s.Name == "" || s.UID == "" {
		return fmt.Errorf("definition source requires namespace, name and UID")
	}
	return nil
}

func (s Source) Ref() *apiv1.ResourceReference {
	return &apiv1.ResourceReference{Namespace: s.Namespace, Name: s.Name}
}

// FromSession reads trusted provenance persisted when the Session was reserved.
func FromSession(session *apiv1.Session) Source {
	if agent := session.GetAgent(); agent != nil {
		return Source{Kind: Agent, Namespace: agent.GetRef().GetNamespace(), Name: agent.GetRef().GetName(), UID: agent.GetUid()}
	}
	if schedule := session.GetScheduledRun(); schedule != nil {
		return Source{Kind: ScheduledRun, Namespace: schedule.GetRef().GetNamespace(), Name: schedule.GetRef().GetName(), UID: schedule.GetUid()}
	}
	return Source{}
}

// ValidateSpec checks the reference shape before compilation or schedule admission.
func ValidateSpec(spec v1alpha3.AgentSpec) error {
	if (spec.Template.Inline == nil) == (spec.Template.Ref == nil) {
		return fmt.Errorf("agent template requires exactly one of inline or ref")
	}
	if spec.Template.Ref != nil && len(validation.IsDNS1123Subdomain(spec.Template.Ref.Name)) != 0 {
		return fmt.Errorf("agent template ref.name must be a valid resource name")
	}
	if len(validation.IsDNS1123Subdomain(spec.HarnessRef.Name)) != 0 {
		return fmt.Errorf("agent harnessRef.name must be a valid resource name")
	}
	return nil
}

// Reference selects a definition by kind and Kubernetes name, without claiming a UID.
type Reference struct {
	Kind      Kind
	Namespace string
	Name      string
}

func (s Source) Reference() Reference {
	return Reference{Kind: s.Kind, Namespace: s.Namespace, Name: s.Name}
}
func (r Reference) NamespacedName() types.NamespacedName {
	return types.NamespacedName{Namespace: r.Namespace, Name: r.Name}
}

// Tenant retains the Agent endpoint grammar and gives scheduled conversations a
// distinct route. Kubernetes namespaces cannot contain the kind prefix.
func (r Reference) Tenant() string {
	if r.Kind == Agent {
		return r.Namespace + "/" + r.Name
	}
	return string(r.Kind) + "/" + r.Namespace + "/" + r.Name
}

func (r Reference) Validate() error {
	if (r.Kind != Agent && r.Kind != ScheduledRun) || len(validation.IsDNS1123Label(r.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(r.Name)) != 0 {
		return fmt.Errorf("invalid definition reference")
	}
	return nil
}

func ParseTenant(tenant string) (Reference, error) {
	kind := Agent
	if strings.HasPrefix(tenant, string(ScheduledRun)+"/") {
		kind = ScheduledRun
		tenant = strings.TrimPrefix(tenant, string(ScheduledRun)+"/")
	}
	namespace, name, _ := strings.Cut(tenant, "/")
	ref := Reference{Kind: kind, Namespace: namespace, Name: name}
	return ref, ref.Validate()
}

// AgentReference converts an optional Agent filter at the API boundary.
func AgentReference(ref *apiv1.ResourceReference) *Reference {
	if ref == nil {
		return nil
	}
	return &Reference{Kind: Agent, Namespace: ref.Namespace, Name: ref.Name}
}
