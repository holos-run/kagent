package translator

import (
	"istio.io/istio/pkg/kube/krt"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/kagent-dev/kagent/go/api/v1alpha3"
)

// agentSpecReferencePolicies collects policies independently of configuration validity.
// Missing inputs are left to normal resolution; they must not hide policies on
// other inputs. Fetch every reachable reference to retain KRT dependencies even
// while compilation fails. The visited set also bounds invalid template cycles.
func agentSpecReferencePolicies(ctx krt.HandlerContext, inputs Collections, namespace string, spec v1alpha3.AgentSpec) []*v1alpha3.AllowedNamespaces {
	var policies []*v1alpha3.AllowedNamespaces
	addModel := func(ref *v1alpha3.ObjectReference) {
		if ref == nil {
			return
		}
		key := ref.NamespacedName(namespace)
		model := krt.FetchOne(ctx, inputs.ResolvedModelConfigs, krt.FilterObjectName(key))
		if model != nil {
			policies = append(policies, model.Config.Spec.AllowedNamespaces)
		}
	}
	if harness := krt.FetchOne(ctx, inputs.Harnesses, krt.FilterObjectName(types.NamespacedName{Namespace: namespace, Name: spec.HarnessRef.Name})); harness != nil && (*harness).Spec.Kagent != nil {
		spec := (*harness).Spec.Kagent
		if spec.Memory != nil {
			addModel(&spec.Memory.ModelConfigRef)
		}
		if spec.Compaction != nil && spec.Compaction.Summarizer != nil {
			addModel(spec.Compaction.Summarizer.ModelConfigRef)
		}
	}
	seen := map[string]bool{}
	var visitTemplate func(*v1alpha3.AgentTemplateSpec)
	visitNamed := func(name string) {
		if seen[name] {
			return
		}
		seen[name] = true
		template := krt.FetchOne(ctx, inputs.AgentTemplates, krt.FilterObjectName(types.NamespacedName{Namespace: namespace, Name: name}))
		if template != nil {
			visitTemplate(&(*template).Spec)
		}
	}
	visitTemplate = func(spec *v1alpha3.AgentTemplateSpec) {
		addModel(spec.ModelConfig)
		for _, tool := range spec.Tools {
			if tool.MCP != nil && tool.MCP.Server.Kind == "RemoteMCPServer" {
				key := (v1alpha3.ObjectReference{Name: tool.MCP.Server.Name, Namespace: ptr.Deref(tool.MCP.Server.Namespace, "")}).NamespacedName(namespace)
				server := krt.FetchOne(ctx, inputs.RemoteMCPServers, krt.FilterObjectName(key))
				if server != nil {
					policies = append(policies, (*server).Spec.AllowedNamespaces)
				}
			}
			if tool.SubAgent != nil && tool.SubAgent.TemplateRef != nil {
				visitNamed(tool.SubAgent.TemplateRef.Name)
			}
		}
	}
	if spec.Template.Inline != nil {
		visitTemplate(spec.Template.Inline)
	}
	if spec.Template.Ref != nil {
		visitNamed(spec.Template.Ref.Name)
	}
	return policies
}

// AgentSpecNeedsNamespace reports whether any referenced model or MCP policy needs
// source namespace labels, including auxiliary models and subagent references.
func AgentSpecNeedsNamespace(ctx krt.HandlerContext, inputs Collections, namespace string, spec v1alpha3.AgentSpec) bool {
	policies := agentSpecReferencePolicies(ctx, inputs, namespace, spec)
	for _, policy := range policies {
		if policy != nil && policy.From == v1alpha3.NamespacesFromSelector {
			return true
		}
	}
	return false
}
