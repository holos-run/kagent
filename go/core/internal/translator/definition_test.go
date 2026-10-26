package translator_test

import (
	"context"

	"github.com/kagent-dev/kagent/go/api/definition"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/translator"
)

func compileAgent(c *translator.Compiler, ctx context.Context, agent *v1alpha3.Agent) (*translator.CompileResult, error) {
	return c.CompileAgent(ctx, definition.Source{Kind: definition.Agent, Namespace: agent.Namespace, Name: agent.Name, UID: string(agent.UID)}, agent.Spec)
}
