package a2agateway

import (
	"context"

	a2a "github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/kagent-dev/kagent/go/api/definition"
)

// route resolves the source selected by the SDK transport metadata.
func route(ctx context.Context) (definition.Reference, error) {
	tenant, _ := a2a.TenantFrom(ctx)
	ref, err := definition.ParseTenant(tenant)
	if err != nil {
		return definition.Reference{}, a2a.NewError(a2a.ErrInvalidRequest, "invalid definition tenant")
	}
	return ref, nil
}
