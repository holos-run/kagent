# Installation library

The installation library centralizes setup and orchestration so the CLI and
embedding applications can reuse the same behavior. Explicit inputs for charts,
values, worker images, and additional database setup allow callers to extend the
workflow without duplicating its lifecycle logic.

`go/core/pkg/install` owns installation, prerequisite setup, bundled PostgreSQL,
and uninstall orchestration. The CLI owns flags, config loading, environment
reads, progress output, and the final API reachability check.

The package lives in the existing Go module so embedding applications can import it
without copying command internals or introducing another module to version.
Its `Client` takes a `Runner` and a Kubernetes client. `CommandRunner` executes
Helm and kubectl; tests use an in-memory runner and Kubernetes fake.

The application supplies its already loaded configuration and Kubernetes client:

```go
client := install.NewClient(install.CommandRunner{}, kubernetesClient)
plan, err := client.Plan(ctx, cfg)
if err != nil {
    return err
}
return client.Apply(ctx, plan, progress)
```

## Plan and apply

The caller supplies `install.Config` with chart names, repositories, versions,
values, worker image, provider credentials, profile YAML, and database settings.
`Client.Plan` renders the controller deployment to resolve its service account
before creating an ordered plan. It does not write Kubernetes resources, run
setup SQL, or generate signing keys. Chart rendering can fetch artifacts and
read values files.

`Plan.Steps()` returns independent copies of concrete operations:

1. The kagent CRD release.
2. The Substrate CRD release.
3. Substrate namespaces, identity pools and authentication configuration.
4. The pod-certificate controller release.
5. Bundled PostgreSQL deployment and initialization, unless skipped.
6. The Substrate release.
7. The kagent release.

`Client.Apply` executes these operations in order, stopping at the first error.
Its optional progress callback receives descriptions without chart values.
Substrate grants and PostgreSQL certificate mappings use the service account
from the rendered controller deployment. The Substrate CLI owner grant remains
alongside that controller grant.

Chart overrides retain the existing Helm layers: inline YAML, configured values
files in order, then `--set` values. A plan retains values file paths; callers
must keep those files unchanged between planning and applying. Plans contain
credential values and are not a safe serialization format for dry-run output.
Merging files, secret references, and an executable export format belong to the
dry-run feature.

## Embedding applications

Chart artifact names are separate from Helm release names. For example, an
embedding application can select `custom-kagent` and `custom-kagent-crds` while
retaining the `kagent` and `kagent-crds` releases. Each chart has its own repository,
version, inline values, values files, and set values. Loopback OCI registries use
plain HTTP for both rendering and installation.

`Postgres.ExtraSetup` accepts additional SQL and psql variables. These scripts run
after the existing kagent and Substrate setup inside one transaction, so additional
schemas do not require another database implementation. Scripts must
leave transaction boundaries to the library.

Embedding applications can call `Client.PrepareSubstrate`, `Client.PreparePostgres`, and
`Client.RunSQL` directly. `Postgres.ExtraHBA` adds database authentication rules
for an additional database login. `Postgres.Manifest` and `Postgres.SetupScript` expose
the assets independently of execution.

`ProviderAPIKeys` supplies the provider's registered credential names in priority
order. The caller decides how to load them, including Gemini's Google-key-first
environment lookup. The library never reads those environment variables itself.

## Uninstall

`Client.Uninstall` owns cleanup ordering and returns releases already absent.
The command renders warnings and the final result. Cleanup removes sandbox
templates while the controller can finalize them, then kagent and Substrate,
the bundled database if present, pod certificates, CRDs, namespace access grants,
Substrate namespaces and trust bundles. An external database and the kagent
namespace remain. A mismatched installation namespace stops cleanup before
deletion. Application-specific release-ownership checks stay with the caller.
