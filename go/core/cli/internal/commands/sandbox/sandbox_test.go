package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"github.com/kagent-dev/kagent/go/core/cli/internal/connection"
	clioutput "github.com/kagent-dev/kagent/go/core/cli/internal/output"
)

func TestCreateSandboxRetentionFlag(t *testing.T) {
	scenarios := []struct {
		name string
		args []string
		want bool
	}{
		{name: "Given default creation, when the CLI creates a sandbox, assert that suspended retention is disabled"},
		{name: "Given opted-in creation, when the CLI creates a sandbox, assert that the API receives suspended retention", args: []string{"--retain-while-suspended"}, want: true},
		{name: "Given explicitly disabled retention, when the CLI creates a sandbox, assert that the API receives the default policy", args: []string{"--retain-while-suspended=false"}},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, requests := sandboxCLIEndpoint(t)
			root := &cobra.Command{Use: "kagent"}
			connection.RegisterFlags(root.PersistentFlags())
			root.PersistentFlags().String(clioutput.FlagName, "table", "Output format")
			root.AddCommand(NewCmd())
			var output bytes.Buffer
			root.SetOut(&output)
			root.SetErr(&output)
			args := []string{"sandbox", "create", "scratch", "--request-id", "retained-create", "--ttl", "10m", "--namespace", "team-a", "--api-url", "http://sandbox.test", "--timeout", "5s"}
			root.SetArgs(append(args, scenario.args...))

			require.NoError(t, root.ExecuteContext(ctx))
			request := <-requests
			require.Equal(t, scenario.want, request.GetRetainWhileSuspended())
			require.Equal(t, "retained-create", request.GetRequestId())
			require.Equal(t, "team-a", request.GetSandboxTemplate().GetNamespace())
			require.Equal(t, "scratch", request.GetSandboxTemplate().GetName())
			require.Equal(t, 10*time.Minute, request.GetTtl().AsDuration())
			require.Contains(t, output.String(), "sandbox-created")
		})
	}
}

func TestSandboxExpirationDisplay(t *testing.T) {
	deadline := timestamppb.New(time.Date(2026, time.November, 10, 12, 0, 0, 0, time.UTC))
	scenarios := []struct {
		name  string
		value *apiv1.Sandbox
		want  string
	}{
		{
			name:  "Given retained suspension, when the CLI prints a table, assert that storage retention replaces the deadline",
			value: &apiv1.Sandbox{State: apiv1.RuntimeState_RUNTIME_STATE_SUSPENDED, Operation: apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE, RetainWhileSuspended: true},
			want:  "retained while suspended",
		},
		{
			name:  "Given a pending resume, when the CLI prints a table, assert that its deadline is visible",
			value: &apiv1.Sandbox{State: apiv1.RuntimeState_RUNTIME_STATE_SUSPENDED, Operation: apiv1.RuntimeOperation_RUNTIME_OPERATION_RESUME, RetainWhileSuspended: true, ExpiresAt: deadline},
			want:  "2026-11-10T12:00:00Z",
		},
		{
			name:  "Given default suspension, when the CLI prints a table, assert that the fixed deadline is visible",
			value: &apiv1.Sandbox{State: apiv1.RuntimeState_RUNTIME_STATE_SUSPENDED, Operation: apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE, ExpiresAt: deadline},
			want:  "2026-11-10T12:00:00Z",
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			var output bytes.Buffer
			require.NoError(t, writeSandbox(&output, clioutput.FormatTable, scenario.value))
			require.Contains(t, output.String(), scenario.want)
			require.NotContains(t, output.String(), "1970-01-01")
		})
	}
	t.Run("Given retained suspension, when the CLI prints JSON, assert that the expiration field is absent", func(t *testing.T) {
		var output bytes.Buffer
		require.NoError(t, writeSandbox(&output, clioutput.FormatJSON, &apiv1.Sandbox{
			State: apiv1.RuntimeState_RUNTIME_STATE_SUSPENDED, Operation: apiv1.RuntimeOperation_RUNTIME_OPERATION_NONE, RetainWhileSuspended: true,
		}))
		var value map[string]any
		require.NoError(t, json.Unmarshal(output.Bytes(), &value))
		require.Equal(t, true, value["retainWhileSuspended"])
		require.NotContains(t, value, "expiresAt")
	})
}

type sandboxCLIServer struct {
	apiv1.UnimplementedSandboxServiceServer
	requests chan *apiv1.CreateSandboxRequest
}

var _ apiv1.SandboxServiceServer = (*sandboxCLIServer)(nil)

func (s *sandboxCLIServer) CreateSandbox(_ context.Context, request *apiv1.CreateSandboxRequest) (*apiv1.CreateSandboxResponse, error) {
	s.requests <- request
	return &apiv1.CreateSandboxResponse{Sandbox: &apiv1.Sandbox{Id: "sandbox-created", State: apiv1.RuntimeState_RUNTIME_STATE_READY}}, nil
}

func sandboxCLIEndpoint(t *testing.T) (context.Context, <-chan *apiv1.CreateSandboxRequest) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	requests := make(chan *apiv1.CreateSandboxRequest, 1)
	apiv1.RegisterSandboxServiceServer(server, &sandboxCLIServer{requests: requests})
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, listener.Close())
		require.NoError(t, <-done)
	})
	ctx := connection.WithDialOptions(t.Context(), []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
	})
	return ctx, requests
}
