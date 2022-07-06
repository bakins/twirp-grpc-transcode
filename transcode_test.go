package transcode

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twitchtv/twirp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	pb "github.com/bakins/grpc-twirp-transcode/testdata"
)

// to recreate descriptor set, cd to testdata and run
// protoc -I . --include_imports --include_source_info -o helloworld.bin helloworld.proto
// protoc -I .  --go-grpc_out=. --go-grpc_opt=paths=source_relative --go_out=. --go_opt=paths=source_relative  --twirp_opt=paths=source_relative  --twirp_out=. helloworld.proto
func TestHandler(t *testing.T) {
	g := grpc.NewServer()
	pb.RegisterGreeterServer(g, &server{})

	h, err := New(filepath.Join("testdata", "helloworld.bin"), g)
	require.NoError(t, err)

	svr := httptest.NewServer(h2c.NewHandler(h, &http2.Server{}))
	defer svr.Close()

	t.Run("grpc passthrough", func(t *testing.T) {
		// passthrough grpc
		conn, err := grpc.Dial(strings.TrimPrefix(svr.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)

		defer conn.Close()

		client := pb.NewGreeterClient(conn)

		resp, err := client.SayHello(context.Background(), &pb.HelloRequest{Name: "world"})
		require.NoError(t, err)
		require.Equal(t, "hello world", resp.Message)

		_, err = client.SayHello(context.Background(), &pb.HelloRequest{Name: "universe"})
		require.Error(t, err)

		st, ok := status.FromError(err)
		require.True(t, ok)

		require.Equal(t, codes.FailedPrecondition, st.Code())
		require.Equal(t, "cannot do that", st.Message())

		details := st.Details()
		require.Len(t, details, 1)

		info, ok := details[0].(*errdetails.RequestInfo)
		require.True(t, ok)
		require.Equal(t, "42", info.RequestId)
	})

	t.Run("protobuf request", func(t *testing.T) {
		client := pb.NewGreeterProtobufClient(svr.URL, http.DefaultClient)

		resp, err := client.SayHello(context.Background(), &pb.HelloRequest{Name: "world"})
		require.NoError(t, err)

		require.Equal(t, "hello world", resp.Message)
	})

	t.Run("protobuf error", func(t *testing.T) {
		client := pb.NewGreeterProtobufClient(svr.URL, http.DefaultClient)

		_, err := client.SayHello(context.Background(), &pb.HelloRequest{Name: "universe"})
		require.Error(t, err)

		twerr, ok := err.(twirp.Error)
		require.True(t, ok)

		require.Equal(t, twirp.FailedPrecondition, twerr.Code())

		details := twerr.Meta("grpc-status-details-bin")
		require.NotEmpty(t, details)

		data, err := decodeBinHeader(details)
		require.NoError(t, err)

		var status spb.Status
		err = proto.Unmarshal(data, &status)
		require.NoError(t, err)

		require.Equal(t, int32(codes.FailedPrecondition), status.Code)
		require.Equal(t, "cannot do that", status.Message)
		require.Len(t, status.Details, 1)

		var info errdetails.RequestInfo
		err = anypb.UnmarshalTo(status.Details[0], &info, proto.UnmarshalOptions{})
		require.NoError(t, err)
		require.Equal(t, "42", info.RequestId)
	})

	t.Run("json request", func(t *testing.T) {
		client := pb.NewGreeterJSONClient(svr.URL, http.DefaultClient)

		resp, err := client.SayHello(context.Background(), &pb.HelloRequest{Name: "world"})
		require.NoError(t, err)

		require.Equal(t, "hello world", resp.Message)
	})

	t.Run("json error", func(t *testing.T) {
		client := pb.NewGreeterJSONClient(svr.URL, http.DefaultClient)

		_, err := client.SayHello(context.Background(), &pb.HelloRequest{Name: "universe"})
		require.Error(t, err)

		twerr, ok := err.(twirp.Error)
		require.True(t, ok)

		require.Equal(t, twirp.FailedPrecondition, twerr.Code())

		details := twerr.Meta("grpc-status-details-bin")
		require.NotEmpty(t, details)

		data, err := decodeBinHeader(details)
		require.NoError(t, err)

		var status spb.Status
		err = proto.Unmarshal(data, &status)
		require.NoError(t, err)

		require.Equal(t, int32(codes.FailedPrecondition), status.Code)
		require.Equal(t, "cannot do that", status.Message)
		require.Len(t, status.Details, 1)

		var info errdetails.RequestInfo
		err = anypb.UnmarshalTo(status.Details[0], &info, proto.UnmarshalOptions{})
		require.NoError(t, err)
		require.Equal(t, "42", info.RequestId)
	})
}

func decodeBinHeader(v string) ([]byte, error) {
	if len(v)%4 == 0 {
		// Input was padded, or padding was not necessary.
		return base64.StdEncoding.DecodeString(v)
	}
	return base64.RawStdEncoding.DecodeString(v)
}

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	if in.Name == "universe" {
		st := status.New(codes.FailedPrecondition, "cannot do that")
		info := errdetails.RequestInfo{
			RequestId: "42",
		}

		st, err := st.WithDetails(&info)
		if err != nil {
			return nil, err
		}

		return nil, st.Err()
	}
	return &pb.HelloReply{Message: "hello " + in.GetName()}, nil
}
