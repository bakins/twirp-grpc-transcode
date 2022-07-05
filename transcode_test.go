package transcode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

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
	/*
		t.Run("protobuf request", func(t *testing.T) {
			client := &http.Client{
				Transport: &http2.Transport{
					AllowHTTP: true,
					DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
						return net.Dial(network, addr)
					},
				},
			}

			data, err := proto.Marshal(&pb.HelloRequest{Name: "world"})
			require.NoError(t, err)

			req, err := http.NewRequest(http.MethodPost, svr.URL+"/helloworld.Greeter/SayHello", bytes.NewReader(data))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/protobuf")

			resp, err := client.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			data, err = ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "application/protobuf", resp.Header.Get("Content-Type"))

			var reply pb.HelloReply
			err = proto.Unmarshal(data, &reply)
			require.NoError(t, err)
			require.Equal(t, "hello world", reply.Message)
		})

		t.Run("protobuf error", func(t *testing.T) {
			client := &http.Client{
				Transport: &http2.Transport{
					AllowHTTP: true,
					DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
						return net.Dial(network, addr)
					},
				},
			}

			data, err := proto.Marshal(&pb.HelloRequest{Name: "universe"})
			require.NoError(t, err)

			req, err := http.NewRequest(http.MethodPost, svr.URL+"/helloworld.Greeter/SayHello", bytes.NewReader(data))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/protobuf")

			resp, err := client.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			require.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)
			require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

			data, err = ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			var status spb.Status
			err = protojson.Unmarshal(data, &status)
			require.NoError(t, err)

			require.Equal(t, int32(codes.FailedPrecondition), status.Code)
			require.Equal(t, "cannot do that", status.Message)
			require.Len(t, status.Details, 1)

			var info errdetails.RequestInfo
			err = anypb.UnmarshalTo(status.Details[0], &info, newProto.UnmarshalOptions{})
			require.NoError(t, err)
			require.Equal(t, "42", info.RequestId)
		})

	*/

	t.Run("json request", func(t *testing.T) {
		client := pb.NewGreeterJSONClient(svr.URL, http.DefaultClient)

		resp, err := client.SayHello(context.Background(), &pb.HelloRequest{Name: "world"})
		require.NoError(t, err)

		require.Equal(t, "hello world", resp.Message)
	})

	/*
		t.Run("json error", func(t *testing.T) {
			client := &http.Client{
				Transport: &http2.Transport{
					AllowHTTP: true,
					DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
						return net.Dial(network, addr)
					},
				},
			}

			data, err := protojson.Marshal(&pb.HelloRequest{Name: "universe"})
			require.NoError(t, err)

			req, err := http.NewRequest(http.MethodPost, svr.URL+"/helloworld.Greeter/SayHello", bytes.NewReader(data))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			require.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)
			require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

			data, err = ioutil.ReadAll(resp.Body)
			require.NoError(t, err)

			var status spb.Status
			err = protojson.Unmarshal(data, &status)
			require.NoError(t, err)

			require.Equal(t, int32(codes.FailedPrecondition), status.Code)
			require.Equal(t, "cannot do that", status.Message)
			require.Len(t, status.Details, 1)

			var info errdetails.RequestInfo
			err = anypb.UnmarshalTo(status.Details[0], &info, newProto.UnmarshalOptions{})
			require.NoError(t, err)
			require.Equal(t, "42", info.RequestId)
		})
	*/
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
