package transcode

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// GrpcHandler transcodes grpc to twirp
type GrpcHandler struct {
	mu                 sync.Mutex
	next               http.Handler
	methods            map[string]*rpcMethod
	descriptorResolver DescriptorResolver
}

func NewGrpcHandler(next http.Handler, options ...HandlerOption) (*GrpcHandler, error) {
	opts := handlerOptions{
		descriptorResolver: protoregistry.GlobalFiles,
	}

	for _, o := range options {
		o.applyHandlerOption(&opts)
	}

	h := GrpcHandler{
		next:               next,
		methods:            make(map[string]*rpcMethod),
		descriptorResolver: opts.descriptorResolver,
	}

	return &h, nil
}

func (h *GrpcHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		h.next.ServeHTTP(w, r)
		return
	}

	input := &bytes.Buffer{}

	if r.ContentLength > 0 {
		input.Grow(int(r.ContentLength))
	}

	if _, err := io.Copy(input, r.Body); err != nil {
		h.writeError(w, fmt.Errorf("failed to read request %w", err))
		return
	}

	input, err := removeFraming(input, r.Header)
	if err != nil {
		h.writeError(w, fmt.Errorf("failed to remove grpc message framing %w", err))
		return
	}

	r = r.Clone(r.Context())
	r.URL.Path = "/twirp" + r.URL.Path
	r.ContentLength = int64(input.Len())
	r.Header.Del("Content-Length")
	r.Header.Set("Content-Type", "application/protobuf")
	// remove any other headers?
	r.Body = io.NopCloser(input)

	// TODO: "better" version of this
	responseCapture := httptest.NewRecorder()

	h.next.ServeHTTP(responseCapture, r)

	resp := responseCapture.Result()
	// parse non-200
	// add framing as needed
}

func statusFromError(err error) (s *status.Status) {
	if err == nil {
		return status.New(codes.OK, codes.OK.String())
	}
	if se, ok := err.(interface {
		GRPCStatus() *status.Status
	}); ok {
		return se.GRPCStatus()
	}
	return status.New(codes.Internal, err.Error())
}

func (h *GrpcHandler) writeError(w http.ResponseWriter, err error) {
	st := statusFromError(err)
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Trailer", "grpc-status, grpc-message")
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Grpc-Status", strconv.Itoa(int(st.Code())))

	msg := st.Message()
	if msg == "" {
		msg = st.Code().String()
	}

	w.Header().Set("Grpc-Message", encodeGrpcMessage(msg))

	// todo, status details?
}

const (
	spaceByte = ' '
	tildeByte = '~'
)

func encodeGrpcMessage(msg string) string {
	if msg == "" {
		return ""
	}
	lenMsg := len(msg)
	for i := 0; i < lenMsg; i++ {
		c := msg[i]
		if !(c >= spaceByte && c <= tildeByte && c != percentByte) {
			return encodeGrpcMessageUnchecked(msg)
		}
	}
	return msg
}

func encodeGrpcMessageUnchecked(msg string) string {
	var sb strings.Builder
	for len(msg) > 0 {
		r, size := utf8.DecodeRuneInString(msg)
		for _, b := range []byte(string(r)) {
			if size > 1 {
				// If size > 1, r is not ascii. Always do percent encoding.
				fmt.Fprintf(&sb, "%%%02X", b)
				continue
			}

			// The for loop is necessary even if size == 1. r could be
			// utf8.RuneError.
			//
			// fmt.Sprintf("%%%02X", utf8.RuneError) gives "%FFFD".
			if b >= spaceByte && b <= tildeByte && b != percentByte {
				sb.WriteByte(b)
			} else {
				fmt.Fprintf(&sb, "%%%02X", b)
			}
		}
		msg = msg[size:]
	}
	return sb.String()
}
