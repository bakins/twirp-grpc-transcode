package transcode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/twitchtv/twirp"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type TwirpHandler struct {
	next               http.Handler
	mu                 sync.Mutex
	methods            map[string]map[string]*rpcMethod
	marshaler          protojson.MarshalOptions
	unmarshaler        protojson.UnmarshalOptions
	descriptorResolver DescriptorResolver
	singleflightGroup  *singleflight.Group
}

type DescriptorResolver interface {
	FindDescriptorByName(protoreflect.FullName) (protoreflect.Descriptor, error)
}

type rpcMethod struct {
	input  protoreflect.MessageType
	output protoreflect.MessageType
}

type twirpHandlerOptions struct {
	descriptorResolver DescriptorResolver
}

type TwirpHandlerOption interface {
	applyHandlerOption(*twirpHandlerOptions)
}

type twirpHandlerOptionFunc func(*twirpHandlerOptions)

func (f twirpHandlerOptionFunc) applyHandlerOption(o *twirpHandlerOptions) {
	f(o)
}

func WithDescriptorResolver(r DescriptorResolver) TwirpHandlerOption {
	return twirpHandlerOptionFunc(func(o *twirpHandlerOptions) {
		o.descriptorResolver = r
	})
}

// NewTwirpHandler creates a TwirpHandler that transcodes twirp to grpc.
func NewTwirpHandler(next http.Handler, options ...TwirpHandlerOption) (*TwirpHandler, error) {
	opts := twirpHandlerOptions{
		descriptorResolver: protoregistry.GlobalFiles,
	}

	for _, o := range options {
		o.applyHandlerOption(&opts)
	}

	h := TwirpHandler{
		next:               next,
		methods:            make(map[string]map[string]*rpcMethod),
		descriptorResolver: opts.descriptorResolver,
		singleflightGroup:  &singleflight.Group{},
	}

	return &h, nil
}

func (h *TwirpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var isJSON bool
	ct := strings.ToLower(r.Header.Get("Content-Type"))

	switch {
	case strings.HasPrefix(ct, "application/grpc"):
		// passthrough
		h.next.ServeHTTP(w, r)
		return
	case strings.HasPrefix(ct, "application/json"):
		isJSON = true
	case strings.HasPrefix(ct, "application/protobuf"):
	default:
		http.Error(w, "unsupported content-type", http.StatusBadRequest)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		twerr := twirp.InternalErrorWith(fmt.Errorf("failed to read request %w", err))
		h.writeError(w, twerr)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/twirp")

	if isJSON {
		m, err := h.getRPCMethod(path)
		if err != nil {
			twerr := twirp.Unimplemented.Errorf("transcoding not available for %s", r.URL.Path)
			h.writeError(w, twerr)
			return
		}

		p, err := h.jsonToProto(m.input, data)
		if err != nil {
			twerr := twirp.InvalidArgument.Errorf("the request could not be decoded %s", err.Error())
			h.writeError(w, twerr)
			return
		}

		data = p
	}

	data = addFraming(data)

	r = r.Clone(r.Context())
	r.URL.Path = path
	r.ProtoMajor = 2
	r.ProtoMinor = 0
	r.ContentLength = int64(len(data))
	r.Header.Del("Content-Length")
	r.Header.Set("Content-Type", "application/grpc")
	r.Body = io.NopCloser(bytes.NewReader(data))

	capture := newResponseWriter()

	h.next.ServeHTTP(capture, r)

	if statusCode := capture.getStatus(); statusCode != http.StatusOK {
		fmt.Println(capture.body.String())
		twerr := twirp.Internal.Errorf("error from intermediary with HTTP status code %d", statusCode)
		h.writeError(w, twerr)
		return
	}

	if contentType := strings.ToLower(capture.headers.Get("Content-Type")); !strings.HasPrefix(contentType, "application/grpc") {
		twerr := twirp.Internal.Errorf("unexpected content-type returned %s", contentType)
		h.writeError(w, twerr)
		return
	}

	status, err := getTwirpError(capture.headers)
	if err != nil {
		twerr := twirp.InternalErrorWith(fmt.Errorf("failed to get grpc-status %w", err))
		h.writeError(w, twerr)
		return
	}

	if status.Code() != twirp.NoError {
		h.writeError(w, status)
		return
	}

	output, err := removeFraming(&capture.body)
	if err != nil {
		twerr := twirp.InternalErrorWith(fmt.Errorf("failed to remove grpc message framing %w", err))
		h.writeError(w, twerr)
		return
	}

	if isJSON {
		// we already checked above, so this should always work
		m, err := h.getRPCMethod(path)
		if err != nil {
			twerr := twirp.Unimplemented.Errorf("transcoding not available for %s", r.URL.Path)
			h.writeError(w, twerr)
			return
		}

		j, err := h.protoToJSON(m.output, output)
		if err != nil {
			twerr := twirp.InternalErrorWith(fmt.Errorf("failed to decode response %w", err))
			h.writeError(w, twerr)
			return
		}

		output = j
	}

	for k, vv := range capture.headers {
		switch k {
		case "Grpc-Status", "Grpc-Message", "Grpc-Status-Details-Bin", "Content-Type", "Trailer":
			// skip
		default:
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
	}

	if isJSON {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "application/protobuf")
	}

	_, _ = w.Write(output)
}

func grpcStatusToTwirpCode(grpcCode int) twirp.ErrorCode {
	switch codes.Code(grpcCode) {
	case codes.OK:
		return twirp.NoError
	case codes.Canceled:
		return twirp.Canceled
	case codes.Unknown:
		return twirp.Unknown
	case codes.InvalidArgument:
		return twirp.InvalidArgument
	case codes.DeadlineExceeded:
		return twirp.DeadlineExceeded
	case codes.NotFound:
		return twirp.NotFound
	case codes.AlreadyExists:
		return twirp.AlreadyExists
	case codes.PermissionDenied:
		return twirp.PermissionDenied
	case codes.Unauthenticated:
		return twirp.Unauthenticated
	case codes.ResourceExhausted:
		return twirp.ResourceExhausted
	case codes.FailedPrecondition:
		return twirp.FailedPrecondition
	case codes.Aborted:
		return twirp.Aborted
	case codes.OutOfRange:
		return twirp.OutOfRange
	case codes.Unimplemented:
		return twirp.Unimplemented
	case codes.Internal:
		return twirp.Internal
	case codes.Unavailable:
		return twirp.Unavailable
	case codes.DataLoss:
		return twirp.DataLoss
	default:
		return twirp.Internal
	}
}

func getTwirpError(headers http.Header) (twirp.Error, error) {
	gs := headers.Get("Grpc-Status")
	if gs == "" {
		// assume ok - is this reasonable?
		gs = "0"
	}

	num, err := strconv.Atoi(gs)
	if err != nil {
		return nil, fmt.Errorf("invalid grpc-status found %s", gs)
	}

	msg := headers.Get("Grpc-Message")
	if msg != "" {
		msg = decodeGrpcMessage(msg)
	} else {
		msg = codes.Code(num).String()
	}

	twerr := twirp.NewError(grpcStatusToTwirpCode(num), msg)

	if details := headers.Get("Grpc-Status-Details-Bin"); details != "" {
		twerr = twerr.WithMeta("grpc-status-details-bin", details)
	}

	return twerr, nil
}

func decodeGrpcMessage(msg string) string {
	if msg == "" {
		return ""
	}
	lenMsg := len(msg)
	for i := 0; i < lenMsg; i++ {
		if msg[i] == percentByte && i+2 < lenMsg {
			return decodeGrpcMessageUnchecked(msg)
		}
	}
	return msg
}

const (
	percentByte = '%'
)

func decodeGrpcMessageUnchecked(msg string) string {
	var buf bytes.Buffer
	lenMsg := len(msg)
	for i := 0; i < lenMsg; i++ {
		c := msg[i]
		if c == percentByte && i+2 < lenMsg {
			parsed, err := strconv.ParseUint(msg[i+1:i+3], 16, 8)
			if err != nil {
				buf.WriteByte(c)
			} else {
				buf.WriteByte(byte(parsed))
				i += 2
			}
		} else {
			buf.WriteByte(c)
		}
	}
	return buf.String()
}

func addFraming(in []byte) []byte {
	l := len(in)
	out := make([]byte, 5+l)
	binary.BigEndian.PutUint32(out[1:], uint32(l))

	if l > 0 {
		copy(out[5:], in)
	}

	return out
}

func removeFraming(r io.Reader) ([]byte, error) {
	prefix := []byte{0, 0, 0, 0, 0}

	if n, err := io.ReadFull(r, prefix); err != nil {
		// empty body is valid for proto
		if n == 0 && err == io.EOF {
			return []byte{}, nil
		}

		return nil, err
	}

	length := binary.BigEndian.Uint32(prefix[1:])

	// TODO: check for too large of a message?
	out := make([]byte, length)

	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}

	return out, nil
}

func newResponseWriter() *responseWriter {
	w := responseWriter{
		headers: make(http.Header, 3),
	}

	return &w
}

type responseWriter struct {
	body    bytes.Buffer
	headers http.Header
	status  int
}

func (w *responseWriter) Header() http.Header {
	return w.headers
}

func (w *responseWriter) Flush() {
}

func (w *responseWriter) getStatus() int {
	if w.status != 0 {
		return w.status
	}

	return http.StatusOK
}

func (w *responseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}

	return w.body.Write(data)
}

func (w *responseWriter) WriteHeader(statusCode int) {
	if w.status != 0 {
		return
	}

	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	w.status = statusCode
}

func (h *TwirpHandler) jsonToProto(t protoreflect.MessageType, data []byte) ([]byte, error) {
	msg := t.New().Interface()

	if err := h.unmarshaler.Unmarshal(data, msg); err != nil {
		return nil, err
	}

	return proto.Marshal(msg)
}

func (h *TwirpHandler) protoToJSON(t protoreflect.MessageType, data []byte) ([]byte, error) {
	msg := t.New().Interface()

	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}

	return h.marshaler.Marshal(msg)
}

func (h *TwirpHandler) writeError(w http.ResponseWriter, twerr twirp.Error) {
	respBody := h.marshalErrorToJSON(twerr)

	w.Header().Set("Content-Type", "application/json") // Error responses are always JSON
	w.Header().Set("Content-Length", strconv.Itoa(len(respBody)))

	statusCode := twirp.ServerHTTPStatusFromErrorCode(twerr.Code())

	w.WriteHeader(statusCode) // set HTTP status code and send response

	_, _ = w.Write(respBody)
}

type twerrJSON struct {
	Meta map[string]string `json:"meta,omitempty"`
	Code string            `json:"code"`
	Msg  string            `json:"msg"`
}

func (h *TwirpHandler) marshalErrorToJSON(twerr twirp.Error) []byte {
	msg := twerr.Msg()
	if len(msg) > 1e6 {
		msg = msg[:1e6]
	}

	tj := twerrJSON{
		Code: string(twerr.Code()),
		Msg:  msg,
		Meta: twerr.MetaMap(),
	}

	buf, err := json.Marshal(&tj)
	if err != nil {
		buf = []byte("{\"type\": \"" + twirp.Internal + "\", \"msg\": \"There was an error but it could not be serialized into JSON\"}") // fallback
	}

	return buf
}

func (h *TwirpHandler) getRPCMethod(name string) (*rpcMethod, error) {
	parts := strings.SplitN(name, "/", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid rpc path %s", name)
	}

	serviceName := parts[1]
	methodName := parts[2]

	h.mu.Lock()
	service, ok := h.methods[name]
	h.mu.Unlock()

	if !ok {
		var err error
		service, err = h.loadService(serviceName)
		if err != nil {
			return nil, err
		}
	}

	method, ok := service[methodName]
	if !ok {
		return nil, fmt.Errorf("unable to find method %s for service %s", methodName, serviceName)
	}

	return method, nil
}

// this assumes service descriptors do not change during lifetime of handler
func (h *TwirpHandler) loadService(serviceName string) (map[string]*rpcMethod, error) {
	// only do one lookup per service at a time
	res, err, _ := h.singleflightGroup.Do(serviceName, func() (any, error) {
		desc, err := h.descriptorResolver.FindDescriptorByName(protoreflect.FullName(serviceName))
		if err != nil {
			return nil, fmt.Errorf("unable to find descriptor for service %s %w", serviceName, err)
		}

		serviceDescriptor, ok := desc.(protoreflect.ServiceDescriptor)
		if !ok {
			return nil, fmt.Errorf("unexpected descriptor type found for service %s %T", serviceName, desc)
		}

		methods := serviceDescriptor.Methods()

		service := make(map[string]*rpcMethod, methods.Len())

		h.methods[serviceName] = service

		for i := 0; i < methods.Len(); i++ {
			method := methods.Get(i)

			r := &rpcMethod{
				input:  dynamicpb.NewMessageType(method.Input()),
				output: dynamicpb.NewMessageType(method.Output()),
			}

			h.mu.Lock()
			service[string(method.Name())] = r
			h.mu.Unlock()
		}

		return service, nil
	})

	if err != nil {
		return nil, err
	}

	return res.(map[string]*rpcMethod), nil
}
