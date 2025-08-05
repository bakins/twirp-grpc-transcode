package transcode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/twitchtv/twirp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TwirpHandler transcodes twirp to gRPC
type TwirpHandler struct {
	mu                 sync.Mutex
	next               http.Handler
	methods            map[string]*rpcMethod
	marshaler          protojson.MarshalOptions
	unmarshaler        protojson.UnmarshalOptions
	descriptorResolver DescriptorResolver
}

type rpcMethod struct {
	input  protoreflect.MessageType
	output protoreflect.MessageType
}

type DescriptorResolver interface {
	FindDescriptorByName(protoreflect.FullName) (protoreflect.Descriptor, error)
}

type handlerOptions struct {
	descriptorResolver DescriptorResolver
}

type HandlerOption interface {
	applyHandlerOption(*handlerOptions)
}

type handlerOptionFunc func(*handlerOptions)

func (f handlerOptionFunc) applyHandlerOption(o *handlerOptions) {
	f(o)
}

func NewTwirpHandler(next http.Handler, options ...HandlerOption) (*TwirpHandler, error) {
	opts := handlerOptions{
		descriptorResolver: protoregistry.GlobalFiles,
	}

	for _, o := range options {
		o.applyHandlerOption(&opts)
	}

	h := TwirpHandler{
		next:               next,
		methods:            make(map[string]*rpcMethod),
		descriptorResolver: opts.descriptorResolver,
	}

	return &h, nil
}

// var bufferPool = sync.Pool{
// 	New: func() any {
// 		return bufferpool.NewBuffer(nil)
// 	},
// }

// func getBuffer() *bufferpool.Buffer {
// 	b := bufferPool.Get().(*bufferpool.Buffer)
// 	b.Reset()

// 	return b
// }

// func releaseBuffer(b *bufferpool.Buffer) {
// 	bufferPool.Put(b)
// }

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

	input := &bytes.Buffer{}

	if r.ContentLength > 0 {
		input.Grow(int(r.ContentLength))
	}

	if _, err := io.Copy(input, r.Body); err != nil {
		twerr := twirp.InternalErrorWith(fmt.Errorf("failed to read request %w", err))
		h.writeError(w, twerr)
		return
	}

	rpcName := "/" + strings.TrimPrefix(r.URL.Path, "/twirp/")

	if isJSON {
		m, err := h.getRPCMethod(rpcName)
		if err != nil {
			twerr := twirp.Unimplemented.Errorf("transcoding not available for %s", rpcName)
			twerr = twirp.WrapError(twerr, err)
			h.writeError(w, twerr)
			return
		}

		p, err := h.jsonToProto(m.input, input.Bytes())
		if err != nil {
			twerr := twirp.InvalidArgument.Errorf("the request could not be decoded %s", err.Error())
			h.writeError(w, twerr)
			return
		}

		input = bytes.NewBuffer(p)
	}

	input, err := addFraming(input)
	if err != nil {
		twerr := twirp.InternalErrorWith(fmt.Errorf("failed to prepare request %w", err))
		h.writeError(w, twerr)
		return
	}

	r = r.Clone(r.Context())
	r.URL.Path = rpcName
	r.ProtoMajor = 2
	r.ProtoMinor = 0
	r.ContentLength = int64(input.Len())
	r.Header.Del("Content-Length")
	r.Header.Set("Content-Type", "application/grpc")
	// need to remove any other grpc related headers?
	// set grpc-timeout?
	r.Body = io.NopCloser(input)

	capture := newResponseWriter()

	h.next.ServeHTTP(capture, r)

	if statusCode := capture.getStatus(); statusCode != http.StatusOK {
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

	// we can't pool output reliably, as middleware may hold onto references to response body.
	output, err := removeFraming(capture.body, capture.headers)
	if err != nil {
		twerr := twirp.InternalErrorWith(fmt.Errorf("failed to remove grpc message framing %w", err))
		h.writeError(w, twerr)
		return
	}

	if isJSON {
		// we already checked above, so this should never fail
		m, err := h.getRPCMethod(rpcName)
		if err != nil {
			twerr := twirp.Unimplemented.Errorf("transcoding not available for %s", r.URL.Path)
			twerr = twirp.WrapError(twerr, err)
			h.writeError(w, twerr)
			return
		}

		j, err := h.protoToJSON(m.output, output.Bytes())
		if err != nil {
			twerr := twirp.InternalErrorWith(fmt.Errorf("failed to decode response %w", err))
			h.writeError(w, twerr)
			return
		}

		output = bytes.NewBuffer(j)
	}

	for k, vv := range capture.headers {
		switch k {
		case "Grpc-Status", "Grpc-Message", "Grpc-Encoding", "Grpc-Status-Details-Bin", "Content-Type", "Trailer":
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

	_, _ = io.Copy(w, output)
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

func addFraming(in *bytes.Buffer) (*bytes.Buffer, error) {
	l := in.Len()

	out := &bytes.Buffer{}
	out.Grow(5 + l)

	prefix := []byte{0, 0, 0, 0, 0}

	binary.BigEndian.PutUint32(prefix[1:], uint32(l))

	if _, err := out.Write(prefix); err != nil {
		return nil, err
	}

	if _, err := io.Copy(out, in); err != nil {
		return nil, err
	}

	return out, nil
}

func removeFraming(r *bytes.Buffer, headers http.Header) (*bytes.Buffer, error) {
	prefix := []byte{0, 0, 0, 0, 0}

	if n, err := io.ReadFull(r, prefix); err != nil {
		// empty body is valid for proto
		if n == 0 && err == io.EOF {
			return nil, nil
		}

		return nil, err
	}

	length := binary.BigEndian.Uint32(prefix[1:])

	// TODO: check for too large of a message?
	out := &bytes.Buffer{}

	out.Grow(int(length))

	if _, err := io.CopyN(out, r, int64(length)); err != nil {
		return nil, err
	}

	if prefix[0] == 0 {
		return out, nil
	}

	name := headers.Get("Grpc-Encoding")
	if name == "" {
		return nil, errors.New("compression flag set but no Grpc-Encoding header found")
	}

	compressor := encoding.GetCompressor(name)
	if compressor == nil {
		return nil, fmt.Errorf("compressor %s is not supported", name)
	}

	rdr, err := compressor.Decompress(out)
	if err != nil {
		return nil, err
	}

	out.Reset()
	if _, err := io.Copy(out, rdr); err != nil {
		return nil, err
	}

	return out, nil
}

func newResponseWriter() *responseWriter {
	w := responseWriter{
		headers: make(http.Header, 3),
		body:    &bytes.Buffer{},
	}

	return &w
}

type responseWriter struct {
	body    *bytes.Buffer
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

func (h *TwirpHandler) getRPCMethod(name string) (*rpcMethod, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	existing, ok := h.methods[name]
	if ok {
		return existing, nil
	}

	parts := strings.SplitN(name, "/", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid rpc path %s", name)
	}

	serviceName := parts[1]
	methodName := parts[2]

	desc, err := h.descriptorResolver.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return nil, fmt.Errorf("unable to find descriptor for service %s %w", serviceName, err)
	}

	service, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, fmt.Errorf("unexpected descriptor type found for service %s %T", serviceName, desc)
	}

	method := service.Methods().ByName(protoreflect.Name(methodName))
	if method == nil {
		return nil, fmt.Errorf("unable to find method %s for service %s", methodName, serviceName)
	}

	r := &rpcMethod{
		input:  dynamicpb.NewMessageType(method.Input()),
		output: dynamicpb.NewMessageType(method.Output()),
	}

	h.methods[name] = r

	return r, nil
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

/*
type grpcTranscodeHandler struct {
	inputType  protoreflect.MessageType
	outputType protoreflect.MessageType
	client     *connect.Client[bytes.Buffer, bytes.Buffer]
	next       http.Client
}

// need to create a client that just forwards to next handler

func (g *grpcTranscodeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO: pool the buffers?
	input := bytes.NewBuffer([]byte{})

	if r.ContentLength > 0 {
		input.Grow(int(r.ContentLength))
	}

	if _, err := io.Copy(input, r.Body); err != nil {
		///
	}

	// TODO: handle json->protobuf

	request := connect.NewRequest(input)
	response, err := g.client.CallUnary(
		r.Context(),
		request,
	)
	if err != nil {
		// convert connect client errors to twirp errors
	}

	// clean this up
	for k, vv := range response.Header() {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	w.WriteHeader(http.StatusOK)

	// TODO: json if needed

	_, _ = io.Copy(w, response.Msg)
}

type bufferCodec struct{}

func (bufferCodec) Name() string {
	return "proto"
}

func (bufferCodec) Marshal(input any) ([]byte, error) {
	b, ok := input.(*bytes.Buffer)
	if ok {
		return b.Bytes(), nil
	}

	p, ok := input.(proto.Message)
	if ok {
		return proto.Marshal(p)
	}

	return nil, fmt.Errorf("unsupported marshal type %T", input)
}

func (bufferCodec) Unmarshal(data []byte, input any) error {
	b, ok := input.(*bytes.Buffer)
	if ok {
		_, err := b.Write(data)
		return err
	}

	p, ok := input.(proto.Message)
	if ok {
		return proto.Unmarshal(data, p)
	}

	return fmt.Errorf("unsupported unmarshal type %T", input)
}

*/
