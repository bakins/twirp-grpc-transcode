package transcode

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	next        http.Handler
	factory     *dynamic.MessageFactory
	methods     map[string]*rpcMethod
	marshaler   *jsonpb.Marshaler
	unmarshaler *jsonpb.Unmarshaler
}

type rpcMethod struct {
	input  *desc.MessageDescriptor
	output *desc.MessageDescriptor
}

func New(filename string, next http.Handler) (*Handler, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var fds dpb.FileDescriptorSet

	if err := proto.Unmarshal(data, &fds); err != nil {
		return nil, err
	}

	descriptors, err := desc.CreateFileDescriptorsFromSet(&fds)
	if err != nil {
		return nil, err
	}

	registry := dynamic.NewKnownTypeRegistryWithDefaults()
	ext := dynamic.NewExtensionRegistryWithDefaults()

	h := Handler{
		methods: make(map[string]*rpcMethod),
		factory: dynamic.NewMessageFactoryWithRegistries(ext, registry),
		next:    next,
	}

	var resolverDesc []*desc.FileDescriptor

	for _, d := range descriptors {
		resolverDesc = append(resolverDesc, d)

		for _, s := range d.GetServices() {
			for _, m := range s.GetMethods() {
				registry.AddKnownType(m.GetInputType().AsProto(), m.GetOutputType().AsProto())

				// full http post path
				key := "/" + d.GetPackage() + "." + s.GetName() + "/" + m.GetName()

				r := rpcMethod{
					input:  m.GetInputType(),
					output: m.GetOutputType(),
				}

				h.methods[key] = &r
			}
		}
	}

	resolver := dynamic.AnyResolver(h.factory, resolverDesc...)

	h.marshaler = &jsonpb.Marshaler{
		AnyResolver: resolver,
	}

	h.unmarshaler = &jsonpb.Unmarshaler{
		AnyResolver: resolver,
	}

	return &h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var isJson bool
	ct := strings.ToLower(r.Header.Get("Content-Type"))

	switch {
	case strings.HasPrefix(ct, "application/grpc"):
		// passthrough
		h.next.ServeHTTP(w, r)
		return
	case strings.HasPrefix(ct, "application/json"):
		isJson = true
	case strings.HasPrefix(ct, "application/protobuf"):
	default:
		http.Error(w, "unsupported content-type", http.StatusBadRequest)
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		st := status.New(codes.Internal, fmt.Sprintf("failed to read request %s", err.Error()))
		h.writeError(w, st)
		return
	}

	if isJson {
		m, ok := h.methods[r.URL.Path]
		if !ok {
			st := status.New(
				codes.Unimplemented,
				fmt.Sprintf("transcoding not availible for %s", r.URL.Path),
			)
			h.writeError(w, st)
			return
		}

		p, err := h.jsonToProto(m.input, data)
		if err != nil {
			st := status.New(
				codes.InvalidArgument,
				fmt.Sprintf("the request could not be decoded %s", err.Error()),
			)
			h.writeError(w, st)
			return
		}

		data = p
	}

	data = addFraming(data)

	r = r.Clone(r.Context())
	r.ContentLength = int64(len(data))
	r.Header.Del("Content-Length")
	r.Header.Set("Content-Type", "application/grpc")
	r.Body = ioutil.NopCloser(bytes.NewReader(data))

	capture := newResponseWriter()

	h.next.ServeHTTP(capture, r)

	if statusCode := capture.getStatus(); statusCode != http.StatusOK {
		st := status.New(
			codes.Internal,
			fmt.Sprintf("error from intermediary with HTTP status code %d", statusCode),
		)
		h.writeError(w, st)
		return
	}

	if contentType := strings.ToLower(capture.headers.Get("Content-Type")); !strings.HasPrefix(contentType, "application/grpc") {
		st := status.New(
			codes.Internal,
			fmt.Sprintf("unexpected content-type returned %s", contentType),
		)
		h.writeError(w, st)
		return
	}

	grpcStatus, err := getGrpcStatus(capture.headers)
	if err != nil {
		st := status.New(
			codes.Internal,
			fmt.Sprintf("failed to get grpc-status %s", err.Error()),
		)
		h.writeError(w, st)
		return
	}

	if grpcStatus.Code() != codes.OK {
		h.writeError(w, grpcStatus)
		return
	}

	output, err := removeFraming(&capture.body)
	if err != nil {
		st := status.New(
			codes.Internal,
			fmt.Sprintf("failed to remove grpc message framing %s", err.Error()),
		)
		h.writeError(w, st)
		return
	}

	if isJson {
		// we already checked above
		m, ok := h.methods[r.URL.Path]
		if !ok {
			st := status.New(
				codes.Unimplemented,
				fmt.Sprintf("transcoding not availible for %s", r.URL.Path),
			)
			h.writeError(w, st)
			return
		}

		j, err := h.protoToJson(m.output, output)
		if err != nil {
			st := status.New(
				codes.Internal,
				fmt.Sprintf("failed to decode response %s", err.Error()),
			)
			h.writeError(w, st)
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

	if isJson {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "application/protobuf")
	}

	_, _ = w.Write(output)
}

// based on twirp's mapping:
func grpcToStatusCode(code codes.Code) int {
	switch code {
	case codes.Canceled:
		return 408 // RequestTimeout
	case codes.Unknown:
		return 500 // Internal Server Error
	case codes.InvalidArgument:
		return 400 // BadRequest
	case codes.DeadlineExceeded:
		return 408 // RequestTimeout
	case codes.NotFound:
		return 404 // Not Found
	case codes.AlreadyExists:
		return 409 // Conflict
	case codes.PermissionDenied:
		return 403 // Forbidden
	case codes.Unauthenticated:
		return 401 // Unauthorized
	case codes.ResourceExhausted:
		return 429 // Too Many Requests
	case codes.FailedPrecondition:
		return 412 // Precondition Failed
	case codes.Aborted:
		return 409 // Conflict
	case codes.OutOfRange:
		return 400 // Bad Request
	case codes.Unimplemented:
		return 501 // Not Implemented
	case codes.Internal:
		return 500 // Internal Server Error
	case codes.Unavailable:
		return 503 // Service Unavailable
	case codes.DataLoss:
		return 500 // Internal Server Error
	case codes.OK:
		return 200 // OK
	default:
		return 500 // Invalid! set to internal
	}
}

func getGrpcStatus(headers http.Header) (*status.Status, error) {
	gs := headers.Get("Grpc-Status")
	if gs == "" {
		// assume ok - is this reasonable?
		gs = "0"
	}

	num, err := strconv.Atoi(gs)
	if err != nil {
		return nil, fmt.Errorf("invalid grpc-status found %s", gs)
	}

	code := codes.Code(num)

	msg := headers.Get("Grpc-Message")
	if msg != "" {
		msg = decodeGrpcMessage(msg)
	} else {
		msg = code.String()
	}

	// TODO: handled detailed-bin status

	details := headers.Get("Grpc-Status-Details-Bin")
	if details == "" {
		return status.New(code, msg), nil
	}

	data, err := decodeBinHeader(details)
	if err != nil {
		// return what we have
		return status.New(code, msg), nil
	}

	var statusProto spb.Status
	if err := proto.Unmarshal(data, &statusProto); err != nil {
		// return what we have
		return status.New(code, msg), nil
	}

	if len(statusProto.Details) == 0 {
		return status.New(code, msg), nil
	}

	s := spb.Status{
		Code:    int32(code),
		Message: msg,
		Details: statusProto.Details,
	}

	return status.FromProto(&s), nil
}

func decodeBinHeader(v string) ([]byte, error) {
	if len(v)%4 == 0 {
		// Input was padded, or padding was not necessary.
		return base64.StdEncoding.DecodeString(v)
	}
	return base64.RawStdEncoding.DecodeString(v)
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
	spaceByte   = ' '
	tildeByte   = '~'
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

func (h *Handler) jsonToProto(m *desc.MessageDescriptor, data []byte) ([]byte, error) {
	msg := h.factory.NewMessage(m)

	if err := h.unmarshaler.Unmarshal(bytes.NewReader(data), msg); err != nil {
		return nil, err
	}

	return proto.Marshal(msg)
}

func (h *Handler) protoToJson(m *desc.MessageDescriptor, data []byte) ([]byte, error) {
	msg := h.factory.NewMessage(m)

	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	if err := h.marshaler.Marshal(&buf, msg); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (h *Handler) writeError(w http.ResponseWriter, st *status.Status) {
	respBody := h.marshalErrorToJSON(st)

	w.Header().Set("Content-Type", "application/json") // Error responses are always JSON
	w.Header().Set("Content-Length", strconv.Itoa(len(respBody)))

	statusCode := grpcToStatusCode(st.Code())

	w.WriteHeader(statusCode) // set HTTP status code and send response

	_, _ = w.Write(respBody)
}

func (h *Handler) marshalErrorToJSON(st *status.Status) []byte {
	var buf bytes.Buffer

	// fmt.Println(st.Proto())

	if err := h.marshaler.Marshal(&buf, st.Proto()); err != nil {
		// internal error
		return []byte(`{"code": 13, "message": "There was an error but it could not be serialized into JSON"}`)
	}

	return buf.Bytes()
}
