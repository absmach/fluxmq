// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	resolveCertificateV2Method = "/atom.v1.CertificateService/ResolveCertificateV2"
	maximumServiceTokenBytes   = 64 * 1024
)

type grpcResolver struct {
	connection       *grpc.ClientConn
	serviceTokenFile string
	request          protoreflect.MessageDescriptor
	response         protoreflect.MessageDescriptor
}

func newGRPCResolver(config Config) (*grpcResolver, error) {
	if _, err := readServiceToken(config.ServiceTokenFile); err != nil {
		return nil, err
	}
	request, response, err := certificateMessageDescriptors()
	if err != nil {
		return nil, err
	}
	var transport credentials.TransportCredentials
	if config.ResolverInsecure {
		transport = insecure.NewCredentials()
	} else {
		transport = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	connection, err := grpc.NewClient(
		config.ResolverAddress,
		grpc.WithTransportCredentials(transport),
	)
	if err != nil {
		return nil, fmt.Errorf("create Atom certificate resolver client: %w", err)
	}
	return &grpcResolver{
		connection:       connection,
		serviceTokenFile: config.ServiceTokenFile,
		request:          request,
		response:         response,
	}, nil
}

func (r *grpcResolver) ResolveCertificateV2(ctx context.Context, request ResolverRequest) (ResolverResult, error) {
	token, err := readServiceToken(r.serviceTokenFile)
	if err != nil {
		return ResolverResult{}, err
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

	input := dynamicpb.NewMessage(r.request)
	setBytes(input, "certificate_der", request.CertificateDER)
	setString(input, "fingerprint_sha256", request.FingerprintSHA256)
	setString(input, "issuer_fingerprint_sha256", request.IssuerFingerprintSHA256)
	setString(input, "serial_number", request.SerialNumber)
	setString(input, "expected_tenant_id", request.ExpectedTenantID)
	output := dynamicpb.NewMessage(r.response)
	if err := r.connection.Invoke(
		ctx,
		resolveCertificateV2Method,
		input,
		output,
		grpc.MaxCallRecvMsgSize(64*1024),
	); err != nil {
		return ResolverResult{}, fmt.Errorf("Atom ResolveCertificateV2: %w", err)
	}
	return ResolverResult{
		EntityID:     getString(output, "entity_id"),
		TenantID:     getString(output, "tenant_id"),
		CredentialID: getString(output, "credential_id"),
		IssuerID:     getString(output, "issuer_id"),
		ExpiresAt:    getString(output, "expires_at"),
		Status:       getString(output, "status"),
	}, nil
}

func (r *grpcResolver) Close() error {
	return r.connection.Close()
}

func readServiceToken(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("open Atom resolver service token: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximumServiceTokenBytes+1))
	if err != nil {
		return "", fmt.Errorf("read Atom resolver service token: %w", err)
	}
	if len(data) == 0 || len(data) > maximumServiceTokenBytes {
		return "", fmt.Errorf("Atom resolver service token is empty or exceeds %d bytes", maximumServiceTokenBytes)
	}
	token := strings.TrimSpace(string(data))
	if token == "" || strings.IndexFunc(token, unicode.IsSpace) >= 0 {
		return "", fmt.Errorf("Atom resolver service token must be one non-empty value")
	}
	return token, nil
}

func certificateMessageDescriptors() (protoreflect.MessageDescriptor, protoreflect.MessageDescriptor, error) {
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	bytesType := descriptorpb.FieldDescriptorProto_TYPE_BYTES
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("atom_certificate_v2_runtime.proto"),
		Package: proto.String("atom.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("ResolveCertificateV2Request"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("certificate_der"), Number: proto.Int32(1), Label: &optional, Type: &bytesType},
					{Name: proto.String("fingerprint_sha256"), Number: proto.Int32(2), Label: &optional, Type: &stringType},
					{Name: proto.String("issuer_fingerprint_sha256"), Number: proto.Int32(3), Label: &optional, Type: &stringType},
					{Name: proto.String("serial_number"), Number: proto.Int32(4), Label: &optional, Type: &stringType},
					{Name: proto.String("expected_tenant_id"), Number: proto.Int32(5), Label: &optional, Type: &stringType},
				},
			},
			{
				Name: proto.String("ResolveCertificateV2Response"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("entity_id"), Number: proto.Int32(1), Label: &optional, Type: &stringType},
					{Name: proto.String("tenant_id"), Number: proto.Int32(2), Label: &optional, Type: &stringType},
					{Name: proto.String("credential_id"), Number: proto.Int32(3), Label: &optional, Type: &stringType},
					{Name: proto.String("issuer_id"), Number: proto.Int32(4), Label: &optional, Type: &stringType},
					{Name: proto.String("expires_at"), Number: proto.Int32(5), Label: &optional, Type: &stringType},
					{Name: proto.String("status"), Number: proto.Int32(6), Label: &optional, Type: &stringType},
				},
			},
		},
	}, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build Atom certificate resolver protobuf descriptors: %w", err)
	}
	messages := file.Messages()
	return messages.ByName("ResolveCertificateV2Request"), messages.ByName("ResolveCertificateV2Response"), nil
}

func setBytes(message *dynamicpb.Message, name protoreflect.Name, value []byte) {
	if len(value) == 0 {
		return
	}
	field := message.Descriptor().Fields().ByName(name)
	message.Set(field, protoreflect.ValueOfBytes(value))
}

func setString(message *dynamicpb.Message, name protoreflect.Name, value string) {
	if value == "" {
		return
	}
	field := message.Descriptor().Fields().ByName(name)
	message.Set(field, protoreflect.ValueOfString(value))
}

func getString(message *dynamicpb.Message, name protoreflect.Name) string {
	field := message.Descriptor().Fields().ByName(name)
	return message.Get(field).String()
}

var _ Resolver = (*grpcResolver)(nil)
