// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/absmach/fluxmq/pkg/proto/queue/v1/queuev1connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Handler embeds UnimplementedQueueServiceHandler, so it satisfies the generated
// interface whether or not it implements anything. A handler method whose name
// drifts from its RPC therefore compiles cleanly and is silently replaced by a
// stub returning CodeUnimplemented — the RPC looks served and answers nothing.
//
// Two RPCs shipped in exactly that state: AppendQueue was implemented as
// AppendStream, and ConsumeQueue as ConsumeStream. Neither was reachable, and
// nothing failed.
//
// Reflection cannot catch this: a promoted method has its own wrapper, so its
// code pointer never equals the embedded stub's. The check has to be at the
// source level — does this package declare a method of that name on *Handler?
func TestEveryRPCIsImplementedNotStubbed(t *testing.T) {
	declared := declaredHandlerMethods(t)
	ifaceType := reflect.TypeOf((*queuev1connect.QueueServiceHandler)(nil)).Elem()
	require.Positive(t, ifaceType.NumMethod(), "generated interface exposes no methods")

	for i := range ifaceType.NumMethod() {
		name := ifaceType.Method(i).Name
		t.Run(name, func(t *testing.T) {
			assert.Contains(t, declared, name,
				"no method named %s is declared on *Handler, so the RPC falls through to "+
					"UnimplementedQueueServiceHandler and answers CodeUnimplemented. "+
					"The method name must match the RPC name exactly.", name)
		})
	}
}

// Methods that exist on *Handler but match no RPC are dead: they were most
// likely renamed away from the RPC they were written for.
func TestNoOrphanedHandlerMethods(t *testing.T) {
	ifaceType := reflect.TypeOf((*queuev1connect.QueueServiceHandler)(nil)).Elem()
	rpcs := make(map[string]struct{}, ifaceType.NumMethod())
	for i := range ifaceType.NumMethod() {
		rpcs[ifaceType.Method(i).Name] = struct{}{}
	}

	for _, name := range declaredHandlerMethods(t) {
		if _, isRPC := rpcs[name]; isRPC {
			continue
		}
		// Unexported helpers are implementation detail, not RPC surface.
		if !ast.IsExported(name) {
			continue
		}
		t.Errorf("*Handler declares exported method %s, which serves no RPC", name)
	}
}

// declaredHandlerMethods returns the names of every method declared on *Handler
// in this package's non-test sources.
func declaredHandlerMethods(t *testing.T) []string {
	t.Helper()

	sources, err := filepath.Glob("*.go")
	require.NoError(t, err)

	fset := token.NewFileSet()
	var names []string
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, source, nil, 0)
		require.NoError(t, err)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			ident, ok := star.X.(*ast.Ident)
			if !ok || ident.Name != "Handler" {
				continue
			}
			names = append(names, fn.Name.Name)
		}
	}
	require.NotEmpty(t, names, "no methods found on *Handler")
	return names
}
