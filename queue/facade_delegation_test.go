// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// transitionFacadeMethods are the Manager methods that apply a queue state
// transition. Each must hand the transition to the command processor rather
// than perform it, so there is one place where a transition is defined.
//
// Routing, lifecycle and admin methods are deliberately absent: subscriptions,
// heartbeats and queue administration are the facade's own work, not
// transitions, and the core has no opinion about them.
var transitionFacadeMethods = map[string]string{
	"Ack":          "Ack",
	"Nack":         "Nack",
	"Reject":       "Reject",
	"CommitOffset": "CommitOffset",
}

// The facade must not carry queue-state logic of its own. A transition that
// reaches past the state machine — into the consumer manager or a store — is a
// second definition of that transition, and the two drift.
//
// This reads the source rather than using reflection because the property is
// about which call a body makes, which no runtime check can see.
func TestFacadeDelegatesEveryTransition(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "manager.go", nil, 0)
	require.NoError(t, err)

	found := make(map[string]bool, len(transitionFacadeMethods))

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil {
			continue
		}
		command, tracked := transitionFacadeMethods[fn.Name.Name]
		if !tracked {
			continue
		}
		found[fn.Name.Name] = true

		delegates := false
		reaches := []string{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			receiver, ok := selector.X.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch receiver.Sel.Name {
			case "stateMachine":
				if selector.Sel.Name == command {
					delegates = true
				}
			case "consumerManager", "queueStore", "groupStore", "records":
				reaches = append(reaches, receiver.Sel.Name+"."+selector.Sel.Name)
			}
			return true
		})

		require.True(t, delegates,
			"Manager.%s must delegate to stateMachine.%s", fn.Name.Name, command)
		require.Empty(t, reaches,
			"Manager.%s applies its own state logic via %s; the transition belongs in the command processor",
			fn.Name.Name, strings.Join(reaches, ", "))
	}

	for name := range transitionFacadeMethods {
		require.True(t, found[name], "Manager.%s not found; update transitionFacadeMethods", name)
	}
}
