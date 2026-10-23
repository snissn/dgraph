// SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
// SPDX-License-Identifier: Apache-2.0

// Command schemaproof is a bounded real-gRPC correctness diagnostic. The Python
// launcher owns processes and artifacts; this companion never times throughput.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type proofState struct {
	UIDs       map[string]string `json:"uids"`
	SeedTs     uint64            `json:"seed_ts"`
	PendingOne *api.TxnContext   `json:"pending_one,omitempty"`
	PendingTwo *api.TxnContext   `json:"pending_two,omitempty"`
	PendingUID string            `json:"pending_uid,omitempty"`
	StableUID  string            `json:"stable_uid,omitempty"`
}

type record struct {
	Method string `json:"method"`
	Input  any    `json:"input"`
	Output any    `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

type event struct {
	Kind           string     `json:"event"`
	Phase          string     `json:"phase"`
	Success        bool       `json:"success"`
	State          proofState `json:"state"`
	ResponseUnixNS int64      `json:"response_unix_ns,omitempty"`
	Records        []record   `json:"records,omitempty"`
	Error          string     `json:"error,omitempty"`
}

type proof struct {
	client     api.DgraphClient
	ctx        context.Context
	state      proofState
	phase      string
	records    []record
	responseNS int64
}

func main() {
	addr := flag.String("addr", "localhost:9080", "Alpha gRPC endpoint")
	phase := flag.String("phase", "", "seed/count-cut/count-check/conversion-cut/conversion-check/conversion-final/unique-prepare/unique-decide/final")
	statePath := flag.String("state", "", "launcher-owned prior phase state JSON")
	token := flag.String("auth-token", "", "Alpha auth-token metadata")
	flag.Parse()
	p := &proof{ctx: context.Background(), phase: *phase, state: proofState{UIDs: map[string]string{}}}
	if *token != "" {
		p.ctx = metadata.AppendToOutgoingContext(p.ctx, "auth-token", *token)
	}
	if *statePath != "" {
		data, err := os.ReadFile(*statePath)
		if err != nil {
			p.finish("failure", err)
			os.Exit(1)
		}
		if err := json.Unmarshal(data, &p.state); err != nil {
			p.finish("failure", err)
			os.Exit(1)
		}
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		p.finish("failure", err)
		os.Exit(1)
	}
	defer conn.Close()
	p.client = api.NewDgraphClient(conn)
	kind, err := p.run()
	if err != nil {
		p.finish("failure", err)
		os.Exit(1)
	}
	if kind == "ack_cut" {
		// Emit only the small cut event first. The launcher terminates servers
		// before reading/writing transcripts or making another network request.
		p.emit(event{Kind: kind, Phase: p.phase, Success: true, State: p.state, ResponseUnixNS: p.responseNS})
		p.finish("transcript", nil)
		return
	}
	p.finish(kind, nil)
}

func (p *proof) emit(e event) {
	if err := json.NewEncoder(os.Stdout).Encode(e); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func (p *proof) finish(kind string, err error) {
	e := event{Kind: kind, Phase: p.phase, Success: err == nil, State: p.state, Records: p.records}
	if err != nil {
		e.Error = err.Error()
	}
	p.emit(e)
}

func (p *proof) alter(schema string) error {
	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()
	input := &api.Operation{Schema: schema}
	output, err := p.client.Alter(ctx, input)
	p.responseNS = time.Now().UnixNano()
	p.note("/api.Dgraph/Alter", input, output, err)
	return err
}
func (p *proof) call(input *api.Request) (*api.Response, error) {
	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()
	output, err := p.client.Query(ctx, input)
	p.responseNS = time.Now().UnixNano()
	p.note("/api.Dgraph/Query", input, output, err)
	return output, err
}
func (p *proof) decision(input *api.TxnContext) (*api.TxnContext, error) {
	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()
	output, err := p.client.CommitOrAbort(ctx, input)
	p.responseNS = time.Now().UnixNano()
	p.note("/api.Dgraph/CommitOrAbort", input, output, err)
	return output, err
}
func (p *proof) note(method string, input, output any, err error) {
	r := record{Method: method, Input: input, Output: output}
	if err != nil {
		r.Error = err.Error()
	}
	p.records = append(p.records, r)
}
func (p *proof) mutate(set, del any, commit bool) (*api.Response, error) {
	mu := &api.Mutation{}
	var err error
	if set != nil {
		mu.SetJson, err = json.Marshal(set)
		if err != nil {
			return nil, err
		}
	}
	if del != nil {
		mu.DeleteJson, err = json.Marshal(del)
		if err != nil {
			return nil, err
		}
	}
	return p.call(&api.Request{Mutations: []*api.Mutation{mu}, CommitNow: commit})
}
func (p *proof) expectQuery(query string, ts uint64, want any) error {
	response, err := p.call(&api.Request{Query: query, StartTs: ts, ReadOnly: true})
	if err != nil {
		return err
	}
	var got any
	if err = json.Unmarshal(response.Json, &got); err != nil {
		return err
	}
	encoded, err := json.Marshal(want)
	if err != nil {
		return err
	}
	var expected any
	if err = json.Unmarshal(encoded, &expected); err != nil {
		return err
	}
	a, _ := json.Marshal(normalize(got))
	b, _ := json.Marshal(normalize(expected))
	if string(a) != string(b) {
		return fmt.Errorf("query mismatch at ts=%d: %s: got %s want %s", ts, query, a, b)
	}
	return nil
}
func normalize(value any) any {
	switch v := value.(type) {
	case []any:
		for i := range v {
			v[i] = normalize(v[i])
		}
		sort.Slice(v, func(i, j int) bool {
			a, _ := json.Marshal(v[i])
			b, _ := json.Marshal(v[j])
			return string(a) < string(b)
		})
	case map[string]any:
		for k, child := range v {
			v[k] = normalize(child)
		}
	}
	return value
}
func rows(items ...map[string]any) any  { return map[string]any{"q": items} }
func emptyRows() any                    { return map[string]any{"q": []any{}} }
func (p *proof) uid(name string) string { return p.state.UIDs[name] }
func (p *proof) count(reverse bool, n int, want ...string) error {
	attr := "proof_f"
	if reverse {
		attr = "~proof_f"
	}
	items := make([]map[string]any, 0, len(want))
	for _, name := range want {
		items = append(items, map[string]any{"uid": p.uid(name)})
	}
	return p.expectQuery(fmt.Sprintf("{q(func:eq(count(%s),%d)){uid}}", attr, n), 0, rows(items...))
}
func (p *proof) verifyCount() error {
	if err := p.count(false, 2, "a"); err != nil {
		return err
	}
	if err := p.count(false, 1, "b"); err != nil {
		return err
	}
	if err := p.count(true, 2, "t4"); err != nil {
		return err
	}
	return p.count(true, 1, "t5")
}
func (p *proof) unique(value string, want ...string) error {
	items := make([]map[string]any, 0, len(want))
	for _, uid := range want {
		items = append(items, map[string]any{"uid": uid, "proof_u": value})
	}
	return p.expectQuery(fmt.Sprintf("{q(func:eq(proof_u,%q)){uid proof_u}}", value), 0, rows(items...))
}

func (p *proof) run() (string, error) {
	switch p.phase {
	case "seed":
		if err := p.alter("proof_f: [uid] .\nproof_c: [string] @count @index(exact) .\nproof_s: string .\nproof_scalarint: string .\nproof_bad: string .\nproof_u: string @index(exact) ."); err != nil {
			return "", err
		}
		response, err := p.mutate([]any{
			map[string]any{"uid": "_:a", "proof_f": []any{map[string]any{"uid": "_:t4"}, map[string]any{"uid": "_:t5"}}, "proof_c": []string{"01", "1"}, "proof_s": "42", "proof_scalarint": "7", "proof_bad": "not-an-int", "proof_u": "alpha"},
			map[string]any{"uid": "_:b", "proof_f": map[string]any{"uid": "_:t4"}, "proof_u": "alpha"},
		}, nil, true)
		if err != nil {
			return "", err
		}
		p.state.UIDs = response.Uids
		p.state.SeedTs = response.GetTxn().GetCommitTs()
		for _, name := range []string{"a", "b", "t4", "t5"} {
			if p.uid(name) == "" {
				return "", fmt.Errorf("seed missing UID %s", name)
			}
		}
		if p.state.SeedTs == 0 {
			return "", errors.New("seed has no public commit timestamp")
		}
		// A fresh read drains the seed's ordered Delta before a schema change.
		if err = p.unique("alpha", p.uid("a"), p.uid("b")); err != nil {
			return "", err
		}
	case "count-cut":
		if err := p.alter("proof_f: [uid] @count ."); err != nil {
			return "", err
		}
		if err := p.count(false, 2, "a"); err != nil {
			return "", err
		}
		if err := p.alter("proof_f: [uid] @count @reverse ."); err != nil {
			return "", err
		}
		if err := p.verifyCount(); err != nil {
			return "", err
		}
		if err := p.alter("proof_f: [uid] @count ."); err != nil {
			return "", err
		}
		if err := p.expectRefusal(&api.Request{Query: "{q(func:eq(count(~proof_f),2)){uid}}", ReadOnly: true}, "reverse"); err != nil {
			return "", err
		}
		if err := p.alter("proof_f: [uid] @count @reverse ."); err != nil {
			return "", err
		}
		return "ack_cut", nil
	case "count-check":
		if err := p.verifyCount(); err != nil {
			return "", err
		}
		if err := p.alter("proof_f: [uid] @reverse ."); err != nil {
			return "", err
		}
		if err := p.expectRefusal(&api.Request{Query: "{q(func:eq(count(proof_f),2)){uid}}", ReadOnly: true}, "count"); err != nil {
			return "", err
		}
		if err := p.alter("proof_f: [uid] @reverse @count ."); err != nil {
			return "", err
		}
		if err := p.verifyCount(); err != nil {
			return "", err
		}
		if _, err := p.mutate(nil, map[string]any{"uid": p.uid("a"), "proof_f": map[string]any{"uid": p.uid("t5")}}, true); err != nil {
			return "", err
		}
		q := fmt.Sprintf("{q(func:uid(%s)){uid proof_f{uid}}}", p.uid("a"))
		if err := p.expectQuery(q, 0, rows(map[string]any{"uid": p.uid("a"), "proof_f": []any{map[string]any{"uid": p.uid("t4")}}})); err != nil {
			return "", err
		}
		if err := p.expectQuery(q, p.state.SeedTs, rows(map[string]any{"uid": p.uid("a"), "proof_f": []any{map[string]any{"uid": p.uid("t4")}, map[string]any{"uid": p.uid("t5")}}})); err != nil {
			return "", err
		}
		if _, err := p.mutate(map[string]any{"uid": p.uid("a"), "proof_f": map[string]any{"uid": p.uid("t5")}}, nil, true); err != nil {
			return "", err
		}
		if err := p.verifyCount(); err != nil {
			return "", err
		}
	case "conversion-cut":
		// The later incompatible conversion must refuse before an earlier
		// predicate in the same Alter loses its existing count/reverse indexes.
		err := p.alter("proof_f: [uid] .\nproof_bad: int @index(int) .")
		if err == nil {
			return "", errors.New("invalid populated conversion unexpectedly succeeded")
		}
		if status.Code(err) != codes.InvalidArgument && !strings.Contains(strings.ToLower(err.Error()), "convert") && !strings.Contains(strings.ToLower(err.Error()), "parse") {
			return "", fmt.Errorf("unclassified conversion refusal: %w", err)
		}
		if err = p.verifyCount(); err != nil {
			return "", err
		}
		if err = p.expectQuery("schema(pred:[proof_bad]){predicate type}", 0, map[string]any{"schema": []any{map[string]any{"predicate": "proof_bad", "type": "string"}}}); err != nil {
			return "", err
		}
		if err = p.alter("proof_c: [int] @count @index(int) .\nproof_s: [int] .\nproof_scalarint: int ."); err != nil {
			return "", err
		}
		return "ack_cut", nil
	case "conversion-check":
		if err := p.verifyConversion(); err != nil {
			return "", err
		}
		if _, err := p.mutate(map[string]any{"uid": p.uid("a"), "proof_c": []int{1}}, nil, true); err != nil {
			return "", err
		}
		if err := p.verifyConversion(); err != nil {
			return "", err
		}
		if _, err := p.mutate(nil, map[string]any{"uid": p.uid("a"), "proof_c": []int{1}}, true); err != nil {
			return "", err
		}
		return "ack_cut", nil
	case "conversion-final":
		if err := p.expectQuery("{q(func:eq(proof_c,1)){uid}}", 0, emptyRows()); err != nil {
			return "", err
		}
		if err := p.expectQuery("{q(func:eq(count(proof_c),1)){uid}}", 0, emptyRows()); err != nil {
			return "", err
		}
		if _, err := p.mutate(map[string]any{"uid": p.uid("a"), "proof_c": []int{1}}, nil, true); err != nil {
			return "", err
		}
		if err := p.verifyConversion(); err != nil {
			return "", err
		}
	case "unique-prepare":
		if err := p.alter("proof_u: string @index(exact) @unique ."); err == nil || !strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			return "", fmt.Errorf("expected existing duplicate refusal enabling unique: %v", err)
		}
		if err := p.unique("alpha", p.uid("a"), p.uid("b")); err != nil {
			return "", err
		}
		if err := p.expectQuery("schema(pred:[proof_u]){predicate type unique}", 0, map[string]any{"schema": []any{map[string]any{"predicate": "proof_u", "type": "string"}}}); err != nil {
			return "", err
		}
		if _, err := p.mutate(map[string]any{"uid": p.uid("b"), "proof_u": "beta"}, nil, true); err != nil {
			return "", err
		}
		if err := p.unique("beta", p.uid("b")); err != nil {
			return "", err
		}
		if err := p.alter("proof_u: string @index(exact) @unique ."); err != nil {
			return "", err
		}
		if err := p.expectQuery("schema(pred:[proof_u]){predicate type unique}", 0, map[string]any{"schema": []any{map[string]any{"predicate": "proof_u", "type": "string", "unique": true}}}); err != nil {
			return "", err
		}
		if _, err := p.mutate(map[string]any{"uid": p.uid("a"), "proof_u": "alpha"}, nil, true); err != nil {
			return "", err
		}
		if err := p.unique("alpha", p.uid("a")); err != nil {
			return "", err
		}
		if err := p.expectRefusal(&api.Request{CommitNow: true, Mutations: []*api.Mutation{{SetNquads: []byte("_:duplicate <proof_u> \"alpha\" .")}}}, "duplicate"); err != nil {
			return "", err
		}
		one, err := p.mutate(map[string]any{"uid": "_:pending-one", "proof_u": "gamma"}, nil, false)
		if err != nil {
			return "", err
		}
		p.state.PendingOne = one.Txn
		p.state.PendingUID = one.Uids["pending-one"]
		two, err := p.mutate(map[string]any{"uid": "_:pending-two", "proof_u": "gamma"}, nil, false)
		if err != nil {
			return "", err
		}
		p.state.PendingTwo = two.Txn
		if p.state.PendingOne.GetStartTs() == 0 || p.state.PendingTwo.GetStartTs() == 0 || len(p.state.PendingOne.GetKeys()) == 0 || len(p.state.PendingTwo.GetKeys()) == 0 {
			return "", errors.New("pending unique mutations lack canonical transaction/conflict context")
		}
		shared := false
		for _, oneKey := range p.state.PendingOne.GetKeys() {
			for _, twoKey := range p.state.PendingTwo.GetKeys() {
				if oneKey == twoKey {
					shared = true
				}
			}
		}
		if !shared {
			return "", errors.New("pending unique mutations do not carry a shared Zero conflict key")
		}
		return "alpha_restart", nil
	case "unique-decide":
		if p.state.PendingOne == nil || p.state.PendingTwo == nil {
			return "", errors.New("missing retained pending contexts")
		}
		one, err := p.decision(p.state.PendingOne)
		if err != nil {
			return "", err
		}
		if one.GetAborted() || one.GetCommitTs() == 0 {
			return "", errors.New("first pending transaction did not commit")
		}
		two, err := p.decision(p.state.PendingTwo)
		if err == nil && !two.GetAborted() {
			return "", errors.New("conflicting pending transaction did not abort")
		}
		if err != nil && status.Code(err) != codes.Aborted && !strings.Contains(strings.ToLower(err.Error()), "abort") {
			return "", fmt.Errorf("unclassified conflict result: %w", err)
		}
		if err = p.unique("gamma", p.state.PendingUID); err != nil {
			return "", err
		}
		last, err := p.mutate(map[string]any{"uid": "_:stable", "proof_u": "stable-ack"}, nil, true)
		if err != nil {
			return "", err
		}
		p.state.StableUID = last.Uids["stable"]
		return "ack_cut", nil
	case "final":
		if err := p.verifyCount(); err != nil {
			return "", err
		}
		if err := p.verifyConversion(); err != nil {
			return "", err
		}
		for value, uid := range map[string]string{"alpha": p.uid("a"), "beta": p.uid("b"), "gamma": p.state.PendingUID, "stable-ack": p.state.StableUID} {
			if err := p.unique(value, uid); err != nil {
				return "", err
			}
		}
	default:
		return "", fmt.Errorf("unknown proof phase %q", p.phase)
	}
	return "phase_done", nil
}

func (p *proof) expectRefusal(input *api.Request, contains string) error {
	_, err := p.call(input)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), contains) {
		return fmt.Errorf("expected %q refusal, got %v", contains, err)
	}
	return nil
}
func (p *proof) verifyConversion() error {
	if err := p.expectQuery(fmt.Sprintf("{q(func:uid(%s)){proof_c proof_s proof_scalarint}}", p.uid("a")), 0, rows(map[string]any{"proof_c": []int{1}, "proof_s": []int{42}, "proof_scalarint": 7})); err != nil {
		return err
	}
	if err := p.expectQuery("{q(func:eq(proof_c,1)){uid}}", 0, rows(map[string]any{"uid": p.uid("a")})); err != nil {
		return err
	}
	if err := p.expectQuery("{q(func:eq(count(proof_c),1)){uid}}", 0, rows(map[string]any{"uid": p.uid("a")})); err != nil {
		return err
	}
	return p.expectQuery("{q(func:eq(count(proof_c),2)){uid}}", 0, emptyRows())
}
