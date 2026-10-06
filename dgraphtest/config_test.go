// SPDX-License-Identifier: Apache-2.0
package dgraphtest

import (
	"github.com/dgraph-io/dgraph/v25/worker"
	"github.com/dgraph-io/ristretto/v2/z"
	"reflect"
	"testing"
)

func TestLocalClusterCacheProfile(t *testing.T) {
	for _, tc := range []struct {
		name, profile string
		args          []string
		size          int64
		pct           string
		remove        bool
		bad           bool
	}{
		{name: "default", size: 4096, pct: "40,40,20"},
		{name: "public", profile: "512", size: 512, pct: "40,40,20"},
		{name: "override", profile: "512", args: []string{"--cache=size-mb=768;percentage=10,20,70;remove-on-update=true;"}, size: 768, pct: "10,20,70", remove: true},
		{name: "ordinary override", args: []string{"--cache=size-mb=768;"}, size: 768, pct: "40,40,20"},
		{name: "invalid env", profile: "1024", bad: true},
		{name: "invalid env bool", profile: "true", bad: true},
		{name: "duplicate flags", profile: "512", args: []string{"--cache=size-mb=768", "--cache=size-mb=512"}, bad: true},
		{name: "duplicate normalized controls", profile: "512", args: []string{"--cache=size-mb=768;SIZE_MB=512;"}, bad: true},
		{name: "missing value", profile: "512", args: []string{"--cache=512"}, bad: true},
		{name: "split flag", profile: "512", args: []string{"--cache"}, bad: true},
		{name: "empty override", profile: "512", args: []string{"--cache="}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc := NewClusterConfig()
			cc.startupArgs = append(cc.startupArgs, tc.args...)
			got, err := localClusterCacheProfile(cc, tc.profile)
			if tc.bad {
				if err == nil {
					t.Fatal("invalid control admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(tc.args) > 0 && !reflect.DeepEqual(got.startupArgs, tc.args) {
				t.Fatal("explicit caller override changed")
			}
			value := ""
			for _, arg := range got.startupArgs {
				value = arg[len("--cache="):]
			}
			sf := z.NewSuperFlag(value).MergeAndCheckDefault(worker.CacheDefaults)
			if sf.GetInt64("size-mb") != tc.size || sf.GetString("percentage") != tc.pct || sf.GetBool("remove-on-update") != tc.remove {
				t.Fatalf("incorrect effective Alpha cache: %s", sf)
			}
		})
	}
}
func TestLocalClusterRejectsCacheProfileBeforeDocker(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent-pr41-docker.sock")
	t.Setenv("DGRAPH_CI_LOCAL_ALPHA_CACHE_MB", "invalid")
	cluster, err := NewLocalCluster(NewClusterConfig())
	if err == nil || cluster != nil || err.Error() != "DGRAPH_CI_LOCAL_ALPHA_CACHE_MB must be empty or 512" {
		t.Fatalf("admission reached Docker: %v, %v", cluster, err)
	}
}
