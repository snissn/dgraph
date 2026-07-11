// SPDX-License-Identifier: Apache-2.0
package main

import (
	"flag"
	"fmt"
	"github.com/dgraph-io/dgraph/v25/worker/treedb/livebench"
	"os"
)

func main() {
	repeats := flag.Int("repeats", 3, "repeats per matrix cell")
	out := flag.String("out", "", "new report path")
	profileDir := flag.String("profile-dir", "", "directory containing complete separate TreeDB profile artifacts")
	flag.Parse()
	if *out == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: reportcmd --repeats N --out NEW.md [--profile-dir DIR] result.json...")
		os.Exit(2)
	}
	results, err := livebench.LoadResults(flag.Args())
	if err == nil {
		var report string
		if *profileDir == "" {
			report, err = livebench.RenderReport(results, *repeats)
		} else {
			var profiles livebench.ProfileArtifacts
			profiles, err = livebench.DiscoverProfileArtifacts(*profileDir)
			if err == nil {
				report, err = livebench.RenderReportWithProfiles(results, *repeats, profiles)
			}
		}
		if err == nil {
			err = livebench.WriteImmutableBytes(*out, []byte(report))
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
