// SPDX-License-Identifier: Apache-2.0
package livebench

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHarnessRejectsRelativeInRepoArtifactBeforeCreation(t *testing.T) {
	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repoRoot := filepath.Clean(filepath.Join(workingDir, "../../.."))
	rel := fmt.Sprintf(".durability-ab-in-repo-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	abs := filepath.Join(repoRoot, rel)
	t.Cleanup(func() { _ = os.RemoveAll(abs) })

	cmd := exec.Command("bash", "worker/treedb/run_durability_ab.sh", "--smoke", "--artifact-dir", rel)
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("in-repo artifact path unexpectedly accepted: %s", output)
	}
	if !strings.Contains(string(output), "must be outside repository") {
		t.Fatalf("unexpected rejection: %s", output)
	}
	if _, err := os.Stat(abs); !os.IsNotExist(err) {
		t.Fatalf("in-repo artifact path was created before rejection: %v", err)
	}
}
