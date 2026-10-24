#!/usr/bin/env python3
"""Prepare external module inputs only; never apply, commit, build or run sources."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def git(source, *args):
    return subprocess.check_output(['git', '-C', str(source), *args], text=True).strip()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dgraph-source', required=True)
    parser.add_argument('--gomap-source', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    packet = Path(__file__).resolve().parent
    manifest = json.loads((packet / 'manifest.json').read_text())
    dgraph = Path(args.dgraph_source).resolve(strict=True)
    gomap = Path(args.gomap_source).resolve(strict=True)
    output = Path(args.output).resolve()
    for source in (dgraph, gomap):
        if Path(git(source, 'rev-parse', '--show-toplevel')) != source:
            raise SystemExit('source must be a repository root')
        if git(source, 'status', '--porcelain=v1', '--untracked-files=all'):
            raise SystemExit('source must be clean including untracked files')
        if output == source or source in output.parents:
            raise SystemExit('build module output must be outside source repositories')
    if git(gomap, 'rev-parse', 'HEAD') != manifest['gomap_base']:
        raise SystemExit('wrong Gomap revision')
    if git(gomap, 'rev-parse', 'HEAD^{tree}') != manifest['gomap_base_tree']:
        raise SystemExit('wrong Gomap tree')
    if git(dgraph, 'rev-parse', 'HEAD^{tree}') != manifest['expected_adapted_tree']:
        raise SystemExit('Dgraph is not the exact committed adapted baseline tree')
    subprocess.run(['git', '-C', str(dgraph), 'merge-base', '--is-ancestor',
                    manifest['dgraph_base'], 'HEAD'], check=True)
    if digest((packet / 'baseline.patch').read_bytes()) != manifest['patch_sha256']:
        raise SystemExit('patch binding mismatch')
    for entry in manifest['files']:
        if digest((dgraph / entry['path']).read_bytes()) != entry['adapted_sha256']:
            raise SystemExit('adapted source file mismatch: ' + entry['path'])
    original_mod = (packet / 'original-go.mod').read_bytes()
    original_sum = (packet / 'original-go.sum').read_bytes()
    if (dgraph / 'go.mod').read_bytes() != original_mod or (dgraph / 'go.sum').read_bytes() != original_sum:
        raise SystemExit('baseline module inputs changed')
    output.mkdir(parents=True, exist_ok=False)
    historical_mod = (packet / 'historical-resolved-dgraph.mod').read_bytes()
    historical_sum = (packet / 'historical-resolved-dgraph.sum').read_bytes()
    for name, raw in (('dgraph.mod', historical_mod), ('dgraph.sum', historical_sum)):
        if digest(raw) != manifest['module']['historical_resolved_sha256'][name]:
            raise SystemExit('historical module binding mismatch: ' + name)
    old_replace = b'replace github.com/snissn/gomap => /mnt/fast4tb/gomap-cow-merge-20261023/worktrees/merged-gomap-final'
    if historical_mod.count(old_replace) != 1:
        raise SystemExit('unexpected historical replacement')
    replacement = 'replace github.com/snissn/gomap => ' + json.dumps(str(gomap))
    build_mod = historical_mod.replace(old_replace, replacement.encode())
    (output / 'build.mod').write_bytes(build_mod)
    (output / 'build.sum').write_bytes(historical_sum)
    binding = {'status': 'PREPARED_UNBUILT_UNEXECUTED',
               'dgraph_revision': git(dgraph, 'rev-parse', 'HEAD'),
               'dgraph_tree': manifest['expected_adapted_tree'],
               'dgraph_source': str(dgraph), 'gomap_source': str(gomap),
               'gomap_revision': manifest['gomap_base'],
               'packet_manifest_sha256': digest((packet / 'manifest.json').read_bytes()),
               'build_mod_sha256': digest(build_mod),
               'build_sum_sha256': digest(historical_sum)}
    (output / 'prepared-module.json').write_text(json.dumps(binding, indent=2) + '\n')
    print(json.dumps(binding, indent=2))


if __name__ == '__main__':
    main()
