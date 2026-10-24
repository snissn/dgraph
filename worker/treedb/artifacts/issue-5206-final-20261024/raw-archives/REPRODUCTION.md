# Raw archive custody (staging)

These gzip files reproduce the original selected-file tar archives exactly. GNU `gzip -n -6 -c` omits the original filename and forces gzip mtime to zero. The tool version, source tar and gzip SHA256/byte counts, member inventory, original receipt hashes and per-file commands are in `RAW_ARCHIVE_CUSTODY.json`. The source tar SHA256 must match both the unchanged original receipt and the decompressed gzip stream. Gzip output identity is bound to the recorded implementation/version; decompressed tar identity is the original authority.

To recover an archive without altering its contents:

```sh
gzip -d -c -- process-12-EVIDENCE.tar.gz > /new/path/process-12-EVIDENCE.tar
sha256sum /new/path/process-12-EVIDENCE.tar
```

Check the SHA256 against `process-12-ARCHIVE_RECEIPT.json` before extraction. Apply the corresponding command/hash to each archive. The archive member inventory contains only selected evidence files, not runtime databases or executable binaries. CPU profile data is intentionally retained. Original local tar archives remain untouched. Baseline custody is included and verified; final report acceptance/publication remains pending. This custody receipt is not a successful campaign verdict.
