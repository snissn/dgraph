# SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
# SPDX-License-Identifier: Apache-2.0
import os
from pathlib import Path
import tempfile
import unittest
import zlib

from verify_retention_resources import fixture_value, storage_inventory


class ResourceObserverTests(unittest.TestCase):
    def test_high_entropy_overwrites_preserve_exact_size_and_identity(self):
        for size in (32, 256, 16384):
            first = fixture_value(0, 0, size, "shake256")
            self.assertEqual(len(first.encode("ascii")), size)
            self.assertEqual(first, fixture_value(0, 0, size, "shake256"))
            self.assertNotEqual(first, fixture_value(1, 0, size, "shake256"))
            self.assertNotEqual(first, fixture_value(0, 1, size, "shake256"))
        # Guard against accidentally retaining the old repeated-byte fixture.
        self.assertGreater(len(zlib.compress(first.encode())), size // 2)
        self.assertEqual(fixture_value(0, 1, 32, "repeat"),
                         "00000000-00000001-".ljust(32, "x"))

    def test_inventory_tracks_names_and_storage_identity_without_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "value.log"
            path.write_bytes(b"old-value")
            os.link(path, root / "alias.log")
            first = storage_inventory(root)
            self.assertEqual([row["path"] for row in first["files"]],
                             ["alias.log", "value.log"])
            self.assertEqual(first["files"][0]["inode"], first["files"][1]["inode"])
            path.unlink()
            path.write_bytes(b"new-value-longer")
            second = storage_inventory(root)
            self.assertNotEqual(second["files"][0]["inode"], second["files"][1]["inode"])
            self.assertEqual(path.read_bytes(), b"new-value-longer")
            self.assertEqual((root / "alias.log").read_bytes(), b"old-value")
            with self.assertRaisesRegex(RuntimeError, "observer file bound"):
                storage_inventory(root, max_files=1)


if __name__ == "__main__":
    unittest.main()
