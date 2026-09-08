#!/usr/bin/env python3
"""Verify local patch source availability before the image dependency cook.

This is a source-selection contract, not a Docker build. cargo-chef 0.1.73
prepares workspace members only; excluded path patches need an explicit COPY.
"""
from pathlib import Path
import shlex
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[3]


class VendoredPatchContext(unittest.TestCase):
    def test_excluded_patches_are_copied_before_dependency_cook(self):
        manifest = tomllib.loads((ROOT / 'Cargo.toml').read_text())
        excluded = set(manifest['workspace'].get('exclude', []))
        patches = manifest.get('patch', {}).get('crates-io', {})
        required = [Path(p['path']) for p in patches.values()
                    if isinstance(p, dict) and p.get('path') in excluded]
        self.assertTrue(required, 'this regression must cover an excluded local patch')
        builder = False
        found_cook = False
        copies = []
        for line in (ROOT / 'deploy/docker/connect-server.Dockerfile').read_text().splitlines():
            words = shlex.split(line, comments=True) if not line.rstrip().endswith('\\') else []
            if not words:
                continue
            if words[0] == 'FROM':
                builder = words[-1] == 'builder'
            elif builder and words[0:3] == ['RUN', 'cargo', 'chef'] and words[3] == 'cook':
                found_cook = True
                break
            elif builder and words[0] == 'COPY' and not words[1].startswith('--from='):
                self.assertEqual(len(words), 3, 'keep pre-cook source COPY explicit')
                copies.append((Path(words[1]), Path(words[2])))
        self.assertTrue(found_cook, 'must inspect the real dependency-cook boundary')
        for patch in required:
            for source in [patch / 'Cargo.toml', *sorted((ROOT / patch / 'src').rglob('*.rs'))]:
                source = source.relative_to(ROOT) if source.is_absolute() else source
                destination = None
                for src, dest in copies:
                    if source.is_relative_to(src):
                        destination = dest / source.relative_to(src)
                self.assertEqual(destination, source,
                                 f'{source} missing or relocated before cargo chef cook')
                self.assertTrue((ROOT / source).is_file())


if __name__ == '__main__':
    unittest.main()
