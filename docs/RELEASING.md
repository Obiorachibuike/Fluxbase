# Releasing

Publishing is manual and requires explicit authorisation. The repository never publishes automatically.

## 1. Dry run (safe, any time)

```bash
npm ci
npm run release:dry-run      # typecheck, lint, tests, build, package verification, npm pack --dry-run
```

Or run the **Release dry run** GitHub workflow (`workflow_dispatch`). It runs the same checks, uploads the tarball as a build artifact, and never publishes.

Before a real release, also run the distributed suite against Redis (the CI job does this):

```bash
REDIS_URL=redis://127.0.0.1:6379 npm test
```

## 2. Check the name

```bash
npm view fluxbase name   # 404 means not published yet. Re-check at publish time.
```

## 3. Publish (manual, only with authorisation)

1. Bump `version` in `package.json` and move the CHANGELOG entry from "unreleased".
2. Tag: `git tag v0.1.0 && git push origin v0.1.0`.
3. `npm login` with the publishing account (2FA enabled).
4. `npm publish --access public --provenance` from a clean checkout of the tag. `publishConfig` already sets public access and provenance, which needs a CI environment with OIDC; for a local publish, drop `--provenance`.
5. Verify: `npm view fluxbase version` and install the package into an empty project.

`prepublishOnly` runs the full `check` script, so a failing check blocks publishing.
