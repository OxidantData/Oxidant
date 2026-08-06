# Site hosting: oxidantdata.com on S3 + CloudFront

Static hosting for the Oxidant marketing/showcase site (`site/`, Vite + React).
Replaces the previous GitHub Pages deployment (the `site/public/CNAME` artifact
is gone; Pages is no longer used).

## Architecture

```
Browser ──HTTPS──▶ CloudFront ──OAC (sigv4)──▶ S3 bucket (private)
                        ▲
                     ACM cert (us-east-1, DNS-validated)

Route53 hosted zone: alias A/AAAA → CloudFront, ACM validation CNAMEs
GitHub Actions ──OIDC (no stored keys)──▶ IAM role ──▶ s3 sync + create-invalidation
```

- **S3 bucket `oxidantdata.com`** (us-east-1) — origin. Fully private:
  BlockPublicAccess all ON, no website-hosting mode. Only CloudFront can read
  it, via a bucket policy scoped to the distribution ARN.
- **CloudFront distribution `E3BG86EZYJNHTO`** (`d2a7770knck57q.cloudfront.net`) —
  default root object `index.html`, HTTP→HTTPS redirect, managed
  CachingOptimized cache policy, price class 100, HTTP/2+3, IPv6. Custom error
  responses map 403 and 404 → `/index.html` with a 200 as an SPA safety net
  (the site routes by hash, e.g. `/#/performance`, so this is only a fallback).
  Origin access goes through **OAC `EZ4MLYMCDE52V`** (sigv4, always sign).
- **ACM certificate** (us-east-1, required by CloudFront), DNS validation,
  covers `oxidantdata.com` and `www.oxidantdata.com`:
  `arn:aws:acm:us-east-1:810738286322:certificate/ed47b6a8-a155-454e-99cc-2546a92db488`
- **Route53 hosted zone `Z0014528AK93TYSRKI11`** for `oxidantdata.com` — holds
  the ACM validation CNAMEs and alias A/AAAA records for the apex and `www`
  pointing at the distribution.
- **IAM role `oxidant-site-deploy`**
  (`arn:aws:iam::810738286322:role/oxidant-site-deploy`) — assumed by GitHub
  Actions via the `token.actions.githubusercontent.com` OIDC provider; trust is
  pinned to `repo:OxidantData/Oxidant:ref:refs/heads/main`. Least-privilege
  inline policy: `s3:ListBucket` on the bucket, `s3:GetObject/PutObject/DeleteObject`
  on `bucket/*`, `cloudfront:CreateInvalidation` on the distribution ARN only.
  The workflow reads the ARN from repo variable `AWS_SITE_DEPLOY_ROLE_ARN`.

All resources are tagged `Project=oxidant-site`.

## DNS cutover (founder action at GoDaddy) — PENDING

The domain is registered at GoDaddy and still uses GoDaddy nameservers
(`ns39/ns40.domaincontrol.com`). To go live, switch the domain's nameservers to
the hosted zone's NS records:

- `ns-235.awsdns-29.com`
- `ns-1011.awsdns-62.net`
- `ns-1361.awsdns-42.org`
- `ns-1544.awsdns-01.co.uk`

Until that happens the ACM certificate stays `PENDING_VALIDATION` (its
validation CNAMEs only resolve once the zone is authoritative). That is the
expected pending state — nothing is broken.

### After the NS switch propagates (one-time, ~5 min)

1. Wait for the cert to validate:
   `aws acm wait certificate-validated --region us-east-1 --certificate-arn <cert-arn>`
2. Attach the cert + aliases to the distribution (it was created with the
   default `*.cloudfront.net` cert and no aliases because CloudFront rejects an
   unissued cert). The exact commands are in `setup-site.sh` under
   **PHASE 2 — after DNS cutover**.

## Deploys

`.github/workflows/deploy-site.yml` runs on pushes to `main` touching `site/**`
(or manually): `npm ci` + `npm run build` in `site/`, assumes the deploy role
via OIDC, `aws s3 sync site/dist s3://oxidantdata.com --delete`, then
invalidates `/*` on the distribution.

## Reproducing / auditing the setup

`setup-site.sh` contains the exact AWS CLI commands used to provision all of
the above, parameterized at the top. It is idempotent-ish but intended as a
reviewable record — do not re-run blindly against the live account.

## Cost

At low traffic: ~$1–5/mo (S3 pennies + CloudFront price-class-100 requests/GB
+ one Route53 hosted zone at $0.50/mo). ACM public certs are free.
