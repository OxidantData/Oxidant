#!/usr/bin/env bash
# Provision static hosting for the Oxidant marketing site on AWS:
# private S3 + CloudFront (OAC) + ACM (us-east-1) + Route53 + GitHub OIDC deploy role.
#
# This is the exact command sequence used to set up the live resources on
# 2026-08-06 (account 810738286322), cleaned up and parameterized. It is kept
# for review/reproducibility — the resources already exist, so re-running it
# as-is will fail on name collisions (bucket, role, OAC, ...).
#
# Usage:
#   ./setup-site.sh           # PHASE 1: everything except the cert-gated pieces
#   ./setup-site.sh phase2    # AFTER the GoDaddy NS cutover: attach cert + aliases
set -euo pipefail

# ---- Parameters -------------------------------------------------------------
DOMAIN="oxidantdata.com"
WWW_DOMAIN="www.${DOMAIN}"
BUCKET="${DOMAIN}"                 # fallback if taken: oxidantdata-site-810738286322
AWS_REGION="us-east-1"             # all site resources live here (ACM must be us-east-1 for CloudFront)
AWS_ACCOUNT_ID="810738286322"
GITHUB_REPO="OxidantData/Oxidant"
ROLE_NAME="oxidant-site-deploy"
TAG_KEY="Project"
TAG_VALUE="oxidant-site"
DISTRIBUTION_ID="E3BG86EZYJNHTO"   # output of phase 1; needed by phase 2 + workflow
CERT_ARN="arn:aws:acm:us-east-1:810738286322:certificate/ed47b6a8-a155-454e-99cc-2546a92db488"
HOSTED_ZONE_ID="Z0014528AK93TYSRKI11"
CLOUDFRONT_ALIAS_ZONE_ID="Z2FDTNDATAQYW2"  # fixed AWS zone ID for CloudFront alias targets
GH_OIDC_THUMBPRINT="6938fd4d98bab03faadb97b34396831e3780aea1"

if [ "${1:-}" != "phase2" ]; then
# =============================================================================
# PHASE 1 — run once. After this, DNS cutover at GoDaddy is the pending step.
# =============================================================================

# --- 1. S3 bucket: private, no website hosting, BlockPublicAccess all ON -----
aws s3api create-bucket --bucket "$BUCKET" --region "$AWS_REGION"
aws s3api put-public-access-block --bucket "$BUCKET" --region "$AWS_REGION" \
  --public-access-block-configuration \
  'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'
aws s3api put-bucket-tagging --bucket "$BUCKET" --region "$AWS_REGION" \
  --tagging "TagSet=[{Key=$TAG_KEY,Value=$TAG_VALUE}]"

# --- 2. Route53 hosted zone --------------------------------------------------
aws route53 create-hosted-zone --name "$DOMAIN" \
  --caller-reference "${DOMAIN}-$(date +%s)" \
  --hosted-zone-config 'Comment="Oxidant marketing site"'
# -> note the returned hosted zone ID and the 4 DelegationSet NS records
aws route53 change-tags-for-resource --resource-type hostedzone \
  --resource-id "$HOSTED_ZONE_ID" --add-tags "Key=$TAG_KEY,Value=$TAG_VALUE"

# --- 3. ACM cert (us-east-1, DNS validation) --------------------------------
aws acm request-certificate --region "$AWS_REGION" \
  --domain-name "$DOMAIN" --subject-alternative-names "$WWW_DOMAIN" \
  --validation-method DNS --tags "Key=$TAG_KEY,Value=$TAG_VALUE"

# Create the DNS validation CNAMEs in the hosted zone (generated from the cert).
aws acm describe-certificate --region "$AWS_REGION" --certificate-arn "$CERT_ARN" \
  --query 'Certificate.DomainValidationOptions[].ResourceRecord' > /tmp/acm-rrs.json
python3 - "$HOSTED_ZONE_ID" <<'PYEOF'
import json, subprocess, sys
rrs = json.load(open("/tmp/acm-rrs.json"))
batch = {"Changes": [
    {"Action": "UPSERT", "ResourceRecordSet": {
        "Name": r["Name"], "Type": r["Type"], "TTL": 300,
        "ResourceRecords": [{"Value": r["Value"]}]}}
    for r in rrs]}
subprocess.run(["aws", "route53", "change-resource-record-sets",
                "--hosted-zone-id", sys.argv[1],
                "--change-batch", json.dumps(batch)], check=True)
PYEOF
# NOTE: the cert stays PENDING_VALIDATION until the GoDaddy NS cutover makes
# this hosted zone authoritative. Expected — do not work around it.

# --- 4. CloudFront: OAC + distribution --------------------------------------
aws cloudfront create-origin-access-control --origin-access-control-config \
  "Name=${DOMAIN//./-}-oac,Description=OAC for ${DOMAIN} S3 origin,SigningProtocol=sigv4,SigningBehavior=always,OriginAccessControlOriginType=s3"
# -> OAC id: EZ4MLYMCDE52V
OAC_ID="EZ4MLYMCDE52V"

# CloudFront refuses a not-yet-issued ACM cert (InvalidViewerCertificate), so
# the distribution starts with the default *.cloudfront.net cert and NO
# aliases; phase 2 attaches cert + aliases after the NS cutover.
cat > /tmp/dist-config.json <<EOF
{
  "CallerReference": "${DOMAIN}-$(date +%Y%m%d)",
  "Comment": "Oxidant marketing site (${DOMAIN})",
  "Enabled": true,
  "DefaultRootObject": "index.html",
  "Aliases": {"Quantity": 0},
  "Origins": {
    "Quantity": 1,
    "Items": [{
      "Id": "s3-${DOMAIN}",
      "DomainName": "${BUCKET}.s3.${AWS_REGION}.amazonaws.com",
      "OriginPath": "",
      "S3OriginConfig": {"OriginAccessIdentity": ""},
      "OriginAccessControlId": "${OAC_ID}",
      "ConnectionAttempts": 3,
      "ConnectionTimeout": 10
    }]
  },
  "DefaultCacheBehavior": {
    "TargetOriginId": "s3-${DOMAIN}",
    "ViewerProtocolPolicy": "redirect-to-https",
    "AllowedMethods": {
      "Quantity": 3,
      "Items": ["GET", "HEAD", "OPTIONS"],
      "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]}
    },
    "CachePolicyId": "658327ea-f89d-4fab-a63d-7e88639e58f6",
    "Compress": true
  },
  "CustomErrorResponses": {
    "Quantity": 2,
    "Items": [
      {"ErrorCode": 403, "ResponsePagePath": "/index.html", "ResponseCode": "200", "ErrorCachingMinTTL": 10},
      {"ErrorCode": 404, "ResponsePagePath": "/index.html", "ResponseCode": "200", "ErrorCachingMinTTL": 10}
    ]
  },
  "PriceClass": "PriceClass_100",
  "ViewerCertificate": {"CloudFrontDefaultCertificate": true},
  "Restrictions": {"GeoRestriction": {"RestrictionType": "none", "Quantity": 0}},
  "HttpVersion": "http2and3",
  "IsIPV6Enabled": true
}
EOF
aws cloudfront create-distribution --distribution-config file:///tmp/dist-config.json
# -> distribution id E3BG86EZYJNHTO, domain d2a7770knck57q.cloudfront.net
aws cloudfront tag-resource \
  --resource "arn:aws:cloudfront::${AWS_ACCOUNT_ID}:distribution/${DISTRIBUTION_ID}" \
  --tags "Items=[{Key=$TAG_KEY,Value=$TAG_VALUE}]"

# Bucket policy: allow this distribution (OAC) to read the bucket.
DIST_DOMAIN="d2a7770knck57q.cloudfront.net"
cat > /tmp/bucket-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowCloudFrontOAC",
    "Effect": "Allow",
    "Principal": {"Service": "cloudfront.amazonaws.com"},
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::${BUCKET}/*",
    "Condition": {"StringEquals": {
      "AWS:SourceArn": "arn:aws:cloudfront::${AWS_ACCOUNT_ID}:distribution/${DISTRIBUTION_ID}"
    }}
  }]
}
EOF
aws s3api put-bucket-policy --bucket "$BUCKET" --region "$AWS_REGION" \
  --policy file:///tmp/bucket-policy.json

# --- 5. Route53 alias A/AAAA records (live only after NS cutover) ------------
cat > /tmp/alias-records.json <<EOF
{
  "Changes": [
    {"Action": "UPSERT", "ResourceRecordSet": {"Name": "${DOMAIN}", "Type": "A",
      "AliasTarget": {"HostedZoneId": "${CLOUDFRONT_ALIAS_ZONE_ID}", "DNSName": "${DIST_DOMAIN}", "EvaluateTargetHealth": false}}},
    {"Action": "UPSERT", "ResourceRecordSet": {"Name": "${DOMAIN}", "Type": "AAAA",
      "AliasTarget": {"HostedZoneId": "${CLOUDFRONT_ALIAS_ZONE_ID}", "DNSName": "${DIST_DOMAIN}", "EvaluateTargetHealth": false}}},
    {"Action": "UPSERT", "ResourceRecordSet": {"Name": "${WWW_DOMAIN}", "Type": "A",
      "AliasTarget": {"HostedZoneId": "${CLOUDFRONT_ALIAS_ZONE_ID}", "DNSName": "${DIST_DOMAIN}", "EvaluateTargetHealth": false}}},
    {"Action": "UPSERT", "ResourceRecordSet": {"Name": "${WWW_DOMAIN}", "Type": "AAAA",
      "AliasTarget": {"HostedZoneId": "${CLOUDFRONT_ALIAS_ZONE_ID}", "DNSName": "${DIST_DOMAIN}", "EvaluateTargetHealth": false}}}
  ]
}
EOF
aws route53 change-resource-record-sets --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///tmp/alias-records.json

# --- 6. GitHub Actions OIDC provider + least-privilege deploy role -----------
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list "$GH_OIDC_THUMBPRINT" \
  --tags "Key=$TAG_KEY,Value=$TAG_VALUE"

cat > /tmp/trust.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Federated": "arn:aws:iam::${AWS_ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"},
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {"StringEquals": {
      "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
      "token.actions.githubusercontent.com:sub": "repo:${GITHUB_REPO}:ref:refs/heads/main"
    }}
  }]
}
EOF
aws iam create-role --role-name "$ROLE_NAME" \
  --assume-role-policy-document file:///tmp/trust.json \
  --description "GitHub Actions OIDC deploy role for ${DOMAIN} site (S3+CloudFront)" \
  --tags "Key=$TAG_KEY,Value=$TAG_VALUE"

cat > /tmp/deploy-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "S3SiteSync", "Effect": "Allow",
     "Action": ["s3:ListBucket"],
     "Resource": "arn:aws:s3:::${BUCKET}"},
    {"Sid": "S3SiteObjects", "Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
     "Resource": "arn:aws:s3:::${BUCKET}/*"},
    {"Sid": "CloudFrontInvalidate", "Effect": "Allow",
     "Action": "cloudfront:CreateInvalidation",
     "Resource": "arn:aws:cloudfront::${AWS_ACCOUNT_ID}:distribution/${DISTRIBUTION_ID}"}
  ]
}
EOF
aws iam put-role-policy --role-name "$ROLE_NAME" \
  --policy-name "$ROLE_NAME" --policy-document file:///tmp/deploy-policy.json

# Repo variable consumed by .github/workflows/deploy-site.yml
gh variable set AWS_SITE_DEPLOY_ROLE_ARN --repo "$GITHUB_REPO" \
  --body "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}"

echo "Phase 1 done. Now switch the GoDaddy nameservers to the hosted zone's NS"
echo "records, then run: $0 phase2"
exit 0
fi

# =============================================================================
# PHASE 2 — AFTER the GoDaddy NS cutover: cert validates, attach it + aliases.
# =============================================================================
aws acm wait certificate-validated --region "$AWS_REGION" --certificate-arn "$CERT_ARN"

aws cloudfront get-distribution-config --id "$DISTRIBUTION_ID" > /tmp/dist-current.json
python3 - "$CERT_ARN" <<'PYEOF'
import json, sys
doc = json.load(open("/tmp/dist-current.json"))
cfg = doc["DistributionConfig"]
cfg["Aliases"] = {"Quantity": 2, "Items": ["oxidantdata.com", "www.oxidantdata.com"]}
cfg["ViewerCertificate"] = {
    "ACMCertificateArn": sys.argv[1],
    "SSLSupportMethod": "sni-only",
    "MinimumProtocolVersion": "TLSv1.2_2021",
}
json.dump(cfg, open("/tmp/dist-update.json", "w"), indent=2)
PYEOF
ETAG=$(python3 -c 'import json; print(json.load(open("/tmp/dist-current.json"))["ETag"])')
aws cloudfront update-distribution --id "$DISTRIBUTION_ID" \
  --if-match "$ETAG" --distribution-config file:///tmp/dist-update.json
echo "Distribution updated: aliases + ACM cert attached. Site live at https://${DOMAIN}/"
