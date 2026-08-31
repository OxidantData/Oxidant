#!/usr/bin/env python3
"""Regenerate `crates/oxidant-spark-compat/databricks-functions.json` from the Databricks manual.

The catalog is the Databricks half of the `oxidant-parity functions` gap report. It is committed
rather than fetched at runtime so the gap number is reproducible offline and a change to the
Databricks surface shows up as a reviewable diff.

Usage (from the repo root):

    curl -sSL -A Mozilla/5.0 \
      https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin \
      -o /tmp/dbx-builtin.html
    python3 crates/oxidant-spark-compat/scripts/scrape-databricks-functions.py /tmp/dbx-builtin.html

Then bump `scraped` below, re-run `oxidant-parity functions`, and update the counts asserted in
`crates/oxidant-spark-compat/src/functions.rs::catalog_parses_and_scopes`.

Name resolution: the manual's link *text* is authoritative, not the URL slug. Slugs disambiguate
overloads (`date_add3` is the three-argument `date_add`, `decode_cs` is `decode(expr, charSet)`,
`spark_partition` is `spark_partition_id()`), so entries are keyed by the name in the rendered
signature and overload pages are merged.
"""
import os
import sys
import re, json, collections, html as htmlmod

SRC = "https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin"
h = open(sys.argv[1] if len(sys.argv) > 1 else 'dbx-builtin.html').read()

# Walk the page in document order. Each function link carries BOTH a URL slug and the rendered
# signature; the signature is authoritative for the name (the slug disambiguates overloads:
# `date_add3` is the 3-arg `date_add`, `decode_cs` is `decode(expr, charSet)`, `spark_partition`
# is `spark_partition_id()`).
tok = re.compile(r'<h[23][^>]*>([^<]+)|href=/aws/en/sql/language-manual/functions/([a-z0-9_]+)>([^<]*)')
sig = re.compile(r'^([a-z_][a-z0-9_]*)\s*\(')

texts = collections.defaultdict(list)     # slug -> [link text]
cats = collections.defaultdict(list)      # slug -> [category]
cur = None
for m in tok.finditer(h):
    head, slug, text = m.group(1), m.group(2), m.group(3)
    if head:
        cur = head.strip()
    elif slug and cur:
        texts[slug].append(htmlmod.unescape(text.strip()))
        if cur not in cats[slug]:
            cats[slug].append(cur)

# Grammar constructs that render like a call but are never registry entries.
SYNTAX_OVERRIDE = {'cast', 'try_cast', 'case', 'extract', 'cube'}
# Spark registers these as callable functions as well as operators (`SELECT like('abc','a%')`),
# so the bare operator link text must not demote them to syntax.
# Spark registers these as callable functions even though the manual renders them as operator
# forms: `SELECT like('abc','a%')`, `SELECT rlike('abc','a.')`, `SELECT collation('a')`. Three of
# them (`regexp`, `regexp_like`, `rlike`) are already in oxidant's registry via
# `spark_functions/spark_regex_misc.rs`, so demoting them to syntax would under-report coverage.
FUNCTION_OVERRIDE = {'like', 'ilike', 'regexp', 'regexp_like', 'rlike', 'collate', 'collation'}

canon = {}      # slug -> (name, is_function, display)
for slug, tt in texts.items():
    name, disp = None, None
    for t in tt:
        m = sig.match(t)
        if m:
            name, disp = m.group(1), t
            break
    if slug in FUNCTION_OVERRIDE:
        canon[slug] = (slug, True, disp or f'{slug}(…)')
    elif slug.startswith('match_recognize_'):
        # MATCH_RECOGNIZE row-pattern navigators render as `first(...)`/`prev(...)` but are
        # clause-scoped, not registry entries — never merge them onto the real aggregates.
        canon[slug] = (slug, True, disp)
    elif name and name not in SYNTAX_OVERRIDE:
        canon[slug] = (name, True, disp)
    else:
        # Operator or clause syntax: keep the slug as the identifier, record how it renders.
        display = next((t for t in tt if t), slug)
        canon[slug] = (name or slug, False, display)

# Merge slugs that resolve to the same function name (overload pages).
merged = collections.OrderedDict()
for slug in sorted(texts):
    name, is_fn, disp = canon[slug]
    key = name if is_fn else slug
    if key not in merged:
        merged[key] = {'name': name if is_fn else name,
                       'is_function': is_fn,
                       'display': disp,
                       'categories': [],
                       'slugs': []}
    e = merged[key]
    e['slugs'].append(slug)
    for c in cats[slug]:
        if c not in e['categories']:
            e['categories'].append(c)

VECTOR = {'vector_search','vector_cosine_similarity','vector_inner_product',
          'vector_l2_distance','vector_norm','vector_normalize','vector_avg','vector_sum'}
PLATFORM_SOURCE = {'remote_query','http_request','cloud_files_state','event_log','table_changes'}
FILES = {'copy_file','create_file','list_files','to_file','try_copy_file','try_to_file'}
WORKSPACE = {'secret','try_secret','list_secrets','is_member','is_account_group_member',
             'current_metastore','current_recipient','java_method','reflect','try_reflect',
             'isearch','search','collations','sql_keywords','current_version',
             'agg','measure'}
MATCH_RECOGNIZE = {'classifier','match_number','match_recognize_first','match_recognize_last',
                   'match_recognize_next','match_recognize_prev'}


def exclusion(key, e):
    n = e['name']
    if not e['is_function']:
        return ('syntax',
                'Operator or clause syntax (`%s`), not a function-registry entry — parser surface.'
                % e['display'])
    if n.startswith('ai_') or n in VECTOR:
        return ('ai-vector', 'Requires Databricks Model Serving / Vector Search.')
    if n.startswith('read_') or n in PLATFORM_SOURCE:
        return ('platform-source', 'Requires the Databricks control plane or an external service.')
    if n in FILES:
        return ('file', 'Requires Unity Catalog volumes.')
    if n in WORKSPACE:
        return ('workspace',
                'Bound to Databricks workspace identity, secrets, metric views, or JVM reflection.')
    if n in MATCH_RECOGNIZE:
        return ('match-recognize',
                'Belongs to the MATCH_RECOGNIZE clause project, not the function registry.')
    if n.startswith(('kll_','theta_','tuple_','hll_','bitmap_','approx_top_k')) or n == 'count_min_sketch':
        return ('datasketches',
                'Requires byte-compatible Apache DataSketches serialization; deferred sub-project.')
    return None

funcs, counts = [], collections.Counter()
for key in sorted(merged):
    e = merged[key]
    rec = {'name': e['name'], 'categories': e['categories'], 'doc_slugs': sorted(e['slugs'])}
    if e['display'] and e['display'] != e['name']:
        rec['signature'] = e['display']
    ex = exclusion(key, e)
    if ex:
        rec.update(in_scope=False, excluded_reason=ex[0], excluded_detail=ex[1])
        counts[ex[0]] += 1
    else:
        rec['in_scope'] = True
        counts['in_scope'] += 1
    funcs.append(rec)

doc = {
    '_comment': ('Databricks SQL builtin-function surface, scraped from the language manual. One '
                 'entry per distinct function name: overload pages (date_add/date_add3, '
                 'decode/decode_cs) are merged, and the name comes from the rendered signature, '
                 'not the URL slug. `in_scope` marks what Oxidant intends to implement; excluded '
                 'entries name why. See docs/databricks-functions.md for how to regenerate.'),
    'source': SRC,
    'scraped': '2026-08-29',
    'total': len(funcs),
    'counts': dict(counts),
    'functions': funcs,
}
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, 'databricks-functions.json')
with open(out, 'w') as f:
    json.dump(doc, f, indent=2); f.write('\n')
print('distinct names:', len(funcs))
print(json.dumps(dict(counts), indent=1))
