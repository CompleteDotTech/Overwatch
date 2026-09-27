# W&B response guards

This is source-level robustness for Overwatch #5, not evidence that an authorized
W&B or S3 exchange, complete backend suite, or populated UI has passed.

## Pagination metadata

The collector requires `pageInfo` to be an object and `hasNextPage` to be an
actual JSON Boolean. It must not coerce `null`, numbers, strings, arrays, or
objects to a completion decision. The Cursor Connections specification defines
`hasNextPage` as a non-null Boolean:
https://relay.dev/graphql/connections.htm#sec-undefined.PageInfo.Fields

The Boolean and, for a continuing page, the nonempty, nonrepeating string cursor
and nonempty edge list are validated before snapshots from that page are
accepted. A malformed page is an error, not a successfully refreshed source.
Already accepted snapshots from earlier valid pages remain available. This does
not promise transactional validation of every other field in a page.

An invalid response does not revoke registration: `configured_ids` and
`registry_complete` describe the validated registry, while `complete`, source
`status`, and source `refreshed_at` describe this collection attempt. Do not
replace an older successful refresh with a failed attempt's timestamp.

## Response ownership

`WandbPages.fetch_snapshot_page` owns the opened response until it returns the
`Body`. A header-extraction or Content-Length conversion exception closes that
response without reading it, then propagates the exception. Valid responses
transfer ownership to the collector, whose existing `finally` closes the body.
The existing length interpretation and byte-budget rules are unchanged.

No request retries, redirect allowance, credential fallback, run creation,
run finishing, or additional payload reads are introduced. The deadline remains
cooperative: these changes do not cancel an in-flight network/credential call.

## Regressions

`tests/test_kev_laya_wandb_response_guards.py` uses the real collector and the
standalone telemetry validator, synthetic in-memory pages, and an injected
opener. Socket connection attempts are forbidden in this suite. It covers
invalid completion types and page metadata, invalid/repeated cursors, previous
valid-page retention, valid terminal/paginated pages, encoded summary JSON,
known/unknown lengths, source and request/page budgets, identity mismatch,
GraphQL errors, header-error cleanup, ownership transfer, and no added retry.

In a supported, authenticated checkout, run these tests and the unchanged full
CI gates described in [CI_ACCEPTANCE.md](CI_ACCEPTANCE.md). No synthetic result
satisfies the live acceptance boxes in [issue #5](https://github.com/CompleteDotTech/Overwatch/issues/5)
or the fresh producer-to-visible-UI gate in [issue #3](https://github.com/CompleteDotTech/Overwatch/issues/3).

Live qualification must still record the actual endpoint/query shape, bounded
S3/W&B bytes and request/page/read counts, timeout behavior, malformed/outage and
budget-exhaustion cache preservation, and source-registration/freshness
separation, using existing authorized non-production targets. Keep credentials
and private payloads out of source, receipts, logs, and issue comments.
