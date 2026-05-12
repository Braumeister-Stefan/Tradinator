# Bugfixing Principles

## Diagnosis
- Trace the symptom to the code path that generates the failing artifact.
- Test the initial hypothesis against actual code before acting on it.
- Read third-party library source to establish the true format contract.
- Version mismatches between internal format assumptions and library behavior are high-priority suspects.
- An empty response body indicates a malformed request, not a missing resource.

## Validation
- A systematic validator pass reveals hidden assumptions before any fix is scoped.
- Classify each finding by falsifiability and severity before prioritizing.
- High-severity findings are ranked by blast radius, not probability of occurrence.
- Findings that share a symptom require independent verification of each hypothesis.

## Fixing
- Scope the fix to the minimal set of assignments that produced the root cause.
- Update stale comments alongside code fixes to prevent future misdiagnosis.
- Remove dead code exposed by a fix; do not leave orphaned intermediaries.
- The fix must satisfy the library's documented contract, not an inferred one.

## Risk
- Handling transient failures identically to permanent failures produces silent data loss.
- Empty API responses are ambiguous; never attribute them to a single cause.
- File writes without crash-safety create unrecoverable corruption on process interruption.
- A fixed inter-request delay does not guarantee compliance under burst conditions.

## Strong Assumptions
- When a library embeds date strings verbatim in URL path segments, a date containing slashes will return an empty response body rather than a structured error code.
- A validator finding that matches the symptom description does not guarantee the fix direction derived from that finding is correct without verifying it against the actual code.
- Zero-bar API responses cannot be attributed exclusively to non-trading days; rate limiting and malformed parameters produce an identical response.
- If both primary and fallback fetch paths fail due to a transient condition, an instrument will be permanently removed unless the failure mode is explicitly classified.
- A stale inline comment describing an obsolete API format will cause future debuggers to apply the wrong normalisation before they read the library source.
