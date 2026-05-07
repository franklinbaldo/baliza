@green @ops
Feature: Operational hygiene — doctor, orphans, sync_stats
  These scenarios describe the operational guarantees Baliza relies on
  to know whether the public archive on Internet Archive is consistent.
  Each rule maps to a CLI command shipped under `baliza` and to a
  workflow under `.github/workflows/`.

  Reference: `docs/plans/pncp-resource-pipeline.md` §1 ("Current state").

  Rule: `baliza doctor` proves the archive matches the manifest

    Background:
      Given the IA manifest is reachable

    Scenario: Doctor exits 0 when every canonical row has a 64-hex sha256 and on-archive URL
      Given every canonical row carries a 64-hex `sha256`
      And every canonical row's `parquet_url` and `raw_zip_url` point at archive.org
      When I run `baliza doctor --resource contratos`
      Then the command exits with code 0
      And no findings are reported

    Scenario: Doctor exits 1 if any canonical row is missing its sha256
      Given one canonical row has an empty `sha256`
      When I run `baliza doctor --resource contratos`
      Then the command exits with non-zero code
      And the offending row's identifier appears in stderr

    Scenario: Doctor with --head-check refuses to probe non-archive URLs
      Given a canonical row whose `parquet_url` points at "https://evilarchive.org/x.parquet"
      When I run `baliza doctor --resource contratos --head-check`
      Then no HEAD request is sent to the off-archive host
      And the row is reported as a finding

    Scenario: Doctor failures get one rolling tracking issue, not a flood
      Given the weekly Doctor cron has been red for 5 consecutive runs
      When I list open `ci-failure` issues
      Then there is exactly one open tracking issue
      And every recurrence is recorded as a comment on that issue
      And the issue auto-closes on the first clean Doctor run

  Rule: `baliza orphans --apply` only ever deletes files outside the allow-list

    Scenario: Without --apply the command lists orphans and exits 1
      Given a per-month IA item carries a known-legacy file
      When I run `baliza orphans --resource contratos --months 1`
      Then the legacy filename is printed
      And the command exits with non-zero code
      And no `ia.delete` call is made

    Scenario: --apply requires IA credentials to even start
      Given the environment has no `IA_ACCESS_KEY` or `IA_SECRET_KEY`
      When I run `baliza orphans --apply --months 1`
      Then the command exits before any I/O
      And the output names the missing variables

    Scenario: --apply refuses to act when any month's metadata fetch failed
      Given fetching metadata for one month raises a transient error
      When I run `baliza orphans --apply --months 6`
      Then no `ia.delete` call is made for any month
      And the operator is told the scan was partial

    Scenario: --apply is bounded by --max-deletions
      Given 200 orphan files are visible across the inspected months
      When I run `baliza orphans --apply --max-deletions 50`
      Then the command refuses before deleting anything
      And the operator can re-run with a higher cap deliberately

    Scenario Outline: Allow-listed files are never deleted
      Given the IA item lists "<filename>" alongside genuine orphans
      When I run `baliza orphans --apply` with sufficient cap
      Then "<filename>" is not in the set of deleted files

      Examples:
        | filename                          |
        | contratos-2024-04.parquet         |
        | quarentena-2024-04.csv            |
        | raw-2024-04.zip                   |
        | baliza-pncp-2024-04_files.xml     |

  Rule: `sync_stats.json` reflects only canonical contratos rows

    Scenario: A new canonical contratos upload bumps `total_contracts`
      Given `sync_stats.json` was generated before the upload
      When a new monthly canonical contratos parquet is published
      And `scripts/build_sync_stats.py` is re-run
      Then the `total_contracts` field grew by exactly the new row count
      And `total_quarantine` reflects only contratos rows

    Scenario: A non-contratos row never inflates contratos totals
      Given an atas canonical row exists for the same partition
      When `scripts/build_sync_stats.py` is re-run
      Then `total_contracts` is unchanged
      And the per-resource breakdown carries an "atas" entry under `resources`

    Scenario: An IA outage during refresh leaves the published numbers intact
      Given `web/public/data/sync_stats.json` already shows real numbers
      When the IA manifest is unreachable
      And `scripts/build_sync_stats.py` runs
      Then the script exits non-zero
      And `web/public/data/sync_stats.json` is not overwritten with zeros
