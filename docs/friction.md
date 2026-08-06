# Friction log

Every job ends by appending **one line** here. That is wall rule 9.

## Why this file exists

Friction is the thing that slowed a job down and will slow the next one down
too, unless somebody writes it where the next person will read it. Left
unwritten it gets re-discovered every few weeks by whoever happens to hit it,
and the cost is paid again each time.

This is not a complaint box and not a changelog. A line earns its place here
only if it would have saved time had it been known at the *start* of the job.

## Format

`| date | what slowed the job down | becomes | note |`

The **becomes** column must be exactly one of:

| Value | Meaning | Where it lands |
|---|---|---|
| **rule** | A standing constraint everyone must follow | THE WALL |
| **recipe** | A known-good sequence worth writing down once | a skill |
| **tool** | A script that removes the manual step entirely | `scripts/` |
| **alarm** | Something a machine should refuse, not a human remember | a hook |

If a job hit no friction, log that too — a clean run is a data point.

## Entries

| Date | What slowed the job down | Becomes | Note |
|---|---|---|---|
