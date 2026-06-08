# Upgrade Options — JumboCore

Assessment: 16 projects, 15 on net8.0 + 1 on netstandard2.0 → net10.0; 0 incompatible packages; 0 high-risk migrations; 5-tier dependency graph; all SDK-style.

## Strategy

### Upgrade Strategy
Mechanical net8→net10 upgrade with no incompatible packages or high-risk migrations; small enough scope to upgrade as one atomic operation.

| Value | Description |
|-------|-------------|
| **All-at-Once** (selected) | Upgrade all 16 projects simultaneously in a single atomic pass. Fastest approach for mechanical upgrades; no multi-targeting overhead. The solution is briefly broken until all projects are updated. |
| Top-Down | Upgrade entry-point apps first, multi-target shared libraries (net8.0;net10.0) to keep the solution buildable throughout, then consolidate libraries to net10.0 only. Adds noise to library project files; useful when CI must stay green or multiple teams work concurrently. |
