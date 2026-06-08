# .NET Version Upgrade

## Preferences
- **Flow Mode**: Automatic
- **Target Framework**: .NET 10.0 (LTS)

## Source Control
- **Source Branch**: main
- **Working Branch**: upgrade-dotnet-10
- **Commit Strategy**: Single Commit at End

## Upgrade Options
**Source**: .github/upgrades/scenarios/dotnet-version-upgrade/upgrade-options.md

### Strategy
- Upgrade Strategy: All-at-Once

## Strategy
**Selected**: All-At-Once
**Rationale**: 16 projects, all SDK-style, all on modern .NET (net8.0 + one source generator on netstandard2.0). Zero incompatible packages, zero high-risk migrations — mechanical TFM bump where multi-targeting libraries would just add noise.

### Execution Constraints
- Single atomic upgrade — all 15 application/library projects updated to net10.0 together; Ookii.Jumbo.Generator stays on netstandard2.0 (Roslyn source-generator requirement)
- Validate full solution build only after all TFM/package updates are applied — no per-project mid-pass validation
- Build-and-fix is one bounded pass: update everything → restore → build → fix all errors/warnings, not iterative retry
- Tests run only after the solution builds clean (Task 03)
- Commit once at the end after all tasks complete and tests pass
