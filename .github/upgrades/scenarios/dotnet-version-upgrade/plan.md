# .NET Version Upgrade Plan

## Overview

**Target**: Upgrade JumboCore solution from .NET 8 to .NET 10.0 (LTS)
**Scope**: 16 projects in `src/JumboCore.sln` — 15 on `net8.0` (class libraries, console servers, ASP.NET Core web apps, NUnit test project) plus 1 source generator (`Ookii.Jumbo.Generator`) on `netstandard2.0` which stays on netstandard2.0 per Roslyn source-generator requirements. All projects are SDK-style. Zero incompatible packages; 4 BCL/tooling packages have recommended version bumps.

### Selected Strategy
**All-At-Once** — All projects upgraded simultaneously in a single operation.
**Rationale**: 16 projects, all SDK-style, all on modern .NET, 0 incompatible packages, 0 high-risk migrations, mechanical TFM bump. Multi-targeting libraries during a Top-Down would just add noise without benefit.

## Tasks

### 01-prerequisites: Verify .NET 10 SDK and toolchain readiness

Verify the .NET 10 SDK is installed and visible to the build environment. Confirm whether a `global.json` exists in the repo (none was found in the initial scan); if it appears later or pins an older SDK, update it to allow .NET 10. Check the solution still builds cleanly on `net8.0` from the new working branch as a baseline before any TFM changes — this isolates upgrade-induced breakages from preexisting issues.

This task is the safety net for everything that follows. If the baseline build fails on `net8.0`, stop and surface the issue to the user before proceeding.

**Done when**:
- `dotnet --list-sdks` shows a 10.0.x SDK installed
- Solution builds with 0 errors on `net8.0` from the `upgrade-dotnet-10` branch
- `global.json` (if present) does not pin to an older SDK

---

### 02-upgrade-solution: Bump all projects to .NET 10 and apply fixes

Update `<TargetFramework>` from `net8.0` to `net10.0` across all 15 application/library projects. Leave `Ookii.Jumbo.Generator` on `netstandard2.0` (Roslyn source generators must target netstandard2.0). Bump the 4 BCL/tooling packages flagged by the assessment to their `10.0.x` equivalents:

- `Microsoft.Extensions.Logging.Debug` `9.0.16` → `10.0.x` (latest stable)
- `Microsoft.VisualStudio.Web.CodeGeneration.Design` `9.0.0` → `10.0.x`
- `System.Diagnostics.EventLog` `9.0.16` → `10.0.x`
- `System.Security.Cryptography.ProtectedData` `9.0.16` → `10.0.x`

Restore packages, build the entire solution, and fix all resulting compilation errors and warnings in a single bounded pass. Per the assessment, expect ~461 `Api.0002` (source-incompatible) and ~79 `Api.0003` (behavioral) flags — the bulk are concentrated in `Ookii.Jumbo`, `Ookii.Jumbo.Dfs`, `Ookii.Jumbo.Jet` and stem from "Legacy Configuration System" (423) and "System Management / WMI" (34). These are typically RPC/configuration/ConfigurationManager surfaces and Windows-only WMI usage that have evolving annotations across .NET versions; many will be `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]`/nullability/obsolete-message tweaks rather than runtime breaks.

**Research starting points**:
- Query the assessment per-project for `Api.0002`/`Api.0003` rule details before editing — many flags are duplicates of the same underlying call site
- `Ookii.Jumbo/Rpc/RpcProxyBuilder.cs` and `Ookii.Jumbo.Jet` (243 issues) are the likely hot spots — touch these last so simpler fixes don't have to roll forward
- Some flags may be platform attribute changes (`[SupportedOSPlatform("windows")]`) on WMI/EventLog APIs — fix at the call site, do not suppress at project level
- All warnings must be fixed (project-level `<TreatWarningsAsErrors>` settings should be respected); never suppress with `#pragma warning disable` or `<NoWarn>` without explicit user approval

**Done when**:
- All 15 projects targeting `net8.0` now target `net10.0`; `Ookii.Jumbo.Generator` remains on `netstandard2.0`
- 4 listed packages updated to their `10.0.x` versions
- `dotnet build` on the solution produces 0 errors and 0 warnings
- No new `#pragma warning disable`, `<NoWarn>`, or null-forgiving operators added without justification recorded in scenario-instructions.md

---

### 03-validate: Full solution build and test pass

Run the full test suite (`Ookii.Jumbo.Test` is the NUnit test project; `Ookii.Jumbo.Test.Tasks` is a supporting library). Confirm the test runner discovers tests against the upgraded `net10.0` runtime and that all tests pass. If pre-existing test failures exist on `net8.0` baseline (captured in task 01), only new failures introduced by the upgrade need to be addressed in this task.

Also verify the source generator (`Ookii.Jumbo.Generator`) is being consumed correctly by its dependents — generator output should still produce valid code under the new compiler/runtime combo.

**Done when**:
- `dotnet test` reports 0 failures (or only the same failures observed on the `net8.0` baseline)
- `Ookii.Jumbo.Generator` is producing source for `Ookii.Jumbo`, `Ookii.Jumbo.Dfs`, `Ookii.Jumbo.Jet`, etc. with no generator-related build errors
- Solution builds with 0 errors and 0 warnings end-to-end
