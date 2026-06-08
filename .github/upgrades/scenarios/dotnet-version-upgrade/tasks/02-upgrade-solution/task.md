# 02-upgrade-solution: Bump all projects to .NET 10 and apply fixes

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
