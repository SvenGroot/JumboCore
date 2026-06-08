# 01-prerequisites: Verify .NET 10 SDK and toolchain readiness

Verify the .NET 10 SDK is installed and visible to the build environment. Confirm whether a `global.json` exists in the repo (none was found in the initial scan); if it appears later or pins an older SDK, update it to allow .NET 10. Check the solution still builds cleanly on `net8.0` from the new working branch as a baseline before any TFM changes — this isolates upgrade-induced breakages from preexisting issues.

This task is the safety net for everything that follows. If the baseline build fails on `net8.0`, stop and surface the issue to the user before proceeding.

**Done when**:
- `dotnet --list-sdks` shows a 10.0.x SDK installed
- Solution builds with 0 errors on `net8.0` from the `upgrade-dotnet-10` branch
- `global.json` (if present) does not pin to an older SDK
