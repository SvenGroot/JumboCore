# 03-validate: Full solution build and test pass

Run the full test suite (`Ookii.Jumbo.Test` is the NUnit test project; `Ookii.Jumbo.Test.Tasks` is a supporting library). Confirm the test runner discovers tests against the upgraded `net10.0` runtime and that all tests pass. If pre-existing test failures exist on `net8.0` baseline (captured in task 01), only new failures introduced by the upgrade need to be addressed in this task.

Also verify the source generator (`Ookii.Jumbo.Generator`) is being consumed correctly by its dependents — generator output should still produce valid code under the new compiler/runtime combo.

**Done when**:
- `dotnet test` reports 0 failures (or only the same failures observed on the `net8.0` baseline)
- `Ookii.Jumbo.Generator` is producing source for `Ookii.Jumbo`, `Ookii.Jumbo.Dfs`, `Ookii.Jumbo.Jet`, etc. with no generator-related build errors
- Solution builds with 0 errors and 0 warnings end-to-end
