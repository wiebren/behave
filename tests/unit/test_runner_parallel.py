# -*- coding: UTF-8 -*-
"""
Unit tests for :mod:`behave.runner_parallel` (parallel test runner).

Covers the process-free parts: work-item building, format resolution,
result merging, hook validation, runner-alias wiring and configuration
support.
"""

import pytest

from behave.__main__ import has_explicit_runner, select_runner_class_name
from behave.configuration import Configuration, DEFAULT_RUNNER_CLASS_NAME
from behave.exception import ConfigError
from behave.model_type import FileLocation
from behave.runner_parallel import (
    ParallelRunner,
    ScenarioInfo,
    UndefinedStepInfo,
    WorkerOutput,
    group_locations_by_filename,
    make_result,
    merge_status_counts,
    resolve_worker_formats,
    select_outfile_bound_formats,
    select_picklable_params,
    select_summary_reporter,
)
from behave.runner_plugin import RunnerPlugin
from behave.runner_util import make_undefined_step_snippets
from behave.reporter.summary import SummaryReporterV1, SummaryReporterV2


def make_config(command_args=None, **kwargs):
    return Configuration(command_args or [], load_config=False, **kwargs)


# -----------------------------------------------------------------------------
# WORK ITEMS: Scenario selection by line number must survive (finding 1).
# -----------------------------------------------------------------------------
class TestGroupLocationsByFilename:
    def test_groups_locations_of_one_feature_file(self):
        locations = [FileLocation("alice.feature", 12),
                     FileLocation("bob.feature"),
                     FileLocation("alice.feature", 30)]
        grouped = group_locations_by_filename(locations)
        assert list(grouped.keys()) == ["alice.feature", "bob.feature"]
        assert grouped["alice.feature"] == ["alice.feature:12",
                                            "alice.feature:30"]
        assert grouped["bob.feature"] == ["bob.feature"]

    def test_keeps_line_numbers_in_location_text(self):
        grouped = group_locations_by_filename([FileLocation("a.feature", 5)])
        assert grouped["a.feature"] == ["a.feature:5"]

    def test_works_with_location_strings(self):
        grouped = group_locations_by_filename(["a.feature", "b.feature"])
        assert list(grouped.keys()) == ["a.feature", "b.feature"]


# -----------------------------------------------------------------------------
# WORKER FORMAT RESOLUTION:
# -----------------------------------------------------------------------------
class TestResolveWorkerFormats:
    def test_pretty_is_replaced_by_plain(self):
        worker_formats, notes = resolve_worker_formats(["pretty"])
        assert worker_formats == ["plain"]
        assert any("plain" in note and "pretty" in note for note in notes)

    def test_plain_and_progress_pass_through(self):
        worker_formats, notes = resolve_worker_formats(["plain", "progress"])
        assert worker_formats == ["plain", "progress"]
        assert not notes

    @pytest.mark.parametrize("format_name",
                             ["json", "json.pretty", "rerun", "steps.usage"])
    def test_unsupported_format_is_rejected(self, format_name):
        # -- FINDING 4: Must not silently skip (no output is data-loss).
        with pytest.raises(ConfigError, match=format_name):
            resolve_worker_formats([format_name, "plain"])

    def test_outfile_bound_format_is_rejected(self):
        # -- FINDING 4: An --outfile is never written by workers.
        with pytest.raises(ConfigError, match="outfile"):
            resolve_worker_formats(["plain"], outfile_bound=[True])

    def test_outfile_bound_check_is_positional(self):
        # -- FINDING 5: Only the format bound to the outfile is rejected.
        with pytest.raises(ConfigError, match="progress"):
            resolve_worker_formats(["plain", "progress"],
                                   outfile_bound=[False, True])

    def test_console_bound_formats_are_accepted(self):
        worker_formats, _notes = resolve_worker_formats(
            ["plain", "progress"], outfile_bound=[False, False])
        assert worker_formats == ["plain", "progress"]

    def test_empty_result_falls_back_to_plain(self):
        worker_formats, _notes = resolve_worker_formats([])
        assert worker_formats == ["plain"]

    def test_duplicates_are_removed(self):
        worker_formats, _notes = resolve_worker_formats(["pretty", "plain"])
        assert worker_formats == ["plain"]


class TestSelectOutfileBoundFormats:
    def test_named_stream_opener_is_outfile_bound(self):
        config = make_config(["-f", "plain", "-o", "out.txt"])
        assert select_outfile_bound_formats(config) == [True]

    def test_default_stdout_opener_is_not_outfile_bound(self):
        config = make_config(["-f", "plain"])
        assert select_outfile_bound_formats(config) == [False]


# -----------------------------------------------------------------------------
# RESULT MERGING:
# -----------------------------------------------------------------------------
class TestMergeStatusCounts:
    def test_merges_counts_key_wise(self):
        target = {"passed": 1, "failed": 0}
        merge_status_counts(target, {"passed": 2, "failed": 1})
        assert target == {"passed": 3, "failed": 1}

    def test_merges_unknown_keys(self):
        target = {"passed": 1}
        merge_status_counts(target, {"error": 2})
        assert target == {"passed": 1, "error": 2}


class TestUndefinedStepInfo:
    def test_works_with_undefined_step_snippets(self):
        snippets = make_undefined_step_snippets(
            [UndefinedStepInfo("given", "some step")])
        assert len(snippets) == 1
        assert "some step" in snippets[0]

    def test_snippet_generation_can_escape_quotes(self):
        # -- HINT: make_undefined_step_snippet() mutates step.name.
        snippets = make_undefined_step_snippets(
            [UndefinedStepInfo("given", "it's quoted")])
        assert r"it\'s quoted" in snippets[0]

    def test_deduplicates_in_set(self):
        infos = {UndefinedStepInfo("given", "a step"),
                 UndefinedStepInfo("given", "a step"),
                 UndefinedStepInfo("when", "a step")}
        assert len(infos) == 2

    def test_is_sortable(self):
        infos = sorted([UndefinedStepInfo("when", "b"),
                        UndefinedStepInfo("given", "a")])
        assert infos[0].step_type == "given"


class TestScenarioInfo:
    def test_duck_types_scenario_for_summary_reporter(self):
        config = make_config()
        reporter = SummaryReporterV1(config)
        reporter.failed_scenarios.append(ScenarioInfo("a.feature:3", "S1"))
        # -- MUST NOT RAISE: Uses scenario.location and scenario.name.
        reporter.print_failing_scenarios()


class TestMakeResult:
    def test_defaults_are_a_not_run_failure(self):
        result = make_result("some.feature")
        assert result["failed"] is True
        assert result["status"] is None
        assert result["worker_setup_failed"] is False
        assert result["fatal_error"] is False
        assert result["hook_failures"] == 0
        assert result["worker_init_hook_failures"] == 0


class TestSelectSummaryReporter:
    def test_selects_summary_reporter_v1(self):
        config = make_config()
        reporter = SummaryReporterV1(config)
        assert select_summary_reporter([object(), reporter]) is reporter

    def test_returns_none_without_summary_reporter(self):
        assert select_summary_reporter([object()]) is None

    def test_rejects_unsupported_summary_reporter(self):
        # -- FINDING 10: V2 internals differ; must not crash mid-run.
        config = make_config()
        with pytest.raises(ConfigError, match="SummaryReporterV2"):
            select_summary_reporter([SummaryReporterV2(config)])


class TestSelectPicklableParams:
    def test_keeps_picklable_params(self):
        assert select_picklable_params({"a": 1, "b": "x"}) == {"a": 1, "b": "x"}

    def test_drops_non_picklable_params(self):
        selected = select_picklable_params({"good": 1, "bad": lambda: None})
        assert selected == {"good": 1}


class TestWorkerOutput:
    def test_drain_returns_and_clears_text(self):
        output = WorkerOutput()
        output.write("hello")
        assert output.drain() == "hello"
        assert output.drain() == ""


# -----------------------------------------------------------------------------
# PARENT-SIDE RESULT PROCESSING:
# -----------------------------------------------------------------------------
class TestProcessResult:
    @staticmethod
    def make_runner():
        runner = ParallelRunner(make_config())
        runner.context = None
        return runner

    def test_worker_init_failures_are_counted_once_per_worker(self):
        # -- FINDING 2: Must not be lost, must not be counted per task.
        runner = self.make_runner()
        for _ in range(3):
            result = make_result("a.feature", worker_id=0,
                                 worker_init_hook_failures=1)
            runner._process_result(result, None, set())
        runner.worker_hook_failures += sum(runner._worker_init_failures.values())
        assert runner.worker_hook_failures == 1

    def test_worker_init_failures_are_counted_per_worker(self):
        runner = self.make_runner()
        for worker_id in (0, 1):
            result = make_result("a.feature", worker_id=worker_id,
                                 worker_init_hook_failures=1)
            runner._process_result(result, None, set())
        runner.worker_hook_failures += sum(runner._worker_init_failures.values())
        assert runner.worker_hook_failures == 2

    def test_task_hook_failures_are_counted_per_task(self):
        runner = self.make_runner()
        for _ in range(2):
            result = make_result("a.feature", worker_id=0, hook_failures=1)
            runner._process_result(result, None, set())
        assert runner.worker_hook_failures == 2

    def test_result_without_status_is_not_merged(self):
        # -- FINDING 6: A feature that did not run keeps status=None.
        config = make_config()
        summary_reporter = SummaryReporterV1(config)
        runner = self.make_runner()
        result = make_result("a.feature", worker_id=0,
                             feature_summary={"passed": 1})
        runner._process_result(result, summary_reporter, set())
        assert summary_reporter.feature_summary["passed"] == 0


# -----------------------------------------------------------------------------
# HOOK VALIDATION:
# -----------------------------------------------------------------------------
class TestValidateParallelHooks:
    @staticmethod
    def make_runner(hooks):
        runner = ParallelRunner(make_config())
        runner.hooks = dict(hooks)
        return runner

    @staticmethod
    def hook(context):
        pass

    def test_passes_without_any_hooks(self):
        self.make_runner({})._validate_parallel_hooks()  # -- SHOULD NOT RAISE

    def test_fails_with_before_all_only(self):
        runner = self.make_runner({"before_all": self.hook})
        with pytest.raises(ConfigError, match="before_all"):
            runner._validate_parallel_hooks()

    def test_fails_with_after_all_only(self):
        runner = self.make_runner({"after_all": self.hook})
        with pytest.raises(ConfigError, match="after_all"):
            runner._validate_parallel_hooks()

    @pytest.mark.parametrize("counterpart",
                             ["before_parallel", "before_worker"])
    def test_passes_with_before_all_and_counterpart(self, counterpart):
        runner = self.make_runner({"before_all": self.hook,
                                   counterpart: self.hook})
        runner._validate_parallel_hooks()  # -- SHOULD NOT RAISE

    @pytest.mark.parametrize("counterpart",
                             ["after_parallel", "after_worker"])
    def test_passes_with_after_all_and_counterpart(self, counterpart):
        runner = self.make_runner({"after_all": self.hook,
                                   counterpart: self.hook})
        runner._validate_parallel_hooks()  # -- SHOULD NOT RAISE

    def test_all_hooks_are_checked_independently(self):
        runner = self.make_runner({"before_all": self.hook,
                                   "before_worker": self.hook,
                                   "after_all": self.hook})
        with pytest.raises(ConfigError, match="after_all"):
            runner._validate_parallel_hooks()

    def test_injected_default_hooks_are_not_user_defined(self):
        # -- Runner.load_hooks() injects "before_all",
        # ParallelRunner.load_hooks() injects "before_parallel".
        # Neither may satisfy nor trigger the explicitness rule.
        runner = ParallelRunner(make_config())
        runner.hooks = {"before_all": runner.before_all_default_hook,
                        "before_parallel": runner.before_all_default_hook}
        runner._validate_parallel_hooks()  # -- SHOULD NOT RAISE

    def test_injected_before_parallel_does_not_satisfy_before_all(self):
        runner = ParallelRunner(make_config())
        runner.hooks = {"before_all": self.hook,
                        "before_parallel": runner.before_all_default_hook}
        with pytest.raises(ConfigError, match="before_all"):
            runner._validate_parallel_hooks()


# -----------------------------------------------------------------------------
# WIRING:
# -----------------------------------------------------------------------------
class TestRunnerWiring:
    def test_parallel_alias_loads_parallel_runner(self):
        config = make_config(["--runner=parallel"])
        assert isinstance(RunnerPlugin().make_runner(config), ParallelRunner)

    def test_configuration_keeps_command_args(self):
        config = make_config(["--jobs=3", "features"])
        assert config.command_args == ["--jobs=3", "features"]
        assert config.jobs == 3

    def test_configuration_keeps_command_kwargs_and_load_config(self):
        # -- FINDING 7: Workers must be able to rebuild the configuration.
        config = Configuration([], load_config=False, tags="@one")
        assert config.command_kwargs == {"tags": "@one"}
        assert config.command_load_config is False


class TestAutoSelectRunner:
    @pytest.mark.parametrize("command_args", [
        ["--runner=behave.runner:Runner"],
        ["--runner", "behave.runner:Runner"],
        ["-r", "behave.runner:Runner"],
        ["-rbehave.runner:Runner"],
    ])
    def test_explicit_runner_is_detected(self, command_args):
        # -- FINDING 8: An explicit runner must not be overridden.
        assert has_explicit_runner(make_config(command_args)) is True

    def test_default_runner_is_not_explicit(self):
        assert has_explicit_runner(make_config(["--jobs=2"])) is False

    def test_runner_alias_is_resolved(self):
        # -- FINDING 8: A project may alias "default" to its own runner.
        config = make_config()
        config.runner_aliases["default"] = "my.pkg:CustomRunner"
        config.runner = "default"
        assert select_runner_class_name(config) == "my.pkg:CustomRunner"

    def test_default_runner_name_resolves_to_default_class(self):
        config = make_config()
        assert select_runner_class_name(config) == DEFAULT_RUNNER_CLASS_NAME
