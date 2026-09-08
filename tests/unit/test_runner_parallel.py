# -*- coding: UTF-8 -*-
"""
Unit tests for :mod:`behave.runner_parallel` (parallel test runner).

Covers the process-free parts: format resolution, result merging,
hook validation, runner-alias wiring and configuration support.
"""

import pytest

from behave.configuration import Configuration, DEFAULT_RUNNER_CLASS_NAME
from behave.exception import ConfigError
from behave.runner_parallel import (
    ParallelRunner,
    UndefinedStepInfo,
    make_error_result,
    merge_status_counts,
    resolve_worker_formats,
    select_summary_reporter,
)
from behave.runner_plugin import RunnerPlugin
from behave.runner_util import make_undefined_step_snippets
from behave.reporter.summary import SummaryReporterV1


def make_config(command_args=None):
    return Configuration(command_args or [], load_config=False)


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

    @pytest.mark.parametrize("format_name", ["json", "json.pretty", "rerun"])
    def test_unsupported_format_is_dropped_with_warning(self, format_name):
        worker_formats, notes = resolve_worker_formats([format_name, "plain"])
        assert worker_formats == ["plain"]
        assert any(format_name in note for note in notes)

    def test_outfile_bound_formats_are_dropped(self):
        worker_formats, notes = resolve_worker_formats(
            ["plain", "progress"], num_outfile_bound=1)
        assert worker_formats == ["progress"]
        assert any("outfile" in note for note in notes)

    def test_empty_result_falls_back_to_plain(self):
        worker_formats, _notes = resolve_worker_formats(["json"])
        assert worker_formats == ["plain"]

    def test_duplicates_are_removed(self):
        worker_formats, _notes = resolve_worker_formats(["pretty", "plain"])
        assert worker_formats == ["plain"]


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

    def test_empty_source_changes_nothing(self):
        target = {"passed": 1}
        merge_status_counts(target, {})
        assert target == {"passed": 1}


class TestUndefinedStepInfo:
    def test_works_with_undefined_step_snippets(self):
        steps = [UndefinedStepInfo("given", "some step")]
        snippets = make_undefined_step_snippets(steps)
        assert len(snippets) == 1
        assert "some step" in snippets[0]

    def test_snippet_generation_can_escape_quotes(self):
        # -- HINT: make_undefined_step_snippet() mutates step.name.
        steps = [UndefinedStepInfo("given", "it's quoted")]
        snippets = make_undefined_step_snippets(steps)
        assert len(snippets) == 1
        assert r"it\'s quoted" in snippets[0]

    def test_deduplicates_in_set(self):
        infos = {UndefinedStepInfo("given", "a step"),
                 UndefinedStepInfo("given", "a step"),
                 UndefinedStepInfo("when", "a step")}
        assert len(infos) == 2


class TestMakeErrorResult:
    def test_is_failed_with_error_text(self):
        result = make_error_result("some.feature", "BOOM")
        assert result["failed"] is True
        assert result["error_text"] == "BOOM"
        assert result["filename"] == "some.feature"
        assert result["status"] is None


class TestSelectSummaryReporter:
    def test_selects_summary_reporter(self):
        config = make_config()
        reporter = SummaryReporterV1(config)
        assert select_summary_reporter([object(), reporter]) is reporter

    def test_returns_none_without_summary_reporter(self):
        assert select_summary_reporter([object()]) is None


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
        runner = self.make_runner({})
        runner._validate_parallel_hooks()  # -- SHOULD NOT RAISE

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

    def test_injected_default_before_all_hook_is_ignored(self):
        # -- Runner.load_hooks() installs a default "before_all" hook.
        runner = ParallelRunner(make_config())
        runner.hooks = {
            "before_all": runner.before_all_default_hook,
        }
        runner._validate_parallel_hooks()  # -- SHOULD NOT RAISE


# -----------------------------------------------------------------------------
# WIRING:
# -----------------------------------------------------------------------------
class TestRunnerWiring:
    def test_parallel_alias_loads_parallel_runner(self):
        config = make_config(["--runner=parallel"])
        runner = RunnerPlugin().make_runner(config)
        assert isinstance(runner, ParallelRunner)

    def test_default_runner_is_unchanged(self):
        config = make_config()
        assert config.runner == DEFAULT_RUNNER_CLASS_NAME

    def test_configuration_keeps_command_args(self):
        config = make_config(["--jobs=3", "features"])
        assert config.command_args == ["--jobs=3", "features"]
        assert config.jobs == 3

    def test_configuration_keeps_command_args_from_string(self):
        config = Configuration("--jobs=2", load_config=False)
        assert config.command_args == ["--jobs=2"]
