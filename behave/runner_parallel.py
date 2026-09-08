# -*- coding: UTF-8 -*-
"""
Parallel test runner for behave: runs feature files concurrently in worker
processes (used for: ``--jobs N`` with N > 1; runner alias: "parallel").

DESIGN:

* One parent process (this runner) and up to ``config.jobs`` worker processes
  (:class:`concurrent.futures.ProcessPoolExecutor` with "spawn" start-method).
* Work unit: one feature file per task. A worker parses its feature file,
  runs it with a normal (sequential) runner runtime and sends back a picklable
  result (status counts, captured output chunk, undefined steps, ...).
* The parent prints each feature's output chunk when its task completes
  (whole chunks, completion order), merges the counts into the summary
  reporter and composes the final exit status like the sequential runner.

HOOKS (parallel mode never calls "before_all"/"after_all"):

* "before_parallel"/"after_parallel": run once in the PARENT process.
* "before_worker"/"after_worker": run once per worker process.
* All other hooks (feature/rule/scenario/step/tag) run in workers, unchanged.

An environment file that defines "before_all" (or "after_all") without a
matching parallel-mode hook is rejected: what happens with the ``*_all``
hook under parallel execution must be an explicit choice (call it from one
of the parallel hooks, split it up, or replace it).

.. note:: Programmatic use requires an importable main module
    (``if __name__ == "__main__":`` guard), because the "spawn"
    start-method re-imports ``__main__`` in each worker process.
"""

import atexit
import io
import multiprocessing
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import redirect_stdout, redirect_stderr
from types import SimpleNamespace

from behave.configuration import Configuration, DEFAULT_RUNNER_CLASS_NAME
from behave.exception import ConfigError
from behave.formatter._registry import make_formatters
from behave.formatter.base import StreamOpener
from behave.reporter.summary import AbstractSummaryReporter, SummaryReporterV1
from behave.runner import Context, Runner
from behave.runner_util import parse_features, reset_runtime


# -----------------------------------------------------------------------------
# CONSTANTS:
# -----------------------------------------------------------------------------
#: Formatters that cannot write to one shared stream from many workers.
UNSUPPORTED_WORKER_FORMATS = ("json", "json.pretty", "rerun")

#: Each "*_all" hook requires one of these hooks in parallel mode.
PARALLEL_HOOK_REQUIREMENTS = {
    "before_all": ("before_parallel", "before_worker"),
    "after_all": ("after_parallel", "after_worker"),
}

class UndefinedStepInfo:
    """Undefined-step info: duck-types a Step for undefined-step snippets.

    Hashable (used for cross-worker deduplication), but "name" stays
    mutable because snippet generation may rewrite it for quote-escaping
    (see: :func:`behave.runner_util.make_undefined_step_snippet`).
    """
    __slots__ = ("step_type", "name")

    def __init__(self, step_type, name):
        self.step_type = step_type
        self.name = name

    def _key(self):
        return (self.step_type, self.name)

    def __eq__(self, other):
        other_key = getattr(other, "_key", None)
        return other_key is not None and self._key() == other_key()

    def __hash__(self):
        return hash(self._key())

    def __lt__(self, other):
        return self._key() < other._key()

    def __repr__(self):
        return "UndefinedStepInfo(%r, %r)" % (self.step_type, self.name)


# -----------------------------------------------------------------------------
# PURE HELPER FUNCTIONS:
# -----------------------------------------------------------------------------
def resolve_worker_formats(formats, num_outfile_bound=0):
    """Compute the formatter names that workers should use.

    :param formats: Formatter names requested for this test run.
    :param num_outfile_bound: Leading formats bound to an ``--outfile``.
    :return: Tuple (worker_formats, notes) -- notes are user-facing messages.
    """
    worker_formats = []
    notes = []
    for index, name in enumerate(formats):
        if index < num_outfile_bound:
            notes.append(
                'PARALLEL: WARNING -- formatter "%s" with --outfile '
                'is not supported with --jobs > 1 (skipped).' % name)
            continue
        if name in UNSUPPORTED_WORKER_FORMATS:
            notes.append(
                'PARALLEL: WARNING -- formatter "%s" is not supported '
                'with --jobs > 1 (skipped).' % name)
            continue
        if name == "pretty":
            notes.append(
                'PARALLEL: NOTE -- using "plain" formatter instead of '
                '"pretty" (not usable with --jobs > 1).')
            name = "plain"
        if name not in worker_formats:
            worker_formats.append(name)
    if not worker_formats:
        worker_formats.append("plain")
    return worker_formats, notes


def merge_status_counts(target, source):
    """Merge one worker's status-count dict into an accumulator dict."""
    for name, count in source.items():
        target[name] = target.get(name, 0) + count


def select_summary_reporter(reporters):
    """Select the summary reporter from a list of reporters (or None)."""
    for reporter in reporters:
        if isinstance(reporter, AbstractSummaryReporter):
            return reporter
    return None


def make_error_result(filename, error_text):
    """Create a result dict for a feature task that crashed unexpectedly."""
    return {
        "filename": filename,
        "location": filename,
        "failed": True,
        "status": None,
        "feature_summary": {},
        "rule_summary": {},
        "scenario_summary": {},
        "step_summary": {},
        "duration": 0.0,
        "problematic_scenarios": [],
        "undefined_steps": [],
        "hook_failures": 0,
        "output": "",
        "error_text": error_text,
    }


# -----------------------------------------------------------------------------
# WORKER SIDE (runs in worker processes; must be module-level for pickling):
# -----------------------------------------------------------------------------
class WorkerRunner(Runner):
    """Runner runtime used inside one worker process.

    Lives for the whole worker process and runs its features one by one
    on one Context (so that "before_worker" attributes stay visible),
    without ever running "before_all"/"after_all".
    """

    def load_hooks(self, filename=None):
        super(WorkerRunner, self).load_hooks(filename)
        if "before_worker" not in self.hooks:
            # -- DEFAULT-HOOK (like "before_all"): Setup logging subsystem.
            self.hooks["before_worker"] = self.before_all_default_hook


# -- WORKER-PROCESS GLOBALS:
_worker_runner = None
_worker_init_hook_failures = 0


def _apply_worker_config_overrides(config, worker_setup):
    """Adjust a worker's rebuilt Configuration for parallel execution."""
    config.jobs = 1
    config.runner = DEFAULT_RUNNER_CLASS_NAME
    config.format = list(worker_setup["worker_format"])
    config.default_format = "plain"
    # -- HINT: The parent prints merged undefined-step snippets once.
    config.show_snippets = False
    # -- HINT: No per-worker summary; the parent prints the merged summary.
    config.summary = False
    config.reporters = [reporter for reporter in config.reporters
                        if not isinstance(reporter, AbstractSummaryReporter)]


def _worker_init(worker_setup, worker_id_counter):
    """Initialize one worker process (ProcessPoolExecutor initializer)."""
    global _worker_runner, _worker_init_hook_failures
    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer), redirect_stderr(buffer):
            with worker_id_counter.get_lock():
                worker_id = worker_id_counter.value
                worker_id_counter.value += 1

            reset_runtime()
            config = Configuration(worker_setup["command_args"])
            _apply_worker_config_overrides(config, worker_setup)

            runner = WorkerRunner(config)
            runner.path_manager.__enter__()  # -- UNDONE: at process exit.
            runner.setup_paths()
            runner.context = Context(runner)
            runner.load_hooks()
            runner.load_step_definitions()
            # -- BIND: Step registry (normally done by: ModelRunner.run_model).
            from behave.runner import the_step_registry
            runner.step_registry = the_step_registry
            runner.context._set_root_attribute("worker_id", worker_id)
            runner.context._set_root_attribute("jobs", worker_setup["jobs"])
            runner.run_hook("before_worker")
            _worker_init_hook_failures = runner.hook_failures
            _worker_runner = runner
            atexit.register(_worker_shutdown)
    finally:
        text = buffer.getvalue()
        if text:
            sys.__stdout__.write(text)
            sys.__stdout__.flush()


def _worker_shutdown():
    """Finalize one worker process (atexit; skipped on hard terminate)."""
    runner = _worker_runner
    if runner is None:
        return

    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer), redirect_stderr(buffer):
            runner.run_hook("after_worker")
            try:
                runner.context._do_remaining_cleanups()
            except Exception:  # pylint: disable=broad-except
                traceback.print_exc()
    finally:
        text = buffer.getvalue()
        if text:
            sys.__stdout__.write(text)
            sys.__stdout__.flush()


def _run_feature_task(feature_filename):
    """Run one feature file in this worker process; returns a result dict."""
    global _worker_init_hook_failures
    runner = _worker_runner
    buffer = io.StringIO()
    result = make_error_result(feature_filename, error_text=None)
    hook_failures0 = runner.hook_failures if runner else 0
    try:
        if runner is None:
            raise RuntimeError("PARALLEL-WORKER not initialized")
        with redirect_stdout(buffer), redirect_stderr(buffer):
            undefined_steps0 = len(runner.undefined_steps)
            features = parse_features([feature_filename],
                                      language=runner.config.lang)
            if not features:
                raise RuntimeError(
                    "No feature parsed from: %s" % feature_filename)
            feature = features[0]

            runner.feature = feature
            stream_opener = StreamOpener(stream=buffer)
            runner.formatters = make_formatters(runner.config, [stream_opener])
            for formatter in runner.formatters:
                formatter.uri(feature.filename)
            failed = feature.run(runner)
            for formatter in runner.formatters:
                formatter.close()
            runner.formatters = []
            for reporter in runner.config.reporters:
                reporter.feature(feature)

            # -- TALLY: Status counts for this feature (mergeable dicts).
            tally = SummaryReporterV1(runner.config)
            tally.testrun_started()
            tally.process_feature(feature)
            problematic = \
                [("failed", str(scenario.location), scenario.name)
                 for scenario in tally.failed_scenarios] + \
                [("errored", str(scenario.location), scenario.name)
                 for scenario in tally.errored_scenarios]
            new_undefined = runner.undefined_steps[undefined_steps0:]

            result.update(
                failed=bool(failed),
                status=feature.status.name,
                location=str(feature.location),
                feature_summary=tally.feature_summary,
                rule_summary=tally.rule_summary,
                scenario_summary=tally.scenario_summary,
                step_summary=tally.step_summary,
                duration=feature.duration,
                problematic_scenarios=problematic,
                undefined_steps=[(step.step_type, step.name)
                                 for step in new_undefined])
    except Exception as e:  # pylint: disable=broad-except
        result["error_text"] = ("PARALLEL-WORKER ERROR in %s: %s\n%s"
                                % (feature_filename, e,
                                   traceback.format_exc()))
        result["failed"] = True

    if runner is not None:
        result["hook_failures"] = (runner.hook_failures - hook_failures0
                                   + _worker_init_hook_failures)
        _worker_init_hook_failures = 0  # -- CONSUMED-ONCE.
    result["output"] = buffer.getvalue()
    return result


# -----------------------------------------------------------------------------
# PARENT SIDE:
# -----------------------------------------------------------------------------
class ParallelRunner(Runner):
    """Test runner that runs feature files in parallel worker processes.

    Selected via ``--jobs N`` (N > 1) or ``--runner=parallel``.
    Falls back to normal sequential execution for degenerate cases
    (dry-run, jobs <= 1, or at most one feature file).
    """

    def __init__(self, config):
        super(ParallelRunner, self).__init__(config)
        self.worker_hook_failures = 0
        self.cleanups_failed = False

    def run_with_paths(self):
        self.context = Context(self)
        self.load_hooks()

        # -- STEP: Parse all feature files (by using their file location).
        feature_locations = [filename for filename in self.feature_locations()
                             if not self.config.exclude(filename)]
        features = parse_features(feature_locations, language=self.config.lang)
        self.features.extend(features)

        if (self.config.dry_run or self.config.jobs <= 1
                or len(self.features) <= 1):
            # -- DEGENERATE CASE: Run sequentially (like: Runner).
            self.load_step_definitions()
            self.formatters = make_formatters(self.config, self.config.outputs)
            return self.run_model()

        self._validate_parallel_hooks()
        return self.run_parallel()

    # -- HOOK SUPPORT:
    def _has_user_defined_hook(self, hook_name):
        hook = self.hooks.get(hook_name)
        if hook is None:
            return False
        # -- EXCLUDE: Injected default hook (see: Runner.load_hooks()).
        default_hook_func = Runner.before_all_default_hook
        return getattr(hook, "__func__", hook) is not default_hook_func

    def _validate_parallel_hooks(self):
        """An existing "*_all" hook requires an explicit parallel-mode choice."""
        for all_hook_name, alternatives in PARALLEL_HOOK_REQUIREMENTS.items():
            if not self._has_user_defined_hook(all_hook_name):
                continue
            if not any(name in self.hooks for name in alternatives):
                raise ConfigError(
                    'PARALLEL: environment file defines "%(all_hook)s", '
                    'which is not called with --jobs > 1. '
                    'Define "%(parent_hook)s" (parent, once) and/or '
                    '"%(worker_hook)s" (per worker) -- e.g. call '
                    '%(all_hook)s(context) from one of them -- to state '
                    'explicitly what should happen.' % dict(
                        all_hook=all_hook_name,
                        parent_hook=alternatives[0],
                        worker_hook=alternatives[1]))

    # -- PARALLEL EXECUTION:
    def run_parallel(self):
        config = self.config
        features = self.features
        start_time = time.time()

        formats = config.format or [config.default_format]
        # -- HINT: Outfile-bound stream openers have a filename in "name"
        # (the default stdout opener does not) and pair with the leading
        # formats positionally (see: make_formatters).
        num_outfile_bound = len([opener for opener in config.outputs
                                 if getattr(opener, "name", None)])
        worker_formats, notes = resolve_worker_formats(
            formats, num_outfile_bound=num_outfile_bound)
        for note in notes:
            print(note)

        worker_setup = {
            "command_args": list(getattr(config, "command_args", None) or []),
            "worker_format": worker_formats,
            "jobs": config.jobs,
        }
        num_workers = min(config.jobs, len(features))
        mp_context = multiprocessing.get_context("spawn")
        worker_id_counter = mp_context.Value("i", 0)

        if "before_parallel" not in self.hooks:
            # -- DEFAULT-HOOK (like: "before_all"): Setup logging subsystem.
            self.hooks["before_parallel"] = self.before_all_default_hook
        self.context._set_root_attribute("jobs", config.jobs)

        summary_reporter = select_summary_reporter(config.reporters)
        if summary_reporter is not None:
            summary_reporter.testrun_started()

        failed_count = 0
        undefined_steps = set()
        processed_features = set()
        stop_requested = False

        executor = ProcessPoolExecutor(
            max_workers=num_workers, mp_context=mp_context,
            initializer=_worker_init,
            initargs=(worker_setup, worker_id_counter))
        try:
            hook_passed = self.run_hook("before_parallel")
            if not hook_passed:
                self.abort(reason="HOOK-ERROR in hook=before_parallel")

            future_to_feature = {}
            if not self.aborted:
                future_to_feature = {
                    executor.submit(_run_feature_task, feature.filename):
                        feature
                    for feature in features
                }

            try:
                for future in as_completed(future_to_feature):
                    if future.cancelled():
                        continue
                    feature = future_to_feature[future]
                    error = future.exception()
                    if error is not None:
                        result = make_error_result(
                            feature.filename,
                            "PARALLEL-WORKER FAILURE in %s: %s"
                            % (feature.filename, error))
                    else:
                        result = future.result()
                        processed_features.add(id(feature))

                    self._process_result(result, summary_reporter,
                                         undefined_steps)
                    if result["failed"]:
                        failed_count += 1
                        if config.stop and not stop_requested:
                            # -- FAIL-EARLY (best-effort): Cancel pending
                            # tasks; features already in-flight finish.
                            stop_requested = True
                            executor.shutdown(wait=False, cancel_futures=True)
            except KeyboardInterrupt:
                self.abort(reason="KeyboardInterrupt")
                executor.shutdown(wait=False, cancel_futures=True)
                self._terminate_worker_processes(executor)
        finally:
            executor.shutdown(wait=True)

        # -- REPORT: Features that never ran (cancelled/aborted) as untested.
        for feature in features:
            if id(feature) not in processed_features:
                for reporter in config.reporters:
                    reporter.feature(feature)

        self.run_hook_with_capture("after_parallel")
        try:
            self.context._do_remaining_cleanups()
        except Exception:  # pylint: disable=broad-except
            self.cleanups_failed = True

        if self.aborted:
            print("\nABORTED: By user.")
        if summary_reporter is not None:
            summary_reporter.duration = time.time() - start_time
        for reporter in config.reporters:
            reporter.end()

        self._undefined_steps = sorted(undefined_steps)
        failed = ((failed_count > 0) or self.aborted
                  or (self.hook_failures > 0)
                  or (self.worker_hook_failures > 0)
                  or (len(self._undefined_steps) > 0)
                  or self.cleanups_failed)
        return failed

    def _process_result(self, result, summary_reporter, undefined_steps):
        """Print one feature's output chunk and merge its counts."""
        if result["output"]:
            sys.stdout.write(result["output"])
            sys.stdout.flush()
        if result["error_text"]:
            sys.stderr.write(result["error_text"] + "\n")
            sys.stderr.flush()

        self.worker_hook_failures += result["hook_failures"]
        undefined_steps.update(
            UndefinedStepInfo(*info) for info in result["undefined_steps"])

        if summary_reporter is not None and result["status"] is not None:
            merge_status_counts(summary_reporter.feature_summary,
                                result["feature_summary"])
            merge_status_counts(summary_reporter.rule_summary,
                                result["rule_summary"])
            merge_status_counts(summary_reporter.scenario_summary,
                                result["scenario_summary"])
            merge_status_counts(summary_reporter.step_summary,
                                result["step_summary"])
            for kind, location, name in result["problematic_scenarios"]:
                scenario_info = SimpleNamespace(location=location, name=name)
                if kind == "failed":
                    summary_reporter.failed_scenarios.append(scenario_info)
                else:
                    summary_reporter.errored_scenarios.append(scenario_info)

    @staticmethod
    def _terminate_worker_processes(executor):
        """Best-effort hard-stop of worker processes (uses private API)."""
        processes = getattr(executor, "_processes", None) or {}
        for process in list(processes.values()):
            try:
                process.terminate()
            except Exception:  # pylint: disable=broad-except
                pass
