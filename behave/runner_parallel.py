# -*- coding: UTF-8 -*-
"""
Parallel test runner for behave: runs feature files concurrently in worker
processes (used for: ``--jobs N`` with N > 1; runner alias: "parallel").

DESIGN:

* One parent process (this runner) and up to ``config.jobs`` worker processes
  (:class:`concurrent.futures.ProcessPoolExecutor` with "spawn" start-method).
* Work unit: one feature file per task. The parent sends the feature file
  locations (``filename`` or ``filename:line``, so that scenario selection
  is preserved). A worker parses them, runs the feature with a normal
  (sequential) runner runtime and sends back a picklable result
  (status counts, captured output chunk, undefined steps, ...).
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
import os
import pickle
import sys
import time
import traceback
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, as_completed

from behave.configuration import Configuration, DEFAULT_RUNNER_CLASS_NAME
from behave.exception import ConfigError
from behave.formatter._registry import make_formatters
from behave.formatter.base import StreamOpener
from behave.model_type import FileLocation
from behave.parser import ParserError
from behave.reporter.summary import AbstractSummaryReporter, SummaryReporterV1
from behave.runner import Context, Runner
from behave.runner_util import FileLocationParser, parse_features, reset_runtime


# -----------------------------------------------------------------------------
# CONSTANTS:
# -----------------------------------------------------------------------------
#: Formatters that need one shared stream or aggregate over the whole
#: test-run. They cannot be used by many workers at the same time.
UNSUPPORTED_WORKER_FORMATS = frozenset([
    "json", "json.pretty", "rerun",
    "sphinx.steps", "steps", "steps.bad", "steps.catalog", "steps.doc",
    "steps.missing", "steps.usage", "tags", "tags.location",
])

#: Each "*_all" hook requires one of these hooks in parallel mode.
PARALLEL_HOOK_REQUIREMENTS = OrderedDict([
    ("before_all", ("before_parallel", "before_worker")),
    ("after_all", ("after_parallel", "after_worker")),
])

#: Configuration attributes that the parent may have changed after the
#: configuration was built (they select WHICH tests run or WHAT they see).
PROPAGATED_CONFIG_PARAMS = ("stage", "lang", "tags", "userdata")


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


class ScenarioInfo:
    """Duck-types a Scenario for the summary reporter's problem list."""
    __slots__ = ("location", "name")

    def __init__(self, location, name):
        self.location = location
        self.name = name


# -----------------------------------------------------------------------------
# PURE HELPER FUNCTIONS:
# -----------------------------------------------------------------------------
def group_locations_by_filename(locations):
    """Group feature file locations by their feature filename.

    Scenario selection by line number, like "alice.feature:12", must be
    preserved: all locations of one feature file become one work item.

    :param locations: Feature file locations (FileLocation objects or strings).
    :return: Ordered dict with filename as key and location texts as value.
    """
    grouped = OrderedDict()
    for location in locations:
        filename = getattr(location, "filename", None) or str(location)
        filename = os.path.normpath(filename)
        grouped.setdefault(filename, []).append(str(location))
    return grouped


def parse_feature_locations(location_texts, language=None):
    """Parse feature file location texts, like: "alice.feature:12"."""
    locations = []
    for location_text in location_texts:
        location = FileLocationParser.parse(location_text)
        locations.append(FileLocation(os.path.normpath(location.filename),
                                      location.line))
    return parse_features(locations, language=language)


def resolve_worker_formats(formats, outfile_bound=None):
    """Compute the formatter names that workers should use.

    :param formats: Formatter names requested for this test run.
    :param outfile_bound: Flags that tell if format[i] writes to an outfile.
    :return: Tuple (worker_formats, notes) -- notes are user-facing messages.
    :raises ConfigError: If a formatter cannot be used with "--jobs > 1".
    """
    outfile_bound = list(outfile_bound or [])
    worker_formats = []
    notes = []
    for index, name in enumerate(formats):
        is_outfile_bound = (index < len(outfile_bound) and outfile_bound[index])
        if is_outfile_bound:
            raise ConfigError(
                'PARALLEL: formatter "%s" with --outfile is not supported '
                'with --jobs > 1 (many workers cannot write one file). '
                'Use --jobs=1 or drop this formatter.' % name)
        if name in UNSUPPORTED_WORKER_FORMATS:
            raise ConfigError(
                'PARALLEL: formatter "%s" is not supported with --jobs > 1 '
                '(it needs the complete test-run). '
                'Use --jobs=1 or drop this formatter.' % name)
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


def select_outfile_bound_formats(config):
    """Determine which formats write into an "--outfile".

    HINT: make_formatters() pairs format[i] with config.outputs[i].
    A stream-opener without filename writes to the console (not an outfile).
    """
    outputs = config.outputs or []
    return [bool(getattr(opener, "name", None)) for opener in outputs]


def merge_status_counts(target, source):
    """Merge one worker's status-count dict into an accumulator dict."""
    for name, count in source.items():
        target[name] = target.get(name, 0) + count


def select_summary_reporter(reporters):
    """Select the summary reporter that the parent merges results into."""
    for reporter in reporters:
        if isinstance(reporter, AbstractSummaryReporter):
            if not isinstance(reporter, SummaryReporterV1):
                raise ConfigError(
                    "PARALLEL: %s is not supported with --jobs > 1 "
                    "(only SummaryReporterV1 counts can be merged)."
                    % type(reporter).__name__)
            return reporter
    return None


def select_picklable_params(params):
    """Select the parameters that can be sent to a worker process."""
    selected = {}
    for name, value in params.items():
        try:
            pickle.dumps(value)
        except Exception:  # pylint: disable=broad-except
            continue  # -- SKIP: Non-picklable parameter.
        selected[name] = value
    return selected


def make_result(filename, **kwargs):
    """Create a feature task result (all values are picklable)."""
    result = {
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
        # -- HOOK-FAILURES: Of this task only.
        "hook_failures": 0,
        # -- HOOK-FAILURES: Of the worker setup (same value in each result
        # of one worker -- the parent counts them once per worker).
        "worker_init_hook_failures": 0,
        "worker_id": None,
        "worker_setup_failed": False,
        # -- HINT: A parse-error aborts the test-run (like: sequential mode).
        "fatal_error": False,
        "output": "",
        "error_text": None,
    }
    result.update(kwargs)
    return result


# -----------------------------------------------------------------------------
# WORKER SIDE (runs in worker processes; must be module-level for pickling):
# -----------------------------------------------------------------------------
class WorkerOutput(io.StringIO):
    """Output stream of one worker process (for its whole lifetime).

    A worker replaces its ``sys.stdout``/``sys.stderr`` with this stream,
    so that anything bound to them (like logging handlers) keeps writing
    into it. The parent collects the text per feature (and prints it).
    """

    def drain(self):
        """Return the collected text and start over (empty again)."""
        text = self.getvalue()
        self.seek(0)
        self.truncate(0)
        return text


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
_worker_output = None
_worker_id = None
_worker_setup_failed = False
_worker_init_hook_failures = 0
_worker_shutdown_failures = None


def _emit_worker_output(text):
    """Write a worker's output chunk to the real process output stream."""
    stream = sys.__stdout__
    if text and stream is not None:
        stream.write(text)
        stream.flush()


def _apply_worker_config_overrides(config, worker_setup):
    """Adjust a worker's rebuilt Configuration for parallel execution."""
    # -- STEP: Re-apply configuration params that the parent may have changed.
    config_params = worker_setup["config_params"]
    if "stage" in config_params:
        config.setup_stage(config_params["stage"])
    if "lang" in config_params:
        config.lang = config_params["lang"]
    if "userdata" in config_params:
        config.userdata.update(config_params["userdata"])
    if "tags" in config_params:
        config.setup_tag_expression(config_params["tags"])

    # -- STEP: Enforce parallel-worker mode.
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


def _worker_init(worker_setup, worker_id_counter, shutdown_failures):
    """Initialize one worker process (ProcessPoolExecutor initializer)."""
    # pylint: disable=global-statement
    global _worker_runner, _worker_output, _worker_id
    global _worker_setup_failed, _worker_init_hook_failures
    global _worker_shutdown_failures

    # -- SETUP: Use one output stream for the whole worker lifetime,
    # so that logging handlers, etc. keep writing into a collected stream.
    _worker_output = WorkerOutput()
    _worker_shutdown_failures = shutdown_failures
    sys.stdout = _worker_output
    sys.stderr = _worker_output
    try:
        with worker_id_counter.get_lock():
            _worker_id = worker_id_counter.value
            worker_id_counter.value += 1

        reset_runtime()
        config = Configuration(worker_setup["command_args"],
                               load_config=worker_setup["load_config"],
                               **worker_setup["config_kwargs"])
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
        runner.context._set_root_attribute("worker_id", _worker_id)
        runner.context._set_root_attribute("jobs", worker_setup["jobs"])
        _worker_runner = runner

        hook_passed = runner.run_hook("before_worker")
        if not hook_passed:
            # -- LIKE: "before_all" hook-error in sequential mode.
            # HINT: No feature is run by this worker (test-run is aborted).
            _worker_setup_failed = True
        _worker_init_hook_failures = runner.hook_failures
        atexit.register(_worker_shutdown)
    except Exception:  # pylint: disable=broad-except
        _worker_setup_failed = True
        _worker_init_hook_failures = max(_worker_init_hook_failures, 1)
        traceback.print_exc()
    finally:
        _emit_worker_output(_worker_output.drain())


def _worker_shutdown():
    """Finalize one worker process (atexit; skipped on hard terminate)."""
    runner = _worker_runner
    if runner is None:
        return

    failures = 0
    try:
        if not runner.run_hook("after_worker"):
            failures += 1
        try:
            runner.context._do_remaining_cleanups()
        except Exception:  # pylint: disable=broad-except
            traceback.print_exc()
            failures += 1
    finally:
        if failures and _worker_shutdown_failures is not None:
            # -- REPORT: Shutdown failures to the parent process.
            # HINT: Task results are already sent when this hook runs.
            with _worker_shutdown_failures.get_lock():
                _worker_shutdown_failures.value += failures
        if _worker_output is not None:
            _emit_worker_output(_worker_output.drain())


def _run_feature_task(location_texts):
    """Run one feature file in this worker process; returns a result dict."""
    # pylint: disable=global-statement
    global _worker_init_hook_failures
    runner = _worker_runner
    filename = FileLocationParser.parse(location_texts[0]).filename
    result = make_result(filename, worker_id=_worker_id)

    # -- STEP: Report worker-setup failures in EACH result of this worker
    # (a worker may not win any task or its first task may be cancelled).
    result["worker_init_hook_failures"] = _worker_init_hook_failures
    if _worker_setup_failed or runner is None:
        result["worker_setup_failed"] = True
        result["worker_init_hook_failures"] = max(_worker_init_hook_failures, 1)
        result["error_text"] = ("PARALLEL-WORKER SETUP FAILED: %s "
                                "(feature not run)" % filename)
        result["output"] = (_worker_output.drain()
                            if _worker_output is not None else "")
        return result

    hook_failures0 = runner.hook_failures
    try:
        features = parse_feature_locations(location_texts,
                                           language=runner.config.lang)
        if not features:
            raise RuntimeError("No feature parsed from: %s" % filename)
        feature = features[0]

        runner.feature = feature
        stream_opener = StreamOpener(stream=_worker_output)
        runner.formatters = make_formatters(runner.config, [stream_opener])
        undefined_steps0 = len(runner.undefined_steps)
        try:
            for formatter in runner.formatters:
                formatter.uri(feature.filename)
            failed = feature.run(runner)
            for formatter in runner.formatters:
                formatter.close()
        finally:
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
    except ParserError as e:
        # -- LIKE SEQUENTIAL MODE: A parse-error aborts the test-run.
        # HINT: status stays None -- the parent reports it as untested.
        result["error_text"] = "ParserError: %s" % e
        result["failed"] = True
        result["fatal_error"] = True
    except Exception as e:  # pylint: disable=broad-except
        # -- HINT: status stays None -- the parent reports it as untested.
        result["error_text"] = ("PARALLEL-WORKER ERROR in %s: %s\n%s"
                                % (filename, e, traceback.format_exc()))
        result["failed"] = True

    result["hook_failures"] = runner.hook_failures - hook_failures0
    result["output"] = _worker_output.drain()
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
        self._worker_init_failures = {}

    def load_hooks(self, filename=None):
        super(ParallelRunner, self).load_hooks(filename)
        if "before_parallel" not in self.hooks:
            # -- DEFAULT-HOOK (like "before_all"): Setup logging subsystem.
            # HINT: Not a user-defined hook (see: _has_user_defined_hook).
            self.hooks["before_parallel"] = self.before_all_default_hook

    def run_with_paths(self):
        self.context = Context(self)
        self.load_hooks()

        # -- STEP: Select feature files (parsing is done where it is needed).
        locations = [location for location in self.feature_locations()
                     if not self.config.exclude(location)]
        work_items = group_locations_by_filename(locations)

        if (self.config.dry_run or self.config.jobs <= 1
                or len(work_items) <= 1):
            # -- DEGENERATE CASE: Run sequentially (like: Runner).
            self.load_step_definitions()
            self.features.extend(parse_features(locations,
                                                language=self.config.lang))
            self.formatters = make_formatters(self.config, self.config.outputs)
            return self.run_model()

        self._validate_parallel_hooks()
        return self.run_parallel(work_items)

    # -- HOOK SUPPORT:
    def _has_user_defined_hook(self, hook_name):
        hook = self.hooks.get(hook_name)
        if hook is None:
            return False
        # -- EXCLUDE: Injected default hook (see: load_hooks()).
        default_hook_func = Runner.before_all_default_hook
        return getattr(hook, "__func__", hook) is not default_hook_func

    def _validate_parallel_hooks(self):
        """An existing "*_all" hook requires an explicit parallel-mode choice."""
        for all_hook_name, alternatives in PARALLEL_HOOK_REQUIREMENTS.items():
            if not self._has_user_defined_hook(all_hook_name):
                continue
            if not any(self._has_user_defined_hook(hook_name)
                       for hook_name in alternatives):
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
    def make_worker_setup(self, work_items):
        """Create the (picklable) setup data for the worker processes."""
        config = self.config
        formats = config.format or [config.default_format]
        worker_formats, notes = resolve_worker_formats(
            formats, outfile_bound=select_outfile_bound_formats(config))
        for note in notes:
            print(note)

        config_params = {name: getattr(config, name)
                         for name in PROPAGATED_CONFIG_PARAMS
                         if hasattr(config, name)}
        return {
            "command_args": list(getattr(config, "command_args", None) or []),
            "config_kwargs": select_picklable_params(
                getattr(config, "command_kwargs", None) or {}),
            "load_config": getattr(config, "command_load_config", True),
            "config_params": select_picklable_params(config_params),
            "worker_format": worker_formats,
            "jobs": config.jobs,
        }

    def run_parallel(self, work_items):
        config = self.config
        start_time = time.time()
        worker_setup = self.make_worker_setup(work_items)

        num_workers = min(config.jobs, len(work_items))
        mp_context = multiprocessing.get_context("spawn")
        worker_id_counter = mp_context.Value("i", 0)
        shutdown_failures = mp_context.Value("i", 0)

        self.context._set_root_attribute("jobs", config.jobs)
        summary_reporter = select_summary_reporter(config.reporters)
        if summary_reporter is not None:
            summary_reporter.testrun_started()

        failed_count = 0
        undefined_steps = set()
        processed = set()
        stop_requested = False

        executor = ProcessPoolExecutor(
            max_workers=num_workers, mp_context=mp_context,
            initializer=_worker_init,
            initargs=(worker_setup, worker_id_counter, shutdown_failures))
        try:
            if not self.run_hook("before_parallel"):
                self.abort(reason="HOOK-ERROR in hook=before_parallel")

            future_to_filename = {}
            if not self.aborted:
                future_to_filename = {
                    executor.submit(_run_feature_task, locations): filename
                    for filename, locations in work_items.items()
                }

            try:
                for future in as_completed(future_to_filename):
                    if future.cancelled():
                        continue
                    filename = future_to_filename[future]
                    error = future.exception()
                    if error is not None:
                        result = make_result(
                            filename,
                            error_text="PARALLEL-WORKER FAILURE in %s: %s"
                                       % (filename, error))
                    else:
                        result = future.result()

                    self._process_result(result, summary_reporter,
                                         undefined_steps)
                    if result["status"] is not None:
                        # -- HINT: A feature without status did not run.
                        # It is reported as untested (see below).
                        processed.add(filename)
                    if result["failed"]:
                        failed_count += 1
                    if ((result["worker_setup_failed"] or
                            result["fatal_error"]) and not self.aborted):
                        # -- LIKE SEQUENTIAL MODE: before_all hook-error
                        # and parse-error abort the test-run.
                        self.abort(reason=result["error_text"])
                        executor.shutdown(wait=False, cancel_futures=True)
                    elif (result["failed"] and config.stop
                            and not stop_requested):
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

        # -- HINT: Worker shutdown-hooks have run now (on process exit).
        self.worker_hook_failures += shutdown_failures.value
        self.worker_hook_failures += sum(self._worker_init_failures.values())

        # -- REPORT: Features that never ran (cancelled/aborted) as untested.
        self._report_untested_features(work_items, processed)

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

    def _report_untested_features(self, work_items, processed):
        """Report features that did not run to the reporters (as untested)."""
        for filename, locations in work_items.items():
            if filename in processed:
                continue
            try:
                features = parse_feature_locations(locations,
                                                   language=self.config.lang)
            except Exception:  # pylint: disable=broad-except
                # -- SKIP: Unparsable feature (already reported as error).
                continue

            self.features.extend(features)
            for feature in features:
                for reporter in self.config.reporters:
                    reporter.feature(feature)

    def _process_result(self, result, summary_reporter, undefined_steps):
        """Print one feature's output chunk and merge its counts."""
        if result["output"]:
            sys.stdout.write(result["output"])
            sys.stdout.flush()
        if result["error_text"]:
            sys.stderr.write(result["error_text"] + "\n")
            sys.stderr.flush()

        worker_id = result["worker_id"]
        if worker_id is not None:
            # -- HINT: Same value in each result of one worker (count once).
            self._worker_init_failures[worker_id] = \
                result["worker_init_hook_failures"]
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
                scenario_info = ScenarioInfo(location, name)
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

