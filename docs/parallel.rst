.. _id.parallel:

==========================
Parallel Test Execution
==========================

With ``--jobs N`` (N > 1) behave runs feature files in parallel::

    behave --jobs 4

Execution model
===============

* One parent process orchestrates up to N **worker processes**
  (``multiprocessing`` with the "spawn" start-method on all platforms).
* The **work unit is one feature file**: all rules, scenarios and steps of a
  feature file run inside one worker, in their normal order, with the normal
  feature/rule/scenario/step/tag hooks.
* Each worker builds a complete, isolated behave runtime: it re-reads the
  configuration files and command-line options, loads ``environment.py``
  and the step definitions, and keeps one :class:`~behave.runner.Context`
  for its whole lifetime.
* The parent prints each feature's output as one block when the feature
  finishes (completion order), merges the counts of all workers into one
  summary, and computes the exit status like the sequential runner.

The parallel runner is auto-selected when ``--jobs > 1`` is used
(an explicitly selected ``--runner`` class always wins). It can also be
selected directly with ``--runner=parallel``. Degenerate cases -- dry-run
mode, ``--jobs=1``, or at most one feature file -- fall back to normal
sequential execution.

Hooks in parallel mode
======================

Parallel mode **never calls** ``before_all`` / ``after_all``.
It provides its own hooks instead:

=========================== ========================== =================================
Hook                        Sequential mode (jobs=1)   Parallel mode (jobs > 1)
=========================== ========================== =================================
``before_all/after_all``    unchanged                  **never called**
``before_parallel`` /       ignored                    once, in the **parent** process
``after_parallel``
``before_worker`` /         ignored                    once **per worker process**
``after_worker``                                       (startup / shutdown)
other hooks                 unchanged                  run in workers, unchanged
=========================== ========================== =================================

``before_worker(context)`` receives the worker's root context -- attributes
set there are visible to all hooks and steps that the worker later runs,
just like ``before_all`` attributes in sequential mode. The context also
provides ``context.worker_id`` (0..N-1) and ``context.jobs``.
``before_parallel(context)`` runs on the parent context (with ``context.jobs``).

**Explicit migration required:** if your ``environment.py`` defines
``before_all`` (or ``after_all``) and you run with ``--jobs > 1``, behave
refuses to start unless a matching parallel-mode hook exists
(``before_parallel`` and/or ``before_worker``; likewise for ``after_all``).
What happens with your ``*_all`` hook under parallel execution must be an
explicit choice -- for example:

.. code-block:: python

    # -- FILE: features/environment.py
    def before_all(context):
        setup_test_database(context)

    def before_worker(context):
        # -- EXPLICIT CHOICE: each worker needs its own setup.
        before_all(context)

Limitations
===========

* **State is not shared between workers.** Module globals, context
  attributes and resources set up in one worker (or in the parent) do not
  exist in the others. Workers rebuild the configuration from the
  command-line, the configuration files and the constructor parameters;
  changes made to a configuration object after it was built (except
  ``stage``, ``lang``, ``tags`` and ``userdata``) are not seen by workers.
* **Formatters:** ``plain`` and the ``progress`` formatters are supported
  (output arrives in whole-feature blocks) and ``pretty`` is replaced by
  ``plain``. Formatters that need the complete test-run (``json``,
  ``rerun``, the ``steps.*`` and ``tags`` formatters) and any formatter
  bound to an ``--outfile`` are **rejected** with a ``ConfigError``:
  many workers cannot write one file, and silently producing no report
  would be worse than failing. Use ``--jobs=1`` for those.
  The JUnit reporter is fully supported: workers write their independent
  per-feature XML files.
* **Fail-early is best effort:** with ``--stop`` (or ``--wip``), pending
  feature files are cancelled after the first failure, but features already
  running in a worker finish.
* The summary duration is the wall-clock time of the whole run.
* Debugging is easier with ``--jobs=1``: a debugger cannot be used in a
  worker process and tracebacks cross the process boundary as text.
* Programmatic use of the parallel runner requires an importable main
  module (``if __name__ == "__main__":`` guard), because the "spawn"
  start-method re-imports ``__main__`` in each worker process.

Per-worker resources
====================

Use ``context.worker_id`` (0..N-1) to give each worker its own resource,
like a browser port, a test account or a database schema:

.. code-block:: python

    # -- FILE: features/environment.py
    TEST_ACCOUNTS = ["alice", "bob", "charly", "dora"]

    def before_worker(context):
        # -- HINT: Each worker process uses its own test account.
        context.account = TEST_ACCOUNTS[context.worker_id]
        context.server_port = 8080 + context.worker_id

    def after_worker(context):
        release_account(context.account)
