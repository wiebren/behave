@sequential
Feature: Parallel test execution with --jobs option

  As a tester
  I want to run feature files in parallel worker processes
  So that large test suites finish faster.

  . NOTES:
  .  * "--jobs N" (N > 1) auto-selects the parallel runner (alias: "parallel").
  .  * Work unit: one feature file per worker task.
  .  * Parallel mode never calls "before_all"/"after_all"; it uses
  .    "before_parallel"/"after_parallel" (parent process, once) and
  .    "before_worker"/"after_worker" (once per worker process) instead.
  .  * An environment file that defines "before_all" (or "after_all")
  .    without a matching parallel-mode hook is rejected with --jobs > 1.

  @setup
  Scenario: Test Setup
    Given a new working directory
    And a file named "features/steps/steps.py" with:
        """
        from behave import step

        @step('a step passes')
        def step_passes(context):
            pass

        @step('a step fails')
        def step_fails(context):
            assert False, "XFAIL-STEP"
        """
    And a file named "features/environment.py" with:
        """
        def before_all(context):
            print("HOOK: BEFORE-ALL")

        def before_parallel(context):
            print("HOOK: BEFORE-PARALLEL jobs=%s" % context.jobs)

        def after_parallel(context):
            print("HOOK: AFTER-PARALLEL")

        def before_worker(context):
            print("HOOK: WORKER-STARTED")

        def after_worker(context):
            print("HOOK: WORKER-STOPPED")
        """
    And a file named "features/alice.feature" with:
        """
        Feature: Alice
          Scenario: A1
            Given a step passes
            When a step passes
        """
    And a file named "features/bob.feature" with:
        """
        Feature: Bob
          Scenario: B1
            Given a step passes
        """
    And a file named "features/charly.feature" with:
        """
        Feature: Charly
          Scenario: C1
            Given a step passes
        """
    And a file named "features/dora.feature" with:
        """
        Feature: Dora
          Scenario: D1
            Given a step passes
        """

  Scenario: Run many feature files in parallel (case: all passing)
    When I run "behave --jobs=2 -f plain --no-color --no-capture-hooks features/alice.feature features/bob.feature features/charly.feature features/dora.feature"
    Then it should pass with:
        """
        4 features passed, 0 failed, 0 skipped
        """
    And the command output should contain "USING RUNNER: behave.runner_parallel:ParallelRunner"
    And the command output should contain "4 scenarios passed, 0 failed, 0 skipped"
    And the command output should contain "5 steps passed, 0 failed, 0 skipped"
    And the command output should contain "HOOK: BEFORE-PARALLEL jobs=2" 1 times
    And the command output should contain "HOOK: AFTER-PARALLEL" 1 times
    And the command output should contain "HOOK: WORKER-STARTED" 2 times
    And the command output should contain "HOOK: WORKER-STOPPED" 2 times
    And the command output should not contain "HOOK: BEFORE-ALL"

  Scenario: Sequential mode is used with --jobs=1 and keeps before_all
    When I run "behave --jobs=1 -f plain --no-color features/alice.feature"
    Then it should pass with:
        """
        1 feature passed, 0 failed, 0 skipped
        """
    And the command output should contain "USING RUNNER: behave.runner:Runner"
    And the command output should contain "HOOK: BEFORE-ALL"
    And the command output should not contain "HOOK: WORKER-STARTED"
    And the command output should not contain "HOOK: BEFORE-PARALLEL"

  Scenario: Parallel runner with one feature file falls back to sequential mode
    When I run "behave --jobs=2 -f plain --no-color features/alice.feature"
    Then it should pass with:
        """
        1 feature passed, 0 failed, 0 skipped
        """
    And the command output should contain "USING RUNNER: behave.runner_parallel:ParallelRunner"
    And the command output should contain "HOOK: BEFORE-ALL"
    And the command output should not contain "HOOK: WORKER-STARTED"

  Scenario: A failing feature fails the parallel test run
    Given a file named "features/fails.feature" with:
        """
        Feature: Failing
          Scenario: F1
            Given a step passes
            When a step fails
        """
    When I run "behave --jobs=2 -f plain --no-color features/alice.feature features/bob.feature features/fails.feature"
    Then it should fail with:
        """
        2 features passed, 1 failed, 0 skipped
        """
    And the command output should contain "Failing scenarios:"
    And the command output should contain "features/fails.feature:2  F1"

  Scenario: Undefined steps are reported once by the parallel test run
    Given a file named "features/undefined.feature" with:
        """
        Feature: Undefined
          Scenario: U1
            Given an unknown step is used
        """
    When I run "behave --jobs=2 -f plain --no-color features/alice.feature features/bob.feature features/undefined.feature"
    Then it should fail
    And the command output should contain "You can implement step definitions for undefined steps with these snippets:"
    And the command output should contain "@given('an unknown step is used')" 1 times

  Scenario: An environment file with only a before_all hook is rejected in parallel mode
    Given a file named "features/environment.py" with:
        """
        def before_all(context):
            print("HOOK: BEFORE-ALL")
        """
    When I run "behave --jobs=2 -f plain --no-color features/alice.feature features/bob.feature"
    Then it should fail
    And the command output should contain:
        """
        ConfigError: PARALLEL: environment file defines "before_all", which is not called with --jobs > 1.
        """
    But note that "the same environment file works in sequential mode"
    When I run "behave --jobs=1 -f plain --no-color features/alice.feature"
    Then it should pass
    And the command output should contain "HOOK: BEFORE-ALL"

  @regression
  Rule: Parallel mode behaves like sequential mode

    Background: Restore the parallel-mode environment file
      Given a file named "features/environment.py" with:
          """
          import logging

          def before_parallel(context):
              print("HOOK: BEFORE-PARALLEL")

          def before_worker(context):
              print("HOOK: WORKER-STARTED")
          """

    Scenario: Scenario selection by line number is preserved
      Given a file named "features/many.feature" with:
          """
          Feature: Many
            Scenario: M1
              Given a step passes
            Scenario: M2
              Given a step passes
          """
      When I run "behave --jobs=1 -f plain --no-color features/many.feature:2 features/alice.feature"
      Then it should pass with:
          """
          2 scenarios passed, 0 failed, 1 skipped
          """
      But note that "the parallel test run must select the same scenarios"
      When I run "behave --jobs=2 -f plain --no-color features/many.feature:2 features/alice.feature"
      Then it should pass with:
          """
          2 scenarios passed, 0 failed, 1 skipped
          """

    Scenario: A failing before_worker hook aborts the test run
      Given a file named "features/environment.py" with:
          """
          def before_worker(context):
              raise RuntimeError("XFAIL-SETUP")
          """
      When I run "behave --jobs=2 -f plain --no-color features/alice.feature features/bob.feature"
      Then it should fail with:
          """
          0 features passed, 0 failed, 0 skipped, 2 untested
          """
      And the command output should contain "HOOK-ERROR in before_worker"

    Scenario: A failing after_worker hook fails the test run
      Given a file named "features/environment.py" with:
          """
          def after_worker(context):
              raise RuntimeError("XFAIL-TEARDOWN")
          """
      When I run "behave --jobs=2 -f plain --no-color features/alice.feature features/bob.feature"
      Then it should fail
      And the command output should contain "2 features passed, 0 failed, 0 skipped"
      And the command output should contain "HOOK-ERROR in after_worker"

    Scenario: A formatter with an outfile is rejected instead of writing nothing
      When I run "behave --jobs=2 -f plain -o report.txt --no-color features/alice.feature features/bob.feature"
      Then it should fail
      And the command output should contain:
          """
          ConfigError: PARALLEL: formatter "plain" with --outfile is not supported with --jobs > 1
          """

    Scenario: An aggregating formatter is rejected instead of producing partial output
      When I run "behave --jobs=2 -f json --no-color features/alice.feature features/bob.feature"
      Then it should fail
      And the command output should contain:
          """
          ConfigError: PARALLEL: formatter "json" is not supported with --jobs > 1
          """

    Scenario: An explicitly selected runner is not replaced by the parallel runner
      When I run "behave --jobs=4 --runner=behave.runner:Runner -f plain --no-color features/alice.feature features/bob.feature"
      Then it should pass
      And the command output should contain "USING RUNNER: behave.runner:Runner"
      And the command output should not contain "HOOK: WORKER-STARTED"

    Scenario: Log output of the workers is not swallowed
      Given a file named "features/environment.py" with:
          """
          def before_parallel(context):
              print("HOOK: BEFORE-PARALLEL")
          """
      And a file named "features/steps/log_steps.py" with:
          """
          import logging
          from behave import step

          @step('a step logs')
          def step_logs(context):
              logging.getLogger("demo").warning("LOG-FROM-WORKER")
          """
      And a file named "features/logging.feature" with:
          """
          Feature: Logging
            Scenario: L1
              Given a step logs
          """
      When I run "behave --jobs=1 -f plain --no-color --no-logcapture features/logging.feature features/alice.feature"
      Then it should pass
      And the command output should contain "LOG-FROM-WORKER"
      But note that "the parallel test run must not swallow the log output"
      When I run "behave --jobs=2 -f plain --no-color --no-logcapture features/logging.feature features/alice.feature"
      Then it should pass
      And the command output should contain "LOG-FROM-WORKER"

    Scenario: A feature file that cannot be parsed aborts the test run
      Given a file named "features/broken.feature" with:
          """
          Feature: Broken
            Scenario: B1
              Given a step passes
            THIS LINE IS NOT GHERKIN
          """
      When I run "behave --jobs=2 -f plain --no-color features/broken.feature features/alice.feature"
      Then it should fail
      And the command output should contain "ParserError"
