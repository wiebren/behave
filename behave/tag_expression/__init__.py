# pylint: disable=C0209
"""
Common module for tag-expressions:

* v2: cucumber-tag-expressions (with wildcard extension)

.. seealso::

    * https://docs.cucumber.io
    * https://docs.cucumber.io/cucumber/api/#tag-expressions
"""

from .builder import (
    TagExpressionProtocol,  # noqa: F401
    TagExpressionUtil,      # noqa: F401
    make_tag_expression,    # noqa: F401
)

# -- BACKWARD-COMPATIBLE: Tag-Expressions v1 name.
# REASON: The JetBrains behave_runner.py helper (IntelliJ/PyCharm, still in
# 2026.2) does "from behave.tag_expression import TagExpression" at import
# time and crashes without it, so no test can be run from the IDE.
# It only uses the name for an isinstance() check in its scenario filter.
from .model import Expression as TagExpression  # noqa: F401
