import json

import pytest
from lumigo_core.triggers.event_trigger import parse_triggers

from lumigo_tracer.lumigo_utils import Configuration


@pytest.mark.parametrize(
    ("event", "output"),
    [
        (  # Step Function
            {
                "bla": "saart",
                "_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"},
            },
            [
                {
                    "triggeredBy": "stepFunction",
                    "fromMessageIds": ["54589cfc-5ed8-4799-8fc0-5b45f6f225d1"],
                }
            ],
        ),
        (  # Inner Step Function
            {
                "bla": "saart",
                "inner": {"_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"}},
            },
            [
                {
                    "triggeredBy": "stepFunction",
                    "fromMessageIds": ["54589cfc-5ed8-4799-8fc0-5b45f6f225d1"],
                }
            ],
        ),
        (  # Step Function from list
            [
                {
                    "bla": "saart",
                    "inner": {
                        "_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"}
                    },
                },
                {"something": "else"},
            ],
            [
                {
                    "triggeredBy": "stepFunction",
                    "fromMessageIds": ["54589cfc-5ed8-4799-8fc0-5b45f6f225d1"],
                }
            ],
        ),
        (  # Step Function from inner list
            {
                "bla": "saart",
                "inner": [
                    {"_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"}},
                    {"something": "else"},
                ],
            },
            [
                {
                    "triggeredBy": "stepFunction",
                    "fromMessageIds": ["54589cfc-5ed8-4799-8fc0-5b45f6f225d1"],
                }
            ],
        ),
        (  # Step Function - too deep
            {
                "bla": "saart",
                "a": {
                    "b": {
                        "c": {
                            "d": {
                                "_lumigo": {
                                    "step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"
                                }
                            }
                        }
                    }
                },
            },
            [],
        ),
    ],
)
def test_parse_triggered_by(event, output):
    Configuration.is_step_function = True
    triggers = json.loads(json.dumps(parse_triggers(event)))
    assert len({t.pop("id") for t in triggers}) == len(output)
    [t.pop("targetId") for t in triggers]
    assert triggers == output
