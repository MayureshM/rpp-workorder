import os
import pytest
from datetime import datetime
try:
    from freezegun import freeze_time
except Exception:
    freeze_time = None

@pytest.fixture
def freeze_now():
    if freeze_time:
        with freeze_time('2024-01-01 00:00:00'):
            yield datetime.utcnow()
    else:
        yield datetime.utcnow()
