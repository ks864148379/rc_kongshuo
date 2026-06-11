import random

MAX_DELAY = 3600.0  # 1 hour cap
INITIAL_DELAY = 1.0
MULTIPLIER = 2.0
JITTER_FACTOR = 0.2


def compute_delay(attempt_count: int) -> float:
    delay = INITIAL_DELAY * (MULTIPLIER ** (attempt_count - 1))
    delay = min(delay, MAX_DELAY)
    jitter = delay * JITTER_FACTOR * random.random()
    return delay + jitter
