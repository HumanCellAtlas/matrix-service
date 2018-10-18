import time


class WaitFor:

    EXPONENTIAL_BACKOFF_FACTOR = 1.618

    def __init__(self, func, *args):
        self.func = func
        self.func_args = args
        self.start_time = None
        self.backoff_seconds = 1.0

    def to_return_value(self, value=None, timeout_seconds=60):
        self.start_time = time.time()
        timeout_at = self.start_time + timeout_seconds

        while time.time() < timeout_at:
            retval = self._call_func()
            if retval == value:
                return retval
            self._wait_until_next_check_time()
        else:
            raise RuntimeError(f"Function {self._function_signature()} did not return value {value} " +
                               f"within {timeout_seconds} seconds")

    def to_return_a_value_other_than(self, other_than_value=None, timeout_seconds=60):
        self.start_time = time.time()
        timeout_at = self.start_time + timeout_seconds

        while time.time() < timeout_at:
            retval = self._call_func()
            if not retval == other_than_value:
                return retval
            self._wait_until_next_check_time()
        else:
            raise RuntimeError(f"Function {self._function_signature()} did not return a non-{other_than_value} value " +
                               f"within {timeout_seconds} seconds")

    def _call_func(self):
        retval = self.func(*self.func_args)
        print(f"After {self._elapsed_time()}: {self._function_signature()} returned {retval}")
        return retval

    def _wait_until_next_check_time(self):
        next_check_at = time.time() + self.backoff_seconds
        while time.time() < next_check_at:
            time.sleep(1)
        self.backoff_seconds = min(60.0, self.backoff_seconds * self.EXPONENTIAL_BACKOFF_FACTOR)

    def _elapsed_time(self):
        elapsed_delta = time.time() - self.start_time
        return self._duration_h_mm_ss(elapsed_delta)

    def _function_signature(self):
        return f"{self.func.__name__}({self.func_args})"

    @staticmethod
    def _duration_h_mm_ss(duration_secs):
        m, s = divmod(duration_secs, 60)
        h, m = divmod(m, 60)
        return "%d:%02d:%02d" % (h, m, s)
