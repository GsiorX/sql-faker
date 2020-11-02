import time


class Timer:
    def __init__(self, name):
        self.name = name
        self.sum = 0

    def measure(self, measure_name):
        timer = self

        class TimerWrapper:
            def __enter__(self):
                self.start = time.perf_counter()

            def __exit__(self, exc_type, exc_value, traceback):
                end = time.perf_counter()
                diff = end - self.start
                timer.sum += diff

                print(f"{timer.name}-{measure_name}: {diff:0.4f} seconds")

        return TimerWrapper()

    def accumulated(self):
        print(f"Total for {self.name}: {self.sum:0.4f} seconds\n")
