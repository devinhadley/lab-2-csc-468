import json
import os


class Tracer:
    def __init__(self, out_dir, filename="trace.jsonl", debug=False):
        self.path = os.path.join(out_dir, filename)
        self.f = open(self.path, "w")
        self.step = 0
        self.debug = debug

    def emit(self, event):
        """
        Emit a single trace event.
        Automatically increments step and writes JSONL.
        """
        self.step += 1
        event["step"] = self.step

        line = json.dumps(event)
        self.f.write(line + "\n")

        if self.debug:
            print(line)

    def close(self):
        """
        Flush and close the trace file.
        Must be called at the end of the simulation.
        """
        self.f.flush()
        self.f.close()

    def __enter__(self):
        """Allow use as a context manager (with Tracer(...) as tracer)."""
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
