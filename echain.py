import sys
import requests
import json
import itertools

from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option
@Configuration()
class ExStreamCommand(StreamingCommand):
    src_field = Option(require=True)
    dst_field = Option(require=True)
    max_seconds = Option(require=True)
    min_tries = Option(require=True)
    def stream(self, records):
        for record in records:
            record[self.dst_field] = []
            starts_from = 0.0
            if str(type(record[self.src_field])) != '<class \'str\'>':
                for current_range in range(0, len(record[self.src_field])):
                    start_time = 0.0
                    curr_try = 0
                    valid = []
                    r = []
                    for val in record[self.src_field]:
                        tmp = float(val)
                        r.append(tmp)
                    r = sorted(r)
                    for tm in r:
                        curr_try += 1
                        if curr_try >= starts_from + 1:
                            if start_time == 0:
                                start_time = tm
                            if float(tm) <= float(start_time) + float(self.max_seconds):
                                valid.append(tm)
                            else:
                                curr_try -= 1
                                break
                    if len(valid) >= int(self.min_tries):
                        record[self.dst_field].append(','.join(str(x) for x in valid))
                        starts_from = curr_try
                    else:
                        starts_from += 1
            yield record

if __name__ == "__main__":
    dispatch(ExStreamCommand, sys.argv, sys.stdin, sys.stdout, __name__)