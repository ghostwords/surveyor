Requires `top-1m.csv`. Get it from http://s3.amazonaws.com/alexa-static/top-1m.csv.zip

```
usage: survey.py [-h] [-Q] [-s SKIP] [-l LIMIT] [-n NUM_PROCESSES]
                 [-t SECONDS] [-d]
                 PATTERN

positional arguments:
  PATTERN               the regex pattern to search for

optional arguments:
  -h, --help            show this help message and exit
  -Q, --literal         treat PATTERN as literal string, not a regex
  -s SKIP, --skip SKIP  skip this many hostnames from the start
  -l LIMIT, --limit LIMIT
                        stop after this many hostnames
  -n NUM_PROCESSES      use this many processes in parallel (default: 20)
  -t SECONDS, --timeout SECONDS
                        wait this many seconds to connect and again to read
                        before timing out (default: 3.4)
  -d, --debug           enable debugging output
```
