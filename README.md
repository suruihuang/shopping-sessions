# Shopping Sessions: C++ pipeline

One static binary, no engine. It reads the mounted CSV files, applies the rules in `rules.md`, and writes `user_daily/` and `campaign_daily/` as Parquet. The image is 1.7 MB and starts in about a millisecond.

`rules.md` is the specification. If this README and `rules.md` disagree, `rules.md` wins.

## Layout

- `src/main.cpp` is the whole pipeline.
- `Dockerfile` builds the static binary with g++ and copies it into an empty image.
- `fixtures/` holds the 36 edge cases from the starter.
- `scripts/` holds the starter's runners (`run_fixtures.py`, `compare.py`, `docker_run.sh`) and `dev_check.sh`.

## Build and check

You need Docker. The test scripts need Python 3.10+ with `polars`.

```sh
make build                     # linux/amd64 image
make fixtures                  # the image on all 36 fixtures, with the scorer's flags
make dev                       # 1M-event dev set (needs `generate` on PATH)
make run-dev                   # the image on dev/, timed
make check-dev ANSWER=dev-answer
```

To iterate without rebuilding the image, compile and run the fixtures inside any Linux container that has g++ and polars:

```sh
sh scripts/dev_check.sh        # builds with -Wall -Wextra, runs the fixtures under 6 configurations
```

`dev_check.sh` varies the thread count, the chunk size (down to one byte) and the partition counts, and runs once with every fast path switched off. Every configuration must pass all 36 fixtures, because none of those settings may change the output.

The image holds nothing but the binary. `scripts/docker_run.sh` cleans up after a run by calling `chmod` inside the image, which this image does not have: on a Linux host where you are not uid 1000, the output folder stays owned by uid 1000 and you remove it with `sudo`.

## How it works

Every phase runs on all threads.

1. **Links.** The link files are parsed into ID pairs. A lock-free hash table holds every linked ID, with a union-find over its slots. A union always keeps the byte-wise smaller ID as the root, so the root is the person's name. A closing pass stores that name in every slot.
2. **Events and clicks.** The files are memory-mapped and cut into 1 MB units of work. Each event line produces:
   - a 16-byte *token* (event ID, `ingest_ts`, address of its record), filed by a hash of the event ID;
   - if the line is valid, a 16-byte *record* (person, `ts`, price, category), filed by a hash of the person.

   A valid click produces a 24-byte record filed by person.
3. **Dedup.** Each token partition fits in cache. Among the copies of an event the smallest `ingest_ts` wins, and every other copy's record is marked dropped. If the winner was invalid it has no record, so the event is gone, as the rules require.
4. **Per person.** Each person partition is grouped by person and sorted by time. Sessions, credit and assists are computed in one walk, and `user_daily` rows are written a row group at a time. `campaign_daily` is summed from per-thread grids at the end.

### Choices that keep it exact

- **IDs are integers, losslessly.** An ID of up to 10 characters from `[0-9A-Za-z_]` is packed into 60 bits, 6 bits a character in ASCII order, so integer order equals byte order. Any other ID (a space, a tab, a non-ASCII byte, longer than 10) is interned in an exact string table and compared through its bytes. No hash stands in for an ID anywhere.
- **The CSV dialect is hand-written.** Only `\n` ends a line, every comma separates fields, and a quote is legal only as the first and last character of a field. A line passes when its quotes number exactly twice its fields that begin and end with one.
- **Timestamps have a fast path and a general path that accept the same strings.** The fast path looks the ten date characters up in a table of the days around the window; a date that is not there cannot be in a window. The general path does the full calendar check.
- **Click duplicates are not removed.** The rules say copies of a click are identical, so they can change neither the credited campaign nor the set of assisting campaigns.
- **`ingest_ts` in a token is whole seconds in 32 bits.** It orders copies exactly as the rules do while `ingest_ts` is whole seconds (the rules: always F1) and within about 34 years before and 100 years after the window.

### Limits that end the run instead of giving a wrong answer

- A window longer than about 42 days (timestamps are kept as 32-bit milliseconds from 7 days before the window).
- More than 254 product categories.

## Testing knobs

These environment variables exist for testing and must never change the output.

| Variable | Effect |
| --- | --- |
| `NCPU` | Threads (the scorer sets 8) |
| `SS_CHUNK` | Bytes of input per unit of work, default 1048576 |
| `SS_PARTS`, `SS_TP` | Person and token partition counts |
| `SS_NOWC` | Plain stores instead of write-combining lines and non-temporal stores |
| `SS_NOTHP` | Do not ask for huge pages |
| `SS_NOFAST` | Timestamps through the general parser only |
| `SS_NOMMAP` | Read the inputs into memory instead of mapping them (the path taken when a file cannot be mapped) |

## Measured

On a laptop (i7-13620H, 8 threads, files in the page cache), whole process:

| Dataset | Time | Peak memory |
| --- | --- | --- |
| 20M events, 5M clicks | about 0.6 s | 2.7 GB including the mapped input |
| 60M events, 15M clicks | about 1.8 s | 7.9 GB including the mapped input |

The scorer has 4 cores with 8 hyperthreads, so its times will differ; only the board's times count.
