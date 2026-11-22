# Shopping Sessions: C++ pipeline, built as one static binary.
# The scorers are linux/amd64 (Intel Xeon, AVX-512 capable); x86-64-v3 (AVX2) is what the code needs.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
COPY src/ /src/
RUN g++ -O3 -march=x86-64-v3 -std=c++20 -static -pthread -o /pipeline /src/main.cpp

FROM scratch
COPY --from=build /pipeline /app/pipeline
# Reads $DATA_DIR, writes $OUTPUT_DIR; needs no temp files, so nothing else is writable.
CMD ["/app/pipeline"]
