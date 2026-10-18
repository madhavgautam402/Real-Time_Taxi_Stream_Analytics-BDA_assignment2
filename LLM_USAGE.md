# LLM Usage

## Tools used

- Claude (Anthropic), via claude.ai: planned the architecture, wrote and debugged most of the code, helped interpret results.


## Five prompts that materially helped



1. **Project plan from scratch.** "Help me do this assignment and give me a initial plan first. I am working in VS Code and I have the parquet file. Tell me the folder layout, and first tell that all the tools that are required to install." This produced the tool checklist, the folder layout from the submission spec, the Docker Compose file for Kafka, and the config / topic-creation / producer code.

2. **Choosing the engine and the Windows setup.** I pasted my PowerShell output (Python 3.12, Java 21, Docker, Git) and asked how to proceed on native Windows. This led to the choice of Spark Structured Streaming with `foreachBatch`, installing Java 17, `winutils`, and a helper that sets `JAVA_HOME` and `HADOOP_HOME` automatically.

3. **Stage 1 and Stage 2 design.** After the producer worked I asked for Stage 1 (clean, enrich, Bloom filter that marks instead of dropping) and then Stage 2 (reservoir, event vs entity sampling, HyperLogLog, DGIM, event-time windows with watermark and allowed lateness). This produced `common/bloom.py`, `common/sketches.py`, and the window and late-event logic.

4. **Debugging a hung experiment.** I pasted the logs of a run that waited for 30 minutes ("waiting for data..."). The producer log showed `numpy ArrayMemoryError`. I asked for a full corrected `run_pipeline.py`. This gave the cached taxi segment, loading only the needed parquet columns, and the fail-fast behaviour when any process crashes.

5. **Experiment batch and restart test.** I pasted the P1-P4, K2 and rate results and asked how to interpret them, then asked for the restart test (Task 5.2). This produced `run_experiments.py`, `restart_test.py`, and the reasoning about why 6 consumers do not beat 4 on a 4-partition topic and why throughput did not scale.

## Cases where the LLM output was wrong, inefficient or unsuitable

1. **Producer ran out of memory (unsuitable for the pipeline).** The first producer loaded the full 3.7M-row parquet file with all columns while three Spark JVMs were already running. It crashed with `ArrayMemoryError`, and the pipeline script then waited 30 minutes for data that never came. Changes: the producer reads only the 8 needed columns, caches the cleaned segment in `data/segment_*.parquet`, `run_pipeline.py` builds it before starting Spark, and the script now stops within seconds if any process exits with an error.
2. **Wrong throughput metric.** The producer's `actual_eps` divided by total time including the wait for delayed events, giving 185/s when the producer ran at 500/s. Fixed to measure only the sending phase and report the drain time separately.
3. **Overstated DGIM accuracy and Linux-only commands.** The LLM first said DGIM would be "within a few percent"; measured error was 9-34% with 2 buckets per size (the theoretical bound is 50%). We made the buckets per size configurable (`dgim_r`) and measured r = 2, 4, 8 to show the trade-off. Early setup commands (`python3`, `nproc`, `free -h`, bash `mkdir -p`) were Linux commands and did not work in my Windows PowerShell; they were replaced with PowerShell equivalents.
4. **Intermittent Spark failure.** One experiment run (K2) died at start-up with a `NullPointerException` in Spark's block manager heartbeat, a known race in local mode on a busy machine. The fix was not in our code: `run_experiments.py` now retries a failed run once.

## What I can explain without the LLM

I can explain the data flow of the whole pipeline and why each stage reads and writes Kafka topics.
I may struggle to explain the codes line by line because I took much AI help for the coding part.