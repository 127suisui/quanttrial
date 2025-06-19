# Blockhouse Quant Developer Trial

## Day 1 – Kafka Producer
* `kafka_producer.py` streams **l1_day.csv** (59 752 rows) ⇒ topic `mock_l1_stream`.
* Speed factor adjustable via `--speed`.

## Day 2 – Backtest & Allocator
* `allocator.py` implements static Cont–Kukanov split.
* `backtest.py`:
  * Consumes Kafka stream.
  * Grid-searches λ_over / λ_under / θ_queue.
  * Benchmarks vs BestAsk, TWAP, VWAP.
  * Outputs required JSON (see `output.json`).

## Day 3 – Reproducible Environment
```bash
# start infra
cd kafka-docker
docker compose up -d

# run producer (once)
python kafka_producer.py --csv l1_day.csv --speed 10

# run backtest
python backtest.py > output.json
