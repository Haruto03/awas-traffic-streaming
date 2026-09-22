# AWAS point-to-point traffic monitoring — Kafka, Spark Structured Streaming, MongoDB

A real-time speed-violation pipeline modelled on Malaysia's AWAS
(Automated Awareness Safety System). Three roadside cameras stream vehicle
sightings into Kafka; Spark Structured Streaming joins the streams to detect
both instantaneous speeding at a camera and average-speed violations between
cameras; violations are merged per car per day into MongoDB; and a Plotly /
Folium dashboard re-queries Mongo every few seconds.

Built by Haruto Iriyama as a two-person university project.

## Architecture

```
camera_event_A.csv ─▶ producer_a.ipynb ─▶ Kafka topic camera-events-A ─┐
camera_event_B.csv ─▶ producer_b.ipynb ─▶ Kafka topic camera-events-B ─┤
camera_event_C.csv ─▶ producer_c.ipynb ─▶ Kafka topic camera-events-C ─┘
                                                                        │
                       data_design_streaming.ipynb (PySpark)            ▼
     ┌──────────────────────────────────────────────────────────────────────┐
     │  event-time watermark (3 min)                                        │
     │  stream–stream joins A↔B, B↔C on car_plate within a 120 s window    │
     │  stream–static joins against camera limits / segment distances       │
     │  instantaneous + average-speed violation detection                   │
     │  foreachBatch → MongoDB bulk upsert, one doc per (car_plate, date)   │
     └──────────────────────────────────────────────────────────────────────┘
                                                                        │
                       visualisation.ipynb ◀── MongoDB (violations) ◀───┘
                       Plotly time series + Folium camera map, 5 s refresh
```

| Notebook | Role |
|---|---|
| `src/producer_a.ipynb`, `producer_b.ipynb`, `producer_c.ipynb` | Replay one camera's CSV into its Kafka topic, one batch every *n* seconds |
| `src/data_design_streaming.ipynb` | MongoDB data model, collections and indexes (§1–4); Spark streaming, joins, violation rules and Mongo sink (§5–11) |
| `src/visualisation.ipynb` | Live dashboard polling MongoDB |
| `data/` | Input CSVs — see [`data/README.md`](data/README.md); not committed |

## Design decisions

- **Online stream–stream join, not a batch-id join.** A↔B and B↔C are joined
  on `car_plate` within a 120 s event-time window under a 3-minute watermark,
  so Spark state stays bounded while every car above 30 km/h between the
  1 km-apart cameras is still matched.
- **Segment distance is computed, not assumed.** Haversine over the camera
  lat/lon in `camera.csv`.
- **One MongoDB document per car per day** with an embedded `violations[]`
  array, written with idempotent `UpdateOne(upsert=True, $push)` bulk
  operations and a 3-attempt exponential-backoff retry, so a partially
  applied batch can be safely retried. A TTL index enforces 24-month
  retention.
- **Vehicle ownership changes** are handled by keeping duplicate plates in
  `vehicle.csv` and letting the most recent registration win on lookup.

---

## 1. Prerequisites

| Component | Version we developed against | Notes |
|---|---|---|
| Python | 3.10+ | Jupyter 7 or JupyterLab 4 |
| Apache Kafka | 3.5+ | Broker reachable at `${AWAS_HOST}:9092` |
| MongoDB | 6.0+ | Server reachable at `${AWAS_HOST}:27017` |
| Apache Spark | 3.3+ | PySpark driven from the notebook; the Kafka connector JAR auto-matches `pyspark.__version__` |

### Python dependencies

```bash
pip install pyspark kafka-python pymongo pandas numpy plotly folium ipywidgets
```

`kafka-python` is fine on most installs. If your environment ships the broken legacy `kafka` PyPI package (whose `simple.py` contains `self.async` and parse-errors on Python 3.7+), every producer notebook automatically falls back to `kafka3` (the maintained fork). To pre-install it:

```bash
pip install kafka3
```

Detailed pinning we developed against (any version newer than these should work):

```text
pyspark>=3.3.0
kafka-python==2.0.2          # or kafka3 as fallback
pymongo==4.6.1
pandas==2.2.2
numpy==1.26.4
plotly==5.21.0
folium==0.16.0
ipywidgets==8.1.2
```

Kafka topics that the notebooks expect (auto-created by the producers on first publish, but you can pre-create them):

```text
camera-events-A
camera-events-B
camera-events-C
```

---


## 2. Configuration — setting the `HOST_IP` variable

Every notebook targets a specific machine by IP address, and this value is set **directly in the code** (not via an OS environment variable).

> ⚠️ **Important:** The IP shown below is only an example. You **must** replace it with **your own machine's IPv4 address**, otherwise Kafka, MongoDB, and Spark will try to connect to the wrong host and fail.

### Finding your IPv4 address

Run the appropriate command for your operating system:

- **Windows:** `ipconfig` → look for the *IPv4 Address* (e.g. `192.168.x.x`)
- **macOS / Linux:** `ifconfig` or `ip a` → look for the `inet` address on your active network interface

### Setup instructions

Before running any cells in Jupyter Lab, open **each of the five notebooks** and set the `HOST_IP` variable to your own IPv4 address:

```python
HOST_IP = '192.168.100.19'   # <-- EXAMPLE ONLY: replace with YOUR IPv4 address
```

Using a single hardcoded variable in each notebook ensures that the Kafka producers, MongoDB connections, and the Spark Structured Streaming sink all target the correct machine — without relying on OS environment variables.


## 3. How to run

> Order matters. Start the streaming consumer before the producers, so Spark's join state begins empty and consumes fresh events.

1. Start infrastructure — bring up the Kafka + MongoDB containers (any docker-compose stack that exposes both is fine).
   - Kafka broker reachable at `${AWAS_HOST}:9092`.
   - MongoDB reachable at `mongodb://${AWAS_HOST}:27017`.
2. Open `src/data_design_streaming.ipynb` and run all cells top-to-bottom.
   - §1–§4 declare the Mongo data model, create collections and indexes (Task 1).
   - §5–§11 start Spark Structured Streaming, the A/B/C joins, violation detection, and the Mongo sink.
   - Leave the streaming query running while the producers publish.
3. In three new kernels, open and run each producer notebook in parallel:
   - `src/producer_a.ipynb`
   - `src/producer_b.ipynb`
   - `src/producer_c.ipynb`
   - Each publishes one batch every `n` seconds. Default `MAX_BATCHES = 300` keeps the demo under 10 minutes; set to `None` for a full 15-hour replay.
4. Open `src/visualisation.ipynb` while the streaming pipeline is still running. The §0 sanity-check cell will tell you exactly what is wrong if no data has reached Mongo yet. The dashboard refreshes every 5 s.

To stop cleanly, interrupt the producer notebooks first, then run `query.stop()` in the streaming notebook (§12), then shut down infrastructure.

---


## 4. Key parameters (also documented inside `data_design_streaming.ipynb` §1.2)

| Parameter | Value | Why |
|---|---|---|
| Producer publish interval `n` | 1 s | Keeps a demo run to about 5 minutes (300 batches × 1 s = 300 s). Same `n` across A/B/C. |
| `MAX_BATCHES` (per producer) | 300 | ~5 min demo. Producer A publishes ~6 000 events; B and C publish fewer because their CSVs have smaller per-batch counts. `None` = full file (~7.7 h at `n = 1 s`). |
| `DEMO_EVENT_MINUTES` (per producer) | 30 | Event-time cap applied at CSV load. All three producers emit only the first 30 minutes of camera-time so A/B/C overlap on the same window — keeps the stream-stream join densely matched. `None` = no event-time cap (full file). |
| Watermark on every stream | 3 minutes | Bounds Spark state; absorbs the worst observed B↔C arrival skew |
| Join window A↔B and B↔C | 120 s | Covers all cars ≥ 30 km/h between the 1 km cameras (slowest observed ≈ 60 km/h) |
| Trigger interval | 5 s | foreachBatch cadence — small enough to look "live", large enough to amortise Mongo writes |
| Daily merge key | `(car_plate, date)` | One Mongo doc per car per day with embedded `violations[]` array |
| Mongo writes | `bulk_write` with `UpdateOne(upsert=True, $push)` | Idempotent + efficient sink |
| Retention | 24 months | TTL on derived `expires_at = date + 730 days` |
| Visualisation refresh | every 5 s, cap 360 ticks (~30 min) | Re-run the §4 cell to extend |

---


## 5. Violation detection rules

- Instantaneous: flag the event if `speed_reading > camera.speed_limit` at the recording camera. A single car can be flagged independently at cameras 1, 2 and 3.
- Average (segment): for every matched pair (A→B and B→C) where the same `car_plate` is observed in event-time order within the join window, compute
  `avg_speed = distance(cam_start, cam_end) / (t_end − t_start)`.
  Flag if `avg_speed > speed_limit(cam_end)` — the ending camera's limit (the ending camera's limit).
- The cam-to-cam distance is computed from `camera.csv` lat/lon using the Haversine formula — not hardcoded to 1 km.
- All violations for the same `(car_plate, date)` are merged into a single Mongo document under a `violations[]` array.

---


## 6. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Producer / streaming / viz raise `ConnectionRefusedError` to Kafka or Mongo | `AWAS_HOST` is wrong | Set `export AWAS_HOST=<correct host IP>` and restart the Jupyter kernel |
| Producer fails on `from kafka import KafkaProducer` with `SyntaxError: self.async` | The broken legacy `kafka` PyPI package is installed | The producer already falls back to `kafka3` automatically — install it: `pip install kafka3` |
| Streaming notebook hangs on Kafka read with no events | Spark Kafka connector version mismatch | We pin the JAR coordinate to `pyspark.__version__` automatically; if Spark itself is older than 3.3 the connector may not exist — upgrade PySpark |
| Visualisation says `No violations in MongoDB yet` but I started the producers | Either Spark is not running, or it can't reach Kafka | Run the §0 sanity-check cell — it lists the four things to check, in order |
| Visualisation displays the header but no plots appear under `clear_output` refresh | Plotly JS mount not re-attached after `clear_output` | Already fixed — `pio.renderers.default = 'notebook_connected'` is set in cell 1 |
| Streaming logs `[batch=N] no violations in this microbatch` repeatedly | Healthy idle — producers haven't published anything new, or the stream-stream join is buffering | Confirm the producer notebooks are still logging "published batch_id=..." |
| `BulkWriteError` once, then succeeds | Mongo retry path triggered | Expected — the sink has 3-attempt exponential-backoff retries with idempotent ops, so partial-success retries are safe |

---


## 7. Assumptions

- The vehicle CSV header `vechicle_type` is a typo kept as-is in the source file, renamed to `vehicle_type` after ingest.
- Producer A → camera_id 1, Producer B → camera_id 2, Producer C → camera_id 3 (consistent with `camera.csv` and observed in the event CSVs).
- Same `batch_id` across producers is not required to align in event time. We do not join on `batch_id`; we do a true online stream-stream join on event-time + watermark.
- Duplicate `car_plate` rows in `vehicle.csv` are kept; the most recently registered row wins on lookup, to model ownership changes.
- "Online join" is a stream-stream (unbounded) join, not a stream-static join.
- Daily aggregation key is the civil date of the start timestamp of the violation, naive UTC (matches the timestamp format in the source CSVs).
- The Haversine formula is used to compute segment distances from `camera.csv` lat/lon — accurate to a few metres at sub-100 km distances; never hardcoded.
