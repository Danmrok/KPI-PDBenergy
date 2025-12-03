# Dnypro Cascade Hydroelectric Power Stations - Kafka Analytics Platform

**Option 3, Sub-option B (Analytics Focus)**

A project for testing and analyzing Apache Kafka performance for processing data from hydroelectric power stations. The system collects metrics from 15 hydroelectric units, generates synthetic data, and tests various Kafka configurations for analytics optimization.

---

## 📋 Project Structure

```
scripts/
├── hydro_data_generator.py      # Synthetic hydroelectric data generator
├── hydro_analytics.py           # Kafka testing and benchmark
├── hydro_test_1000.json         # Sample dataset (1000 records)
└── README.md
```

---

## 🚀 System Components

### 1. **hydro_data_generator.py** - Data Generator
Generates realistic data from hydroelectric power stations of the Dnieper Cascade.

**Generated parameters per unit:**
- `device_id` - unique identifier (HYDRO_DN_001..015)
- `power_output` - generation power (kW)
- `efficiency` - turbine efficiency (%)
- `temperature` - cooling temperature (°C)
- `voltage` - voltage (V)
- `current` - current (A)
- `status` - status ("generating", "standby", "maintenance")
- `location` - GPS coordinates
- `maintenance_hours` - operating hours
- `water_flow` - water flow rate (m³/s)
- `water_level` - water level (m)
- `turbine_type` - turbine type ("kaplan", "francis", "pelton")

**Built-in realistic patterns:**
- ✓ Daily water flow cycles (peak at noon)
- ✓ Seasonal temperature variations
- ✓ Probabilistic status transitions (85% generating, 10% standby, 5% maintenance)
- ✓ Correlation between power and output

**Usage:**
```bash
python hydro_data_generator.py
```

**Result:** Generates `hydro_test_1000.json` with 1000 records in JSONL format (one JSON per line).

---

### 2. **hydro_analytics.py** - Kafka Testing
Complete Kafka performance testing suite with focus on analytics scenarios.

**Test stages:**

#### **STAGE 1: Topic Creation**
Creates 11 specialized topics:
- `hydro-main` (4 partitions, 30 days retention) - main stream
- `hydro-batch-test` (3 partitions) - batch parameter testing
- `hydro-comp-*` (3 partitions each) - compression tests (none, snappy, lz4, zstd)
- `hydro-part-*` (3 partitions each) - partitioning tests (2, 4, 8 partitions)
- `hydro-analytics` (8 partitions, 1 year retention, segment.ms=1 day) - analytics

#### **STAGE 2: BATCH/LINGER Testing**
Tests impact of buffering on throughput:
```
batch.size × linger.ms combinations:
├─ 16 KB × 0 ms      → Minimal latency, low throughput
├─ 64 KB × 10 ms     
├─ 256 KB × 50 ms    → Balanced parameters
├─ 512 KB × 100 ms   
├─ 1 MB × 200 ms     → Optimal for analytics
└─ 1 MB × 500 ms     → Maximum throughput
```

**Metrics:** throughput (rec/sec), avg/p95/p99 latency (ms), duration

#### **STAGE 3: COMPRESSION Testing**
Compares compression algorithms on 3000 records:

| Algorithm | Compression Ratio | Characteristics |
|-----------|-------------------|-----------------|
| **none** | 0% | Baseline |
| **snappy** | ~60% | Fast, balanced |
| **lz4** | ~65% | Medium compression, fast |
| **zstd** | ~75% | Best compression, slower |

**Metrics:** throughput, avg_latency, original_size, compressed_size, ratio, network_saved

#### **STAGE 4: PARTITIONING Testing**
Measures scalability with partition count (2 → 4 → 8) partitioned by `turbine_type`.

**Metrics:** throughput, scaling_factor relative to baseline, duration

#### **STAGE 5: Summary Report**
Comparative recommendations for analytics configuration.

**Usage:**
```bash
python hydro_analytics.py
```

**Requirements:** Kafka broker must be running on `localhost:9092`.

## 🔧 Installing Dependencies

```bash
pip install kafka-python
```

**Dependencies:**
- `kafka-python` - Apache Kafka client
- `json`, `random`, `datetime`, `statistics`, `time` - Python built-in modules

---

## 📈 Analytics Recommendations

Based on test results:

### ✅ Optimal Configuration

```
Producer Config:
├─ batch.size: 1 MB (1048576)
├─ linger.ms: 200 ms
├─ compression.type: zstd
├─ acks: 1 (or all for critical data)
└─ retries: 3

Topic Config:
├─ partitions: 4-8 (depends on parallelism)
├─ replication_factor: 1-2
├─ retention.ms: 31536000000 (1 year for history)
├─ segment.ms: 86400000 (1 day)
├─ cleanup.policy: delete
└─ compression.type: zstd

Consumer Config:
├─ fetch.min.bytes: 16 KB
├─ fetch.max.wait.ms: 500 ms
└─ session.timeout.ms: 30 sec
```

### 📊 Expected Results

- **Throughput:** 1000+ records/sec with current configuration
- **Compression:** ~75% network traffic reduction (zstd)
- **Latency:** P99 < 50 ms for 1 MB batch size
- **Network Saved:** ~6-7 MB per 10,000 records

### 🎯 Usage Scenarios

1. **Real-time Monitoring:** batch_size=16KB, linger.ms=0, compression=snappy
2. **Batch Analytics:** batch_size=1MB, linger.ms=200ms, compression=zstd
3. **Archival Storage:** compression=zstd, retention=1 year, segment=1 day

---

## 🔌 Kafka Integration

### Running Local Kafka (Docker)
```bash
# Start Zookeeper
docker run -d --name zookeeper -p 2181:2181 \
  -e ZOO_CFG_EXTRA="standaloneEnabled=false" \
  zookeeper

# Start Kafka
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  confluentinc/cp-kafka:7.0.0
```
