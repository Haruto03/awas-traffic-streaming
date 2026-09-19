# Data

The notebooks expect the course-provided AWAS dataset here (not committed —
~145 MB of CSV):

| File | Contents |
|---|---|
| `camera.csv` | `camera_id, latitude, longitude, position, speed_limit` — three roadside cameras |
| `vehicle.csv` | `car_plate, owner_name, owner_addr, vechicle_type, registration_date` (the header typo is in the source data) |
| `camera_event_A.csv`, `_B.csv`, `_C.csv` | `event_id, batch_id, car_plate, camera_id, timestamp, speed_reading` — one file per camera, replayed by the producer notebooks |
| `camera_event_historic.csv` | Historic events used to seed the database |
