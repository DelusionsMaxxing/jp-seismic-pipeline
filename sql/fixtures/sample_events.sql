-- A handful of real USGS payloads, used by CI and by `make demo` so the
-- transformation layer can be exercised without hitting the network.

INSERT INTO raw.earthquake_events (event_id, payload, source) VALUES
(
    'us6000tj22',
    '{
        "type": "Feature",
        "id": "us6000tj22",
        "properties": {
            "mag": 4.2, "place": "3 km WNW of Takahagi, Japan",
            "time": 1786128091903, "updated": 1786129493040,
            "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us6000tj22",
            "status": "reviewed", "tsunami": 0, "sig": 271,
            "magType": "mb", "type": "earthquake"
        },
        "geometry": {"type": "Point", "coordinates": [140.68, 36.7231, 10]}
    }'::jsonb,
    'usgs'
),
(
    'us7000fixt2',
    '{
        "type": "Feature",
        "id": "us7000fixt2",
        "properties": {
            "mag": 6.1, "place": "148 km E of Miyako, Japan",
            "time": 1786038640068, "updated": 1786044697040,
            "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000fixt2",
            "status": "reviewed", "tsunami": 1, "sig": 573,
            "magType": "mww", "type": "earthquake"
        },
        "geometry": {"type": "Point", "coordinates": [143.51, 39.62, 32.4]}
    }'::jsonb,
    'usgs'
),
(
    'us7000fixt3',
    '{
        "type": "Feature",
        "id": "us7000fixt3",
        "properties": {
            "mag": 5.3, "place": "Bonin Islands, Japan region",
            "time": 1785952240000, "updated": 1785955240000,
            "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000fixt3",
            "status": "reviewed", "tsunami": 0, "sig": 432,
            "magType": "mb", "type": "earthquake"
        },
        "geometry": {"type": "Point", "coordinates": [142.11, 27.35, 480.2]}
    }'::jsonb,
    'usgs'
),
(
    'us7000fixt4',
    '{
        "type": "Feature",
        "id": "us7000fixt4",
        "properties": {
            "mag": 3.4, "place": "Sea of Japan",
            "time": 1785865840000, "updated": 1785868840000,
            "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000fixt4",
            "status": "automatic", "tsunami": 0, "sig": 178,
            "magType": "ml", "type": "earthquake"
        },
        "geometry": {"type": "Point", "coordinates": [135.02, 38.11, 15.0]}
    }'::jsonb,
    'usgs'
)
ON CONFLICT (event_id) DO NOTHING;
