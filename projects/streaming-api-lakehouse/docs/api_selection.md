# Streaming API Selection

Source reference: `apis_for_data_engineering_projects.pdf`.

Only APIs that support live event feeds, real-time data, or streaming-style polling were selected.

| API | Reason selected | Pattern |
| --- | --- | --- |
| Wikimedia EventStreams / APIs | Native real-time event feed for Wikimedia changes | Server-sent events |
| MTA Real-Time API | Real-time subway feeds for delay detection | GTFS real-time / polling |
| Coinbase Exchange API | Market data is naturally event oriented | Polling or market feed |
| OpenSky Network API | Aircraft positions can be captured periodically | Polling |
| USGS Earthquake API | New events can be incrementally discovered | Incremental polling |
| USGS Water Services | Sensor-style river and streamflow time series | Sensor polling |
| openSenseMap API | Environmental sensor readings | Sensor polling |
| GDELT API | Worldwide news can be continuously ingested | Continuous polling |
| Hacker News API | Stories/comments can be ingested as trending events | Continuous polling |

Batch-only, lookup-only, catalog, dimension, and historical APIs from the PDF were excluded from this project.
