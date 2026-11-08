# Project Story: Engineering TransitPulse

When CUMTA and Easwari Engineering College announced Problem Statement 6 for the March 2026 Mobility Hackathon, I wanted to move beyond static exploratory charts and build a functional decision-support workbench for transit authorities. The core problem was clear: millions of commuters search for bus and rail routes on the Chennai One mobile app, but nearly half never book a ticket. Traditional manual surveys miss this latent demand entirely.

My biggest challenge was data scale and semantic integrity. The raw search dataset exceeded 1.3 GB, containing millions of rows with circular test records and orphan journeys. Standard database queries caused latency, so I engineered an in-memory ETL pipeline using DuckDB and PyArrow. This allowed me to cleanse the telemetry, match searches to tickets deterministically by journey ID, and compute corridor-level aggregations in under two minutes while maintaining zero personal identifiers. Another challenge was avoiding revenue inflation on premium bus corridors; by implementing parallel standard route fare mapping, I ensured economic projections remained defensible.

Through this project, I learned the importance of establishing a single source of truth early. Decoupling the economic calculation engine from the Streamlit presentation layer guaranteed that KPI cards, simulation dials, and data exports remained in mathematical agreement. 

Moving forward, the logical next step is ingesting live GTFS-RT automated vehicle location (AVL) feeds to correlate search-to-ticket dropoffs directly with physical bus bunching and headway delays across the Chennai Metropolitan Area.

