"""bikeshare_sim: the synthetic upstream bike-share operator.

This package is the source system and has no knowledge of the pipeline. Its
modules are filled in during layer 1b (see PLAN.md §8):

    clock.py    accelerated simulated clock (speed / pause / advance)
    world.py    stations, bikes, zones, demand and weather
    engine.py   deterministic event generator driven by the clock
    faults.py   fault injectors plus the ground-truth fault log
    schemas.py  Pydantic event models: the published contract
    api.py      FastAPI app (SSE stream, REST reference data, admin controls)
"""
