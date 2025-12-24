#!/bin/sh
cron
uvicorn app.services.index:app --host 0.0.0.0 --port 8000
