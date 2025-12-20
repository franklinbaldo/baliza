from datetime import datetime, timedelta, timezone
import pytest
from baliza.utils.dates import humanize_naturaltime

def test_humanize_naturaltime_just_now():
    now = datetime.now(timezone.utc)
    assert humanize_naturaltime(now) == "just now"
    assert humanize_naturaltime(now - timedelta(seconds=59)) == "just now"

def test_humanize_naturaltime_minutes():
    now = datetime.now(timezone.utc)
    assert humanize_naturaltime(now - timedelta(minutes=1)) == "1m ago"
    assert humanize_naturaltime(now - timedelta(minutes=59)) == "59m ago"

def test_humanize_naturaltime_hours():
    now = datetime.now(timezone.utc)
    assert humanize_naturaltime(now - timedelta(hours=1)) == "1h ago"
    assert humanize_naturaltime(now - timedelta(hours=23)) == "23h ago"

def test_humanize_naturaltime_days():
    now = datetime.now(timezone.utc)
    assert humanize_naturaltime(now - timedelta(days=1)) == "1d ago"
    assert humanize_naturaltime(now - timedelta(days=6)) == "6d ago"

def test_humanize_naturaltime_fallback():
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=7)
    # The output should match the YYYY-MM-DD HH:MM format
    assert humanize_naturaltime(old_date) == old_date.strftime("%Y-%m-%d %H:%M")

def test_humanize_naturaltime_future():
    now = datetime.now(timezone.utc)
    future = now + timedelta(seconds=10)
    assert humanize_naturaltime(future) == "in the future"

def test_humanize_naturaltime_naive():
    # Test with naive datetime (assumed local)
    now = datetime.now()
    assert humanize_naturaltime(now - timedelta(minutes=5)) == "5m ago"
