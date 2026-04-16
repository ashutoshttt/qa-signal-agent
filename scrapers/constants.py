"""Shared constants for all scrapers."""

KEYWORDS = [
    "QA Engineer",
    "Quality Assurance Engineer",
    "QA Analyst",
    "Quality Analyst",
    "Software Tester",
    "Manual Tester",
    "QA Tester",
    "SDET",
    "Test Automation Engineer",
    "Automation Test Engineer",
    "QA Automation Engineer",
    "Automation Tester",
    "QA Lead",
    "QA Manager",
    "Test Lead",
    "Test Manager",
    "Lead QA Engineer",
    "Senior QA Engineer",
    "Principal QA Engineer",
    "Staff QA Engineer",
    "Head of QA",
    "Head of Testing",
    "Head of Quality",
    "Director of QA",
    "Director of Quality Assurance",
    "Performance Test Engineer",
    "Performance Tester",
    "Load Test Engineer",
    "Mobile QA Engineer",
    "API Test Engineer",
    "Quality Engineer",
    "Software Quality Engineer",
    "Automation QA Lead",
    "Engineering Manager QA",
]

INDIA_LOCATIONS = {
    "india",
    "bangalore",
    "bengaluru",
    "mumbai",
    "delhi",
    "new delhi",
    "hyderabad",
    "pune",
    "chennai",
    "kolkata",
    "noida",
    "gurgaon",
    "gurugram",
    "ahmedabad",
}


def is_india_location(location: str) -> bool:
    """Return True if location string refers to India or a major Indian city."""
    loc_lower = location.lower()
    return any(city in loc_lower for city in INDIA_LOCATIONS)
