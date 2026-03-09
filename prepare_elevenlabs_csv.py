import csv
from pathlib import Path


INPUT_FILE = "data/barfoot_rural_data.csv"
OUTPUT_FILE = "elevenlabs_kB/barfoot_rural_elevenlabs.csv"

# Columns for ElevenLabs-ready CSV. Adjust names if their schema changes.
OUTPUT_FIELDS = [
    "Title",
    "Text",
    "URL",
]


def build_text(row: dict) -> str:
    """Combine scraped fields into a single text block for the KB."""
    parts = []

    location = (row.get("Location") or "").strip()
    if location:
        parts.append(f"Location: {location}")

    sale_type = (row.get("Sale_Type") or "").strip()
    if sale_type:
        parts.append(f"Sale type: {sale_type}")

    agents = (row.get("Agents") or "").strip()
    if agents:
        parts.append(f"Agents: {agents}")

    url = (row.get("URL") or "").strip()
    if url:
        parts.append(f"URL: {url}")

    description = (row.get("Description") or "").strip()
    if description:
        # Add a blank line before the full description body for readability
        parts.append("")
        parts.append("Description:")
        parts.append(description)

    # Join with newlines so each listing becomes one coherent chunk of text.
    return "\n".join(p for p in parts if p is not None)


def transform():
    input_path = Path(INPUT_FILE)
    output_path = Path(OUTPUT_FILE)

    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    with input_path.open(newline="", encoding="utf-8") as f_in, output_path.open(
        "w", newline="", encoding="utf-8"
    ) as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()

        for row in reader:
            url = (row.get("URL") or "").strip()
            location = (row.get("Location") or "").strip()

            title = location or url or "Barfoot & Thompson listing"
            text = build_text(row)

            writer.writerow(
                {
                    "Title": title,
                    "Text": text,
                    "URL": url,
                }
            )

    print(f"Created ElevenLabs-ready CSV at {output_path}")


if __name__ == "__main__":
    transform()

