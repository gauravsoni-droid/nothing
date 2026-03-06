import asyncio
import csv
import re
from playwright.async_api import async_playwright

INPUT_FILE = "barfoot_rental_urls_available_now.csv"
OUTPUT_FILE = "barfoot_rental_data_available_now.csv"

# CSV columns matching requirements: location, sale type, description, all agent name:number, url
OUTPUT_FIELDS = [
    "URL",
    "Location",
    "Sale_Type",
    "Description",
    "Agents",
]


async def get_text(page, selector: str, default: str = "") -> str:
    """Safely get inner text from first matching element."""
    try:
        loc = page.locator(selector).first
        if await loc.count() > 0:
            return (await loc.inner_text()).strip()
    except Exception:
        pass
    return default


async def scrape():
    # Read URLs from CSV (skip header row for URL column)
    urls = []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("URL", "").strip()
            if url and url.startswith("http"):
                urls.append(url)

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for i, url in enumerate(urls, 1):
            print(f"[{i}/{len(urls)}] Scraping: {url}")
            try:
                await page.goto(url, timeout=60000)
                await page.wait_for_timeout(3000)
            except Exception as e:
                print(f"  ⚠ Failed to load: {e}")
                results.append({**dict.fromkeys(OUTPUT_FIELDS, ""), "URL": url})
                continue

            # --- Location (property title/address) ---
            location = await get_text(page, "h1")

            # --- Sale type / price banner (e.g. "For Sale $430,000", "For sale by negotiation", "Deadline sale", "$1,350 per week, available 30th March") ---
            sale_type = ""
            try:
                sale_pattern = re.compile(
                    r"(for sale|sale by|deadline sale|auction|tender|for rent|per week|weekly rent|per month|pw\b|available\s+\d)",
                    re.IGNORECASE,
                )
                # Common containers for price / sale info
                sale_locators = page.locator(
                    "[class*='price'], [class*='sale'], [data-testid*='price'], .price, .tagline, .listing-price, header *, h2, h3"
                )
                texts = await sale_locators.all_inner_texts()
                for text in texts:
                    text = (text or "").strip()
                    if text and sale_pattern.search(text) and len(text) < 160:
                        sale_type = text
                        break
                # Fallback: scan body text for first sentence containing sale keywords
                if not sale_type:
                    full = (await page.inner_text("body")).strip()
                    match = sale_pattern.search(full)
                    if match:
                        start = max(0, match.start() - 60)
                        end = min(len(full), match.end() + 60)
                        snippet = full[start:end]
                        lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]
                        if lines:
                            sale_type = lines[0][:160]
            except Exception:
                pass

            # --- Description ---
            description = ""
            for selector in (
                "[class*='description']",
                "section[aria-label*='escription']",
                "[data-testid*='description']",
                "section",
            ):
                description = await get_text(page, selector)
                # Prefer a block that looks like prose (multiple words, not just one line)
                if description and len(description) > 80 and " " in description:
                    break
            if not description:
                try:
                    desc_loc = page.locator("section").nth(1)
                    if await desc_loc.count() > 0:
                        description = (await desc_loc.inner_text()).strip()
                except Exception:
                    pass

            # --- Agents: all "name : phone" pairs in a single column ---
            agents = []
            try:
                tel_loc = page.locator('a[href^="tel:"]')
                tel_count = await tel_loc.count()
                seen_phones = set()

                for idx in range(tel_count):
                    link = tel_loc.nth(idx)
                    href = await link.get_attribute("href") or ""
                    raw_phone = href.replace("tel:", "").strip()
                    phone_digits = re.sub(r"[^0-9+]", "", raw_phone)
                    if not phone_digits or phone_digits in seen_phones:
                        continue
                    seen_phones.add(phone_digits)

                    name = ""

                    def _is_sale_text(t: str) -> bool:
                        l = t.lower().strip()
                        return bool(
                            "for sale" in l
                            or "by negotiation" in l
                            or "negotiation" in l
                            or "auction" in l
                            or "tender" in l
                            or "for lease" in l
                            or "deadline" in l
                            or l.startswith("call ")
                            or "$" in t
                            or "gst" in l
                            or re.match(r"^[\d\s\-+()]{6,}$", t)
                        )

                    try:
                        # Prefer the dedicated contact card wrapping the phone number,
                        # e.g. div.ContactListingPerson / div.Person / div.PersonDetails.
                        parent = link.locator(
                            "xpath=ancestor::*[contains(@class,'ContactListingPerson') or contains(@class,'listing-contact-person') or contains(@class,'Person')][1]"
                        )
                        if await parent.count() == 0:
                            # Fallback: nearest section/div that contains this tel link
                            parent = link.locator(
                                "xpath=ancestor::section[.//a[starts-with(@href,'tel:')]][1]"
                            )
                        if await parent.count() == 0:
                            parent = link.locator(
                                "xpath=ancestor::div[.//a[starts-with(@href,'tel:')]][1]"
                            )

                        if await parent.count() > 0:
                            card = parent.first
                            # First, try an explicit people-link inside the card (e.g. /our-people/...)
                            people = card.locator("a[href*='/our-people/'], a[href*='people']").first
                            if await people.count() > 0:
                                candidate = (await people.inner_text()).strip()
                                if candidate and not _is_sale_text(candidate):
                                    name = candidate

                            # Fallback: scan the card text for the line that looks most like a name
                            if not name:
                                parent_text = (await card.inner_text()).strip()
                                for line in parent_text.split("\n"):
                                    line = line.strip()
                                    if not line or _is_sale_text(line):
                                        continue
                                    # Heuristic: real names usually have few words with letters
                                    if re.search(r"[A-Za-z]", line) and len(line.split()) <= 4:
                                        name = line
                                        break
                    except Exception:
                        pass

                    if name:
                        agents.append(f"{name} : {raw_phone[:30]}")
            except Exception:
                pass

            agents_str = " | ".join(agents)

            results.append({
                "URL": url,
                "Location": location,
                "Sale_Type": sale_type,
                "Description": description,
                "Agents": agents_str,
            })

        await browser.close()

    # Save to CSV
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved scraped data to {OUTPUT_FILE} ({len(results)} rows)")


if __name__ == "__main__":
    asyncio.run(scrape())
