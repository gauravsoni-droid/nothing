import asyncio
import csv
import math
from playwright.async_api import async_playwright

BASE_URL = "https://www.barfoot.co.nz"
START_URL = f"{BASE_URL}/properties/rural"
OUTPUT_FILE = "barfoot_rural_urls.csv"
PAGE_SIZE = 48


async def scrape_urls():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        all_urls = set()

        print(f"Loading page 1: {START_URL}")
        await page.goto(START_URL, timeout=60_000)
        await page.wait_for_timeout(5_000)

        total_text = await page.locator('[data-total-listings-attr]').get_attribute("data-total-listings-attr")
        total = int(total_text) if total_text else 274
        total_pages = math.ceil(total / PAGE_SIZE)
        print(f"Total listings: {total}, Pages: {total_pages}")

        for pg in range(1, total_pages + 1):
            if pg > 1:
                url = f"{START_URL}/page={pg}"
                print(f"Loading page {pg}: {url}")
                await page.goto(url, timeout=60_000)
                await page.wait_for_timeout(4_000)

            links = await page.locator('a[href*="/property/"]').all()
            page_urls = set()
            for link in links:
                href = await link.get_attribute("href")
                if href:
                    full = href if href.startswith("http") else BASE_URL + href
                    page_urls.add(full)

            print(f"  Page {pg}: {len(page_urls)} unique URLs")
            all_urls.update(page_urls)

        await browser.close()

    sorted_urls = sorted(all_urls)
    print(f"\nTotal unique listing URLs collected: {len(sorted_urls)}")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["URL"])
        for url in sorted_urls:
            writer.writerow([url])

    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(scrape_urls())
