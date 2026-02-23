import asyncio
import json
import re
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin, urlparse

from apify import Actor
from bs4 import BeautifulSoup
from curl_cffi.requests import Session

SITE_HOST = 'www.goldsilver.com'
SITE_HOSTS = {'goldsilver.com', 'www.goldsilver.com'}
BASE_URL = 'https://www.goldsilver.com'
# GoldSilver.com has a small catalog — category pages list all products
CATEGORY_URLS = {
    'silver': f'{BASE_URL}/buy-online/silver',
    'gold': f'{BASE_URL}/buy-online/gold',
    'platinum': f'{BASE_URL}/buy-online/platinum',
    'palladium': f'{BASE_URL}/buy-online/palladium',
}
SEARCH_URL_TEMPLATE = 'https://www.goldsilver.com/?s={query}&post_type=product'
AVAILABILITY_STATES = ['In Stock', 'Out of Stock', 'Pre-Order', 'Sold Out', 'Coming Soon', 'Discontinued']
MAX_DESCRIPTION_LENGTH = 2000
SKIP_PATH_SEGMENTS = ['/about', '/contact', '/faq', '/help', '/blog', '/my-account', '/cart', '/checkout', '/shipping', '/privacy', '/terms', '/info', '/guide']

products_scraped = 0
scraped_urls: set[str] = set()


def parse_price(price_str: str) -> float | None:
    if not price_str:
        return None
    match = re.search(r'\$?([\d,]+\.?\d*)', price_str)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            return None
    return None


def validate_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ''
        return parsed.scheme in ('http', 'https') and host in SITE_HOSTS
    except Exception:
        return False


def is_search_url(url: str) -> bool:
    return '?s=' in url or '&s=' in url or '/search' in url


def is_product_url(url: str) -> bool:
    if not validate_url(url):
        return False
    if is_search_url(url):
        return False
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    if not path:
        return False
    if any(skip.strip('/') in path for skip in SKIP_PATH_SEGMENTS):
        return False
    # GoldSilver products: /buy-online/{metal}/{type}/{slug}/
    segments = [s for s in path.split('/') if s]
    if len(segments) >= 3 and segments[0] == 'buy-online':
        return True
    return False


def is_category_url(url: str) -> bool:
    if is_search_url(url):
        return False
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    if not path:
        return True
    if path.startswith('buy-online'):
        segments = [s for s in path.split('/') if s]
        # /buy-online or /buy-online/silver or /buy-online/silver/silver-coins = category
        if len(segments) <= 3:
            if not is_product_url(f"{BASE_URL}/{path}"):
                return True
    return False


def extract_listing_products(html: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, 'html.parser')
    products = []
    seen = set()

    for item in soup.select('li.product, .product.type-product, .products .product'):
        link_el = item.select_one('a[href*="/buy-online/"]')
        if not link_el:
            link_el = item.select_one('a[href]')
        if not link_el:
            continue

        url = urljoin(base_url, link_el.get('href', ''))
        if url in seen or not validate_url(url):
            continue
        seen.add(url)

        name_el = item.select_one('.woocommerce-loop-product__title, .product__title, h2, h3')
        name = name_el.get_text(strip=True) if name_el else link_el.get_text(strip=True)

        # Price — GoldSilver uses "As low as" pricing
        price_el = item.select_one('.product-aslowas-field, .woocommerce-Price-amount, .price .amount, .price')
        price_text = None
        if price_el:
            price_text = price_el.get_text(strip=True)
            if price_text and price_text.count('$') > 1:
                prices = re.findall(r'\$[\d,]+\.?\d*', price_text)
                price_text = prices[0] if prices else price_text

        img_el = item.select_one('img')
        image = None
        if img_el:
            image = img_el.get('src') or img_el.get('data-src') or img_el.get('data-lazy-src')

        if name and len(name) > 3:
            products.append({'url': url, 'name': name, 'price': price_text, 'image': image})

    return products


def extract_product_details(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')

    h1 = soup.select_one('h1.product_title, h1.entry-title, h1')
    name = h1.get_text(strip=True) if h1 else None

    price_text = None
    price_numeric = None

    # Try JSON-LD
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or '')
            if isinstance(data, list):
                data = data[0]
            if data.get('@type') == 'Product':
                offers = data.get('offers', {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                p = offers.get('price')
                if p:
                    price_numeric = float(p)
                    price_text = f"${price_numeric:,.2f}"
        except (json.JSONDecodeError, ValueError, TypeError):
            pass

    if not price_text:
        meta_price = soup.select_one('meta[itemprop="price"], meta[property="product:price:amount"]')
        if meta_price:
            content = meta_price.get('content', '')
            if content:
                try:
                    price_numeric = float(content)
                    price_text = f"${price_numeric:,.2f}"
                except ValueError:
                    pass

    if not price_text:
        price_el = soup.select_one(
            '.product-aslowas-field, .woocommerce-Price-amount, '
            '[itemprop="price"], .summary .price'
        )
        if price_el:
            content = price_el.get('content')
            if content:
                try:
                    price_numeric = float(content)
                    price_text = f"${price_numeric:,.2f}"
                except ValueError:
                    pass
            if not price_text:
                price_text = price_el.get_text(strip=True)
                if price_text and price_text.count('$') > 1:
                    prices = re.findall(r'\$[\d,]+\.?\d*', price_text)
                    price_text = prices[0] if prices else price_text
                price_numeric = parse_price(price_text)

    og_image = soup.select_one('meta[property="og:image"]')
    image_url = og_image.get('content') if og_image else None
    if not image_url:
        img_el = soup.select_one('.woocommerce-product-gallery img, img.wp-post-image')
        image_url = img_el.get('src') if img_el else None

    sku = None
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or '')
            if isinstance(data, list):
                data = data[0]
            if data.get('@type') == 'Product':
                sku = data.get('sku')
                if sku:
                    break
        except (json.JSONDecodeError, ValueError):
            pass
    if not sku:
        sku_el = soup.select_one('[itemprop="sku"], .sku')
        if sku_el:
            sku = sku_el.get('content') or sku_el.get_text(strip=True)

    availability = "Unknown"
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or '')
            if isinstance(data, list):
                data = data[0]
            if data.get('@type') == 'Product':
                offers = data.get('offers', {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                avail = offers.get('availability', '')
                if 'InStock' in avail:
                    availability = "In Stock"
                elif 'OutOfStock' in avail:
                    availability = "Out of Stock"
                elif 'PreOrder' in avail:
                    availability = "Pre-Order"
        except (json.JSONDecodeError, ValueError):
            pass

    if availability == "Unknown":
        page_text = soup.get_text()
        for state in AVAILABILITY_STATES:
            if state in page_text:
                availability = state
                break

    desc_el = soup.select_one(
        '.woocommerce-product-details__short-description, '
        '[itemprop="description"], .product-short-description'
    )
    description = desc_el.get_text(strip=True)[:MAX_DESCRIPTION_LENGTH] if desc_el else None

    return {
        'name': name,
        'price': price_text if price_text and '$' in str(price_text) else None,
        'priceNumeric': price_numeric if price_numeric else parse_price(price_text) if price_text else None,
        'imageUrl': image_url,
        'sku': sku,
        'availability': availability,
        'description': description,
    }


def init_session(proxies: dict) -> Session:
    http = Session(impersonate="chrome110")
    home_resp = http.get(f"{BASE_URL}/", proxies=proxies, timeout=30)
    Actor.log.info(f"Homepage warm-up: status={home_resp.status_code}, cookies={len(http.cookies)}")
    if home_resp.status_code != 200:
        Actor.log.warning(f"Homepage returned {home_resp.status_code}, scraping may fail")
    http.headers.update({'Referer': f'{BASE_URL}/'})
    return http


async def scrape_listing(http: Session, url: str, proxies: dict, max_items: int) -> None:
    global products_scraped

    Actor.log.info(f"Fetching listing: {url}")
    try:
        response = http.get(url, proxies=proxies, timeout=30)
    except Exception as e:
        Actor.log.error(f"Failed to fetch listing {url}: {e}")
        return

    if response.status_code != 200:
        Actor.log.warning(f"Non-200 status ({response.status_code}) for listing {url}")
        return

    products = extract_listing_products(response.text, url)
    Actor.log.info(f"Found {len(products)} products on listing page")

    for product in products:
        if products_scraped >= max_items:
            break

        prod_url = product['url'].rstrip('/')
        if not is_product_url(prod_url):
            continue
        if prod_url in scraped_urls:
            continue
        scraped_urls.add(prod_url)

        try:
            prod_resp = http.get(prod_url, proxies=proxies, timeout=30)
            if prod_resp.status_code == 200:
                details = extract_product_details(prod_resp.text)
                await Actor.push_data({
                    'url': prod_url,
                    'name': details['name'] or product.get('name', ''),
                    'price': details['price'] or product.get('price'),
                    'priceNumeric': details['priceNumeric'] or parse_price(product.get('price')),
                    'imageUrl': details['imageUrl'] or product.get('image'),
                    'sku': details['sku'],
                    'availability': details['availability'],
                    'description': details['description'],
                    'scrapedAt': datetime.now(timezone.utc).isoformat(),
                })
            else:
                Actor.log.warning(f"Product page {prod_url} returned {prod_resp.status_code}")
                await Actor.push_data({
                    'url': prod_url,
                    'name': product.get('name', ''),
                    'price': product.get('price'),
                    'priceNumeric': parse_price(product.get('price')),
                    'imageUrl': product.get('image'),
                    'sku': None,
                    'availability': 'Unknown',
                    'description': None,
                    'scrapedAt': datetime.now(timezone.utc).isoformat(),
                })
        except Exception as e:
            Actor.log.warning(f"Failed to fetch product {prod_url}: {e}")

        products_scraped += 1
        Actor.log.info(f"Scraped {products_scraped}/{max_items} products")


async def scrape_search(http: Session, query: str, proxies: dict, max_items: int) -> None:
    """Search by browsing category pages and filtering by keywords."""
    global products_scraped
    query_lower = query.lower()

    # Determine which categories to search
    categories_to_search = []
    if any(w in query_lower for w in ('silver', 'coin', 'eagle', 'maple', 'round', 'bar')):
        categories_to_search.append('silver')
    if any(w in query_lower for w in ('gold',)):
        categories_to_search.append('gold')
    if any(w in query_lower for w in ('platinum',)):
        categories_to_search.append('platinum')
    if any(w in query_lower for w in ('palladium',)):
        categories_to_search.append('palladium')
    if not categories_to_search:
        categories_to_search = ['silver']

    for metal in categories_to_search:
        if products_scraped >= max_items:
            break

        cat_url = CATEGORY_URLS.get(metal)
        if not cat_url:
            continue

        Actor.log.info(f"Searching '{query}' in {metal} category")
        try:
            response = http.get(cat_url, proxies=proxies, timeout=30)
        except Exception as e:
            Actor.log.error(f"Failed to fetch category {cat_url}: {e}")
            continue

        if response.status_code != 200:
            Actor.log.warning(f"Category {cat_url} returned {response.status_code}")
            continue

        all_products = extract_listing_products(response.text, cat_url)

        # Filter by search keywords
        keywords = [w for w in query_lower.split() if len(w) > 2]
        matched = []
        for p in all_products:
            name_lower = p['name'].lower()
            if any(kw in name_lower for kw in keywords):
                matched.append(p)

        if not matched:
            matched = all_products

        Actor.log.info(f"Found {len(matched)} products matching '{query}' in {metal}")

        for product in matched:
            if products_scraped >= max_items:
                break

            prod_url = product['url'].rstrip('/')
            if not is_product_url(prod_url):
                continue
            if prod_url in scraped_urls:
                continue
            scraped_urls.add(prod_url)

            try:
                prod_resp = http.get(prod_url, proxies=proxies, timeout=30)
                if prod_resp.status_code == 200:
                    details = extract_product_details(prod_resp.text)
                    await Actor.push_data({
                        'url': prod_url,
                        'name': details['name'] or product.get('name', ''),
                        'price': details['price'] or product.get('price'),
                        'priceNumeric': details['priceNumeric'] or parse_price(product.get('price')),
                        'imageUrl': details['imageUrl'] or product.get('image'),
                        'sku': details['sku'],
                        'availability': details['availability'],
                        'description': details['description'],
                        'scrapedAt': datetime.now(timezone.utc).isoformat(),
                    })
                else:
                    await Actor.push_data({
                        'url': prod_url,
                        'name': product.get('name', ''),
                        'price': product.get('price'),
                        'priceNumeric': parse_price(product.get('price')),
                        'imageUrl': product.get('image'),
                        'sku': None,
                        'availability': 'Unknown',
                        'description': None,
                        'scrapedAt': datetime.now(timezone.utc).isoformat(),
                    })
            except Exception as e:
                Actor.log.warning(f"Failed to fetch product {prod_url}: {e}")

            products_scraped += 1
            Actor.log.info(f"Scraped {products_scraped}/{max_items} products")


async def scrape_product(http: Session, url: str, proxies: dict, max_items: int) -> None:
    global products_scraped
    if products_scraped >= max_items:
        return

    url = url.rstrip('/')
    if url in scraped_urls:
        return
    scraped_urls.add(url)

    Actor.log.info(f"Fetching product ({products_scraped + 1}/{max_items}): {url}")
    try:
        response = http.get(url, proxies=proxies, timeout=30)
    except Exception as e:
        Actor.log.error(f"Failed to fetch product {url}: {e}")
        return

    if response.status_code != 200:
        Actor.log.warning(f"Non-200 status ({response.status_code}) for product {url}")
        return

    details = extract_product_details(response.text)
    await Actor.push_data({
        'url': url,
        'name': details['name'],
        'price': details['price'],
        'priceNumeric': details['priceNumeric'],
        'imageUrl': details['imageUrl'],
        'sku': details['sku'],
        'availability': details['availability'],
        'description': details['description'],
        'scrapedAt': datetime.now(timezone.utc).isoformat(),
    })

    products_scraped += 1
    Actor.log.info(f"Scraped {products_scraped}/{max_items} products")


async def main():
    global products_scraped

    async with Actor:
        actor_input = await Actor.get_input() or {}
        start_urls_input = actor_input.get("start_urls", [])
        search_terms = actor_input.get("search_terms", [])
        max_items = actor_input.get("max_items", 10)

        search_queries = []
        start_urls = []
        for term in search_terms:
            term = term.strip()
            if term:
                search_queries.append(term)
                Actor.log.info(f"Added search term: '{term}'")

        for item in start_urls_input:
            if isinstance(item, dict) and "url" in item:
                url = item["url"]
            elif isinstance(item, str):
                url = item
            else:
                continue
            if validate_url(url):
                start_urls.append(url)
            else:
                Actor.log.warning(f"Skipping non-GoldSilver URL: {url}")

        if not search_queries and not start_urls:
            default_term = "Silver coin"
            search_queries = [default_term]
            Actor.log.info(f"No input provided, defaulting to search: '{default_term}'")

        Actor.log.info(f"Starting GoldSilver.com Scraper with {len(search_queries)} search queries, {len(start_urls)} start URLs, max_items={max_items}")

        Actor.log.info("Configuring RESIDENTIAL proxy with US country")
        proxy_configuration = await Actor.create_proxy_configuration(
            actor_proxy_input={
                'useApifyProxy': True,
                'apifyProxyGroups': ['RESIDENTIAL'],
                'apifyProxyCountry': 'US',
            },
        )

        proxy_url = await proxy_configuration.new_url()
        proxies = {"http": proxy_url, "https": proxy_url}

        http = init_session(proxies)

        for query in search_queries:
            if products_scraped >= max_items:
                break
            await scrape_search(http, query, proxies, max_items)

        for url in start_urls:
            if products_scraped >= max_items:
                break
            if is_search_url(url):
                await scrape_listing(http, url, proxies, max_items)
            elif is_category_url(url):
                await scrape_listing(http, url, proxies, max_items)
            elif is_product_url(url):
                await scrape_product(http, url, proxies, max_items)
            else:
                Actor.log.warning(f"Could not classify URL, trying as listing: {url}")
                await scrape_listing(http, url, proxies, max_items)

        Actor.log.info(f'Scraping completed. Total products scraped: {products_scraped}')


if __name__ == "__main__":
    asyncio.run(main())
