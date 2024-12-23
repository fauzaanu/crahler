import asyncio
import datetime
import json
import logging
import os
from urllib.parse import urljoin, unquote
from urllib.parse import urlparse, urlunparse

import aiohttp
from crawlee import Glob
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from dotenv import load_dotenv

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOCUMENT_TYPES = {
    # Documents
    '.pdf': 'documents',
    '.doc': 'documents',
    '.docx': 'documents',
    '.txt': 'documents',
    '.rtf': 'documents',
    # Spreadsheets
    '.xls': 'spreadsheets',
    '.xlsx': 'spreadsheets',
    '.csv': 'spreadsheets',
    # Presentations
    '.ppt': 'presentations',
    '.pptx': 'presentations',
    # Images
    '.jpg': 'images',
    '.jpeg': 'images',
    '.png': 'images',
    '.gif': 'images',
    '.bmp': 'images',
    '.webp': 'images',
    # Data formats
    '.json': 'data',
    '.xml': 'data',
    '.yaml': 'data',
    '.yml': 'data'
}

HISTORY_FILE = 'conf/download_history.json'

def load_download_history():
    """Load the download history from file"""
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r') as f:
                return set(json.load(f))
        except:
            return set()
    return set()

def save_download_history(history):
    """Save the download history to file"""
    with open(HISTORY_FILE, 'w') as f:
        json.dump(list(history), f)

def add_www_to_url(url: str) -> str:
    """Add 'www.' to the domain if it's not present."""
    parsed = urlparse(url)
    if not parsed.netloc.startswith('www.'):
        parsed = parsed._replace(netloc=f'www.{parsed.netloc}')
    return urlunparse(parsed)

async def download_file(url: str, save_path: str, context) -> bool:
    """Download a file from the given URL and save it to the specified path."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(save_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)
                    return True
        except Exception as e:
            if 'getaddrinfo failed' in str(e) and not urlparse(url).netloc.startswith('www.'):
                try:
                    www_url = add_www_to_url(url)
                    context.log.info(f'Retrying with www: {www_url}')
                    async with session.get(www_url) as response:
                        if response.status == 200:
                            with open(save_path, 'wb') as f:
                                while True:
                                    chunk = await response.content.read(8192)
                                    if not chunk:
                                        break
                                    f.write(chunk)
                            return True
                except Exception as e2:
                    context.log.error(f"Error downloading (both with and without www): {url}: {str(e2)}")
                    with open("conf/error_links.txt", "a") as error_tracker:
                        error_tracker.write(url+"\n")
                    return False
            else:
                context.log.error(f"Error downloading {url}: {str(e)}")
                with open("conf/error_links.txt", "a") as error_tracker:
                    error_tracker.write(url+"\n")
            return False


def validate_url(url):
    """
    Validates and ensures the input string is a properly formatted HTTP/HTTPS URL.
    If the URL is missing a scheme, it defaults to "http".

    Args:
        url (str): The URL to validate.

    Returns:
        str: A validated and properly formatted URL.

    Raises:
        ValueError: If the URL is invalid.
    """
    if not isinstance(url, str):
        raise ValueError("URL must be a string")

    # Parse the URL
    parsed_url = urlparse(url)

    # If scheme is missing, default to "http"
    if not parsed_url.scheme:
        parsed_url = parsed_url._replace(scheme="http")

    # If netloc is missing but path looks like a domain, adjust accordingly
    if not parsed_url.netloc:
        if '/' in parsed_url.path:
            path_parts = parsed_url.path.split('/', 1)
            parsed_url = parsed_url._replace(netloc=path_parts[0], path='/' + path_parts[1])
        else:
            parsed_url = parsed_url._replace(netloc=parsed_url.path, path="")

    # Ensure the scheme is either HTTP or HTTPS
    if parsed_url.scheme not in ["http", "https"]:
        raise ValueError("URL must start with 'http' or 'https'")

    # Rebuild the URL
    validated_url = urlunparse(parsed_url)

    return validated_url


async def main() -> None:
    base_dir = "downloaded_files"
    os.makedirs(base_dir, exist_ok=True)

    for subdir in set(DOCUMENT_TYPES.values()):
        os.makedirs(os.path.join(base_dir, subdir), exist_ok=True)

    # Load previously downloaded files
    processed_files = load_download_history()
    initial_file_count = len(processed_files)
    logger.info(f"Found {initial_file_count} previously downloaded files")
    from crawlee._types import ConcurrencySettings
    ConSet = ConcurrencySettings(
        max_tasks_per_minute=60
    )

    crawler = BeautifulSoupCrawler(
        request_handler_timeout=datetime.timedelta(seconds=300),
        retry_on_blocked=True,
        concurrency_settings=ConSet
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        soup = context.soup
        links = soup.find_all(['a', 'img'])

        for link in links:
            url = link.get('href') or link.get('src')
            if url:
                absolute_url = urljoin(context.request.url, url)
                decoded_url = unquote(absolute_url)
                file_extension = os.path.splitext(decoded_url.lower())[1]

                if file_extension in DOCUMENT_TYPES:
                    if absolute_url not in processed_files:
                        processed_files.add(absolute_url)

                        subdir = DOCUMENT_TYPES[file_extension]
                        filename = os.path.basename(decoded_url)
                        if not filename or not os.path.splitext(filename)[1]:
                            filename = f"file_{len(processed_files)}{file_extension}"

                        save_path = os.path.join(base_dir, subdir, filename)

                        try:
                            context.log.info(f'Downloading {file_extension} file: {absolute_url}')
                            success = await download_file(absolute_url, save_path, context)
                            if success:
                                context.log.info(f'Successfully downloaded: {filename}')
                                # Save history after each successful download
                                save_download_history(processed_files)
                            else:
                                processed_files.remove(absolute_url)  # Remove from history if download failed
                                context.log.error(f'Failed to download: {filename}')
                        except Exception as e:
                            processed_files.remove(absolute_url)  # Remove from history if download failed
                            context.log.error(f'Failed to download {absolute_url}: {str(e)}')
                else:

                    # dynamically load the link data from a banned.txt file which would have all the GLob links within
                    with open("conf/banned.txt", "r") as banned_links:
                        blinks = banned_links.readlines()

                    banned_links = []

                    for link in blinks:
                        try:
                            url = validate_url(url)
                            banned_links.append(Glob(link.strip()))
                        except ValueError:
                            continue

                    if any(blink.match(url) for blink in banned_links):
                        context.log.info(f'Skipping banned link: {url}')
                        continue


                    if link.name == 'a':
                        await context.enqueue_links(selector=f'a[href="{url}"]', exclude=banned_links)
    load_dotenv()
    initial_url = os.getenv('INITIAL_URL', 'https://example.com')
    await crawler.run([initial_url])

    # Save final history
    save_download_history(processed_files)

    # Print summary using standard logging
    new_downloads = len(processed_files) - initial_file_count
    logger.info("Download session completed:")
    logger.info(f"Previously downloaded files: {initial_file_count}")
    logger.info(f"New files downloaded: {new_downloads}")
    logger.info(f"Total unique files: {len(processed_files)}")

if __name__ == '__main__':
    asyncio.run(main())