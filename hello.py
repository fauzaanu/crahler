import asyncio
import datetime
import os
from urllib.parse import urljoin, unquote
import aiohttp
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

# Define supported file types
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

async def download_file(url: str, save_path: str) -> None:
    """Download a file from the given URL and save it to the specified path."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    with open(save_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(8192)  # Read in chunks
                            if not chunk:
                                break
                            f.write(chunk)
                    return True
                return False
        except Exception as e:
            print(f"Error downloading {url}: {str(e)}")
            return False

async def main() -> None:
    # Create base directory for downloads
    base_dir = "downloaded_files"
    os.makedirs(base_dir, exist_ok=True)

    # Create subdirectories for each file type
    for subdir in set(DOCUMENT_TYPES.values()):
        os.makedirs(os.path.join(base_dir, subdir), exist_ok=True)

    # Keep track of processed files to avoid duplicates
    processed_files = set()

    crawler = BeautifulSoupCrawler(
        request_handler_timeout=datetime.timedelta(seconds=300),
        retry_on_blocked=True,
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Find all links on the page
        soup = context.soup
        links = soup.find_all(['a', 'img'])  # Include both links and images

        for link in links:
            # Handle both href (for links) and src (for images)
            url = link.get('href') or link.get('src')
            if url:
                # Convert relative URLs to absolute URLs
                absolute_url = urljoin(context.request.url, url)

                # Decode URL-encoded characters in the filename
                decoded_url = unquote(absolute_url)

                # Check file extension
                file_extension = os.path.splitext(decoded_url.lower())[1]

                if file_extension in DOCUMENT_TYPES:
                    if absolute_url not in processed_files:
                        processed_files.add(absolute_url)

                        # Get the appropriate subdirectory
                        subdir = DOCUMENT_TYPES[file_extension]

                        # Extract filename from URL or generate one
                        filename = os.path.basename(decoded_url)
                        if not filename or not os.path.splitext(filename)[1]:
                            filename = f"file_{len(processed_files)}{file_extension}"

                        # Create full save path
                        save_path = os.path.join(base_dir, subdir, filename)

                        try:
                            context.log.info(f'Downloading {file_extension} file: {absolute_url}')
                            success = await download_file(absolute_url, save_path)
                            if success:
                                context.log.info(f'Successfully downloaded: {filename}')
                            else:
                                context.log.error(f'Failed to download: {filename}')
                        except Exception as e:
                            context.log.error(f'Failed to download {absolute_url}: {str(e)}')
                else:
                    # Only enqueue links (not image sources) for crawling
                    if link.name == 'a':
                        await context.enqueue_links(selector=f'a[href="{url}"]')

    await crawler.run(['https://www.hdc.mv'])

if __name__ == '__main__':
    asyncio.run(main())