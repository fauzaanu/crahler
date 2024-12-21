# Crahler

A python script based on crawlee's BS4 scraper to quickly scrape all important
documents on a target website.

## Usage

1. Use UV
2. Run `uv sync`
3. rename or copy the .env.sample file to be .env and replace the url to be the url you want to target
4. Run `uv run hello.py`
5. Files will be downloaded to `downloaded_files`

## Features

- Retries with the www. version
- Keeps a history of previously downloaded files
- All files are in specefic folders based on their document type
