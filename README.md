# Kriechen
Kriechen exposes two classes:
- [Crawler](./src/crawler.py), a generic task manager that orchestrates the execution of Consumers and Producers that feed back into each others' source queues
- [Spider](./src/spider.py), a class that uses the `Crawler` to create Producers that generate a queue of links to process, and Consumers that download the HTML of these links

## Spider
### Usage
The `Spider` class can be instantiated with just a URL.

```python
from kriechen import Spider

spider = Spider(url="https://www.tagesschau.de", max_links=100)

# Contains `Page` objects w/ Underlying HTML/"Soup"
results = await spider.crawl()

# Extracts Text from Soup
result_text_only = spider.results(extract_text=True)
```
