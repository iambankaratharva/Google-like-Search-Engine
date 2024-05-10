# Cloud-based Search Engine

This project integrates various components developed throughout the course to build a complete cloud-based web search engine. The key components include a web server, key-value store, analytics engine, crawler, indexer, and PageRank algorithm.

## Overview

### Components
- **Web Server**: Handles HTTP requests and serves the web frontend.
- **Key-Value Store (KVS)**: Manages data storage and retrieval.
- **Analytics Engine (Flame)**: Processes large datasets and performs parallel computations.
- **Crawler**: Collects web pages from the internet, adhering to polite crawling practices.
- **Indexer**: Builds an inverted index mapping words to URLs.
- **PageRank**: Calculates the importance of web pages based on the link graph.

### Objectives
- **Integrate Components**: Combine all developed components into a cohesive search engine.
- **Web Frontend**: Develop a user interface for querying and displaying search results.
- **Robustness**: Enhance the system to handle real-world web data and large-scale operations.
- **Deployment**: Deploy the search engine on Amazon EC2.

### Functionality
- **Search Engine**: Ensure the search engine can accept queries, retrieve results, and rank them effectively.
- **Crawler**: Handle real-world web data, manage crashes, and filter out irrelevant content.
- **Indexer and PageRank**: Process and rank web data efficiently using KVS and Flame workers.

### Enhancements
- **Features**: Implement additional features such as blacklists for the crawler, phrase searches for the indexer, or diagnostic tools for the ranker.
