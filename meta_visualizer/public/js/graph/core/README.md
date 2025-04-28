# Core Graph Components

This directory contains the core functionality for initializing and creating the graph visualization.

## Files

- `graphInitializer.js`: Entry point that fetches metadata and initializes the graph
- `dataProcessor.js`: Processes metadata into nodes and links, handling relationships
- `graphCreator.js`: Main function that orchestrates the creation of the graph visualization

## Responsibilities

- Fetching and parsing metadata
- Processing metadata into graph data structures (nodes and links)
- Orchestrating the creation and linking of all graph components
- Handling initialization and setup of the SVG canvas

## Data Flow

1. `graphInitializer.js` fetches the metadata
2. Data is processed by `dataProcessor.js` into nodes and links
3. `graphCreator.js` uses this processed data to create the visualization
