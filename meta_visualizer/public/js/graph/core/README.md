# Core Graph Components

This directory contains the core modules responsible for loading data, processing it into a graph structure, and orchestrating the creation of the graph visualization.

## Files

- `graphInitializer.js`: Handles the initialization process, including setting up the JSON file upload mechanism, parsing the loaded data, and triggering the graph creation. Displays an initial prompt if no data is loaded.
- `dataProcessor.js`: Processes the raw JSON metadata. It identifies collections (nodes) and their properties, extracts relationships (links) between them (including handling different join types and reverse relationships), and transforms the data into `nodes` and `links` arrays suitable for D3.js.
- `graphCreator.js`: The main orchestrator for the visualization. It takes the processed nodes and links, sets up the SVG canvas, initializes D3 components (force simulation, zoom, drag, tooltips), renders the visual elements (nodes, links, markers), applies styles, and manages user interactions like highlighting and tree view layouts.

## Responsibilities

- Setting up the user interface for loading graph data (JSON file).
- Parsing and validating the loaded JSON data.
- Transforming raw metadata into graph-compatible `nodes` and `links` structures.
- Handling various relationship types and ensuring bidirectionality.
- Orchestrating the D3.js visualization setup, including SVG creation, layout initialization, and component rendering.
- Managing user interactions (zoom, pan, drag, hover, click) and visual feedback (highlighting, tooltips).
- Implementing dynamic layouts like force-directed graphs and hierarchical tree views.

## Data Flow

1. `graphInitializer.js` sets up the UI and waits for a JSON file upload.
2. Upon file load, it reads and parses the JSON data.
3. `graphInitializer.js` calls `createGraph` in `graphCreator.js` with the parsed data.
4. `graphCreator.js` calls `processMetadata` in `dataProcessor.js` to transform the raw data into `nodes` and `links`.
5. `graphCreator.js` uses the processed `nodes` and `links` to build and render the interactive D3.js visualization.
