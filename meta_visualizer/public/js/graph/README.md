# Graph Visualization Component

This directory contains the JavaScript modules responsible for creating and managing the interactive graph visualization component of MetaViewer. It leverages D3.js for rendering and interaction.

## Directory Structure

- **`core/`**: Handles the overall initialization, data processing, and orchestration of the graph creation.

  - `graphInitializer.js`: Sets up the UI (file load button), handles JSON file input, parses data, and calls `graphCreator`.
  - `dataProcessor.js`: Transforms the raw JSON metadata into `nodes` and `links` arrays suitable for D3, processing collections, properties, and relationships (including reverse links).
  - `graphCreator.js`: The main orchestrator that sets up the SVG canvas, calls renderers, initializes layouts and interactions, and manages the main D3 simulation loop.

- **`renderers/`**: Modules focused on creating the visual SVG elements.

  - `nodeRenderer.js`: Renders nodes as SVG groups (rect, text for title/path/columns/subcollections, tooltip indicator).
  - `linkRenderer.js`: Renders links as SVG groups (path with markers, text label).
  - `markerRenderer.js`: Defines SVG arrowheads (`<marker>`) for different link types and states.

- **`layouts/`**: Algorithms for positioning nodes and links.

  - `forceLayout.js`: Implements the default force-directed layout using D3 simulation forces.
  - `treeLayout.js`: Implements a hierarchical tree layout triggered by node clicks, arranging nodes based on outgoing links from a root.

- **`interactions/`**: Modules handling user input and visual feedback.

  - `dragHandler.js`: Enables node dragging and updates fixed positions (`fx`, `fy`).
  - `highlightHandler.js`: Manages highlighting of related nodes/links on hover and click, triggers tree layout.
  - `zoomHandler.js`: Implements zoom/pan functionality with UI controls.
  - `tooltipHandler.js`: Manages the display of informative tooltips on hover.

- **`styles/`**: Styling definitions for the graph.

  - `graphStyles.js`: Injects CSS rules to style nodes, links, tooltips, and interaction states (highlighted, faded, etc.).

- **`utils/`**: Reusable utility functions.
  - `geometryUtils.js`: Provides geometric calculation helpers (intersections, distances, centers).

## Initialization and Data Flow

The visualization is bootstrapped by `main.js`, which calls `initGraphVisualization` from `core/graphInitializer.js`.

1.  `graphInitializer.js` sets up the "Load JSON File" button and waits for user input.
2.  When a user selects a valid JSON file, it's read and parsed.
3.  The parsed data object is passed to `createGraph` in `core/graphCreator.js`.
4.  `graphCreator.js` calls `processMetadata` (from `core/dataProcessor.js`) to get `nodes` and `links`.
5.  `graphCreator.js` then initializes the D3 simulation (`layouts/forceLayout.js`), sets up SVG markers (`renderers/markerRenderer.js`), renders nodes (`renderers/nodeRenderer.js`) and links (`renderers/linkRenderer.js`), applies styles (`styles/graphStyles.js`), and sets up interactions (`interactions/` handlers).
6.  The force simulation automatically positions nodes, and interaction handlers provide zoom, pan, drag, highlight, and tooltip features.
