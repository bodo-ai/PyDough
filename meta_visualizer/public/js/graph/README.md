# Graph Visualization Component

This directory contains the code for the interactive graph visualization component used in MetaViewer.

## Directory Structure

- **core/**: Core functionality for initializing and creating the graph

  - `graphInitializer.js`: Entry point for graph initialization
  - `dataProcessor.js`: Processes the metadata into nodes and links
  - `graphCreator.js`: Main function to create the graph visualization

- **renderers/**: Components responsible for rendering visual elements

  - `nodeRenderer.js`: Renders node elements (collection boxes)
  - `linkRenderer.js`: Renders links between nodes
  - `markerRenderer.js`: Renders arrow markers for links

- **layouts/**: Different layout strategies for positioning nodes

  - `forceLayout.js`: Force-directed graph layout
  - `treeLayout.js`: Tree visualization layout

- **interactions/**: User interaction handling

  - `dragHandler.js`: Drag functionality for nodes
  - `highlightHandler.js`: Highlighting nodes and connections
  - `zoomHandler.js`: Zoom and pan functionality
  - `tooltipHandler.js`: Tooltip display functionality

- **styles/**: Visual styling for the graph

  - `graphStyles.js`: CSS styles for the graph components

- **utils/**: Utility functions
  - `geometryUtils.js`: Utility functions for geometric calculations

## Usage

The graph visualization is initialized when the DOM is loaded:

```javascript
import { initGraphVisualization } from "./graph/core/graphInitializer.js";

document.addEventListener("DOMContentLoaded", () => {
  initGraphVisualization();
});
```

The component reads data from `/data/metadata.json` and creates an interactive visualization of the relationships between collections.
