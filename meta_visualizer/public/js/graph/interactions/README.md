# Graph Interactions

This directory contains components for handling user interactions with the graph.

## Files

- `dragHandler.js`: Implements dragging functionality for nodes
- `highlightHandler.js`: Handles highlighting of nodes and connections on hover/click
- `zoomHandler.js`: Implements zoom and pan functionality for the graph
- `tooltipHandler.js`: Manages tooltip display on hover over nodes and links

## Responsibilities

- Handling user input events (click, drag, hover, etc.)
- Updating visual elements in response to user interactions
- Managing the interactive state of the visualization
- Providing visual feedback for user actions

## Interaction Types

### Drag Interactions

- Allow users to move nodes around the visualization
- Update the simulation to maintain proper link positions

### Highlighting

- Highlight related nodes and connections on hover/click
- Emphasize important information in the visualization

### Zoom and Pan

- Allow users to navigate large graphs
- Provide controls for zooming in/out and resetting the view

### Tooltips

- Show detailed information about nodes and links
- Provide contextual information on hover
