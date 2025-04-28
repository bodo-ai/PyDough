# Graph Renderers

This directory contains components responsible for rendering the visual elements of the graph.

## Files

- `nodeRenderer.js`: Renders node elements (collection boxes) with titles, properties, and subcollections
- `linkRenderer.js`: Renders links between nodes, including path calculation and labels
- `markerRenderer.js`: Renders arrow markers for different link types

## Responsibilities

- Creating and styling visual elements for nodes and links
- Defining the appearance of different node and link types
- Calculating positions for SVG elements
- Setting up visual attributes for elements

## Rendering Process

1. Marker definitions are created to style the arrows on links
2. Nodes are rendered with their internal structure
3. Links are rendered between nodes with appropriate markers and labels
