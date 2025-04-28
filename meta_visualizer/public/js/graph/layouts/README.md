# Graph Layouts

This directory contains different layout strategies for positioning nodes in the graph.

## Files

- `forceLayout.js`: Implements a force-directed graph layout using D3's force simulation
- `treeLayout.js`: Implements a hierarchical tree visualization layout

## Responsibilities

- Determining the positions of nodes in the visualization
- Defining how nodes relate to each other spatially
- Handling the arrangement of nodes in different visualization modes
- Managing transitions between different layouts

## Layout Types

### Force Layout

- Uses physics-based simulation to position nodes
- Nodes repel each other while links act as springs
- Allows for natural, force-balanced arrangements

### Tree Layout

- Arranges nodes in a hierarchical structure
- Positions nodes based on their relationships (parent/child)
- Optimized for visualizing hierarchical data
