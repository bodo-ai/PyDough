# Graph Layouts

This directory contains modules that define strategies for positioning nodes and links within the visualization area.

## Files

- `forceLayout.js`: Configures and initializes the default D3.js force-directed simulation. This layout uses physics-based forces (link attraction, node repulsion, collision avoidance, centering) to automatically arrange the graph.
- `treeLayout.js`: Implements a hierarchical tree layout algorithm. When invoked, it arranges a subset of the graph (starting from a selected root node and following outgoing links) into distinct levels, setting fixed positions (`fx`, `fy`) for nodes in the tree.

## Responsibilities

- Calculating and assigning `x`, `y` coordinates (or `fx`, `fy` for fixed positions) to each node.
- Defining the spatial arrangement of the graph based on either physics simulation or hierarchical structure.
- Interacting with the D3 simulation (e.g., initializing forces, restarting the simulation).
- Providing functions to switch between layout modes (e.g., triggering the tree layout calculation).

## Layout Types

### Force Layout (`forceLayout.js`)

- **Mechanism:** Uses D3's `forceSimulation` with link, charge, center, and collision forces.
- **Behavior:** Nodes push apart, links pull together, nodes avoid overlapping, and the whole graph tends towards the center.
- **Use Case:** Default view for exploring the overall network structure.
- **Activation:** Initialized by `graphCreator.js`.

### Tree Layout (`treeLayout.js`)

- **Mechanism:** Custom BFS algorithm calculates levels and horizontal positions based on outgoing relationships from a root node. Sets `fx`/`fy` coordinates to fix nodes in a tree structure.
- **Behavior:** Organizes nodes into clear parent-child levels. Nodes not part of the selected hierarchy are moved off-screen. Uses fixed positioning, overriding the force simulation for tree nodes.
- **Use Case:** Visualizing the hierarchy or dependencies originating from a specific node.
- **Activation:** Triggered by `highlightHandler.js` (`createTreeLayout` function) upon node click.
