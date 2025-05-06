# Graph Interactions

This directory contains modules responsible for handling user interactions with the graph visualization, providing navigation, exploration, and information display capabilities.

## Files

- `dragHandler.js`: Implements node dragging using `d3.drag`. Allows users to manually position nodes, fixing their position (`fx`, `fy`) and optionally restarting the force simulation. Handles exiting tree view mode upon dragging.
- `highlightHandler.js`: Manages visual highlighting of nodes and links. It includes functions to highlight connected nodes/links on hover (over links or nodes) and provides functionality to switch to a hierarchical tree layout (`createTreeLayout`) originating from a clicked node. Includes logic to reset highlighting.
- `zoomHandler.js`: Implements zoom (mouse wheel) and pan (background drag) using `d3.zoom`. Configures zoom behavior (scale extent, disabling double-click zoom), adds UI controls (zoom buttons, instructions), and prevents page scroll.
- `tooltipHandler.js`: Creates and manages a single HTML tooltip element. Shows context-dependent information for nodes or links on hover (triggered by specific elements like tooltip indicators or link paths). Handles tooltip visibility logic, including delayed hiding and preventing closure when hovering over the tooltip itself.

## Responsibilities

- Capturing and interpreting user input events (click, drag, mouseover, mouseout, wheel) on SVG elements (nodes, links, background, UI controls).
- Modifying the graph's visual state in response to interactions (e.g., applying/removing CSS classes for highlighting, updating element positions, changing SVG transform for zoom/pan).
- Interacting with other modules (e.g., restarting the force simulation during drag, triggering tooltip display).
- Managing interaction-specific states (e.g., whether tree view is active, which element is currently hovered for tooltip management).
- Providing UI elements for interaction control (zoom buttons, instructions).

## Interaction Types

- **Drag & Drop:** Users can reposition nodes by dragging them. The node's position becomes fixed after dragging.
- **Highlighting:** Hovering over a node highlights itself and its outgoing neighbors and links. Hovering over a link highlights the link and its connected nodes. Specific styles are applied via CSS classes.
- **Zoom & Pan:** Users can zoom using the mouse wheel or zoom buttons, and pan by dragging the SVG background.
- **Tooltips:** Hovering over specific indicators on nodes or over links displays detailed information in a floating tooltip panel.
- **Tree View:** Clicking a node triggers `highlightHandler.js` to rearrange the graph into a hierarchical tree layout centered on that node (using logic from `layouts/treeLayout.js`). Clicking another node resets and recreates the tree view.
