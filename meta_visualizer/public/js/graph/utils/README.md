# Graph Utilities

This directory contains reusable utility functions supporting various calculations needed for the graph visualization.

## Files

- `geometryUtils.js`: Provides helper functions for common geometric calculations related to node positions, link paths, and distances.

## Responsibilities

- Encapsulating common mathematical or geometric operations used by other graph modules (renderers, layouts, interactions).
- Providing clean interfaces for calculations like finding intersection points, distances, centers, and midpoints.

## Provided Utilities (`geometryUtils.js`)

- `calculateIntersection(node, targetX, targetY)`: Computes the intersection point between the edge of a rectangular node and a line segment extending from the node's center towards a target point (`targetX`, `targetY`). Used for drawing links precisely to the node boundary.
- `calculateDistance(x1, y1, x2, y2)`: Calculates the distance between two points.
- `calculateCenter(node)`: Returns the center coordinates (`{x, y}`) of a given node rectangle.
- `calculateMidpoint(x1, y1, x2, y2)`: Computes the midpoint between two points.
- `calculateLinkPath(sourceNode, targetNode, offset)`: Calculates the SVG path string for a link between two nodes, connecting to their boundaries, and includes an offset path for reverse links.
