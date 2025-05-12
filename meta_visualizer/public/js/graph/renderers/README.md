# Graph Renderers

This directory contains modules responsible for creating the visual SVG elements of the graph based on the processed data.

## Files

- `nodeRenderer.js`: Creates the visual representation for each node (collection). It builds an SVG group containing a rectangle, title text, table path text, property/column lists, subcollection list, and a tooltip indicator.
- `linkRenderer.js`: Creates the visual representation for each link (relationship). It builds an SVG group containing a path element (the line connecting nodes) with an appropriate arrowhead marker and a text label displaying the relationship name along the path.
- `markerRenderer.js`: Defines reusable SVG `<marker>` elements (arrowheads) used by `linkRenderer.js` to indicate relationship direction and type (e.g., `simple_join`, `cartesian_product`, `highlighted`).

## Responsibilities

- Translating node data (name, path, columns, subcollections) into structured SVG elements (rectangles, text, lines).
- Translating link data (source, target, name, type) into SVG paths and text labels.
- Defining and applying appropriate markers (arrowheads) to links based on relationship type and interaction state.
- Structuring the SVG output within the main graph group (`<g>`) provided by `graphCreator.js`.
- Setting up basic element attributes (IDs, classes) for styling and interaction handling.

## Rendering Process

1. `markerRenderer.js` (`setupMarkers`) is called first by `graphCreator.js` to define the SVG arrowheads in the `<defs>` section.
2. `nodeRenderer.js` (`createNodes`) is called to generate the SVG groups and internal elements for all nodes.
3. `linkRenderer.js` (`createLinks`) is called to generate the SVG groups, paths, and labels for all links, referencing the pre-defined markers.
