# Graph Styles

This directory contains the styling definitions for the graph visualization.

## Files

- `graphStyles.js`: Defines and injects CSS rules for all visual components of the graph, including nodes, links, markers, tooltips, and interaction states.

## Responsibilities

- Defining the visual appearance of nodes (rectangles, text, titles, properties sections) in various states (default, highlighted, primary, faded).
- Styling links based on type (forward, reverse) and state (default, highlighted), including line style, color, thickness, and labels.
- Styling arrow markers for links.
- Defining the appearance and layout of tooltips.
- Using CSS transitions for smooth visual feedback during interactions.
- Ensuring a consistent and readable visual language for the graph.

## Key Styling Areas

- **Nodes:** Base appearance, text formatting (title, headers, items), state-based highlighting (stroke, fill, font size changes).
- **Links:** Distinguishing forward (solid blue) and reverse (dashed orange) relationships, highlighting (thicker green), label appearance and positioning.
- **Tooltips:** Background, border, padding, font sizes, layout for titles and content sections, hints.
- **Interaction States:** Styles for `.node-highlighted`, `.node-primary`, `.node-faded`, `.link-highlighted`, `.arrowhead-highlighted`, `.link-label-highlighted`.
- **Transitions:** Smooth visual changes for properties like stroke, fill, opacity, font-size, etc.
