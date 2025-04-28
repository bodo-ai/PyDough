# MetaViewer - Graph Visualization Tool

MetaViewer is a web-based graph visualization tool designed to display and explore relationships between collections in a metadata schema. It provides an interactive interface for navigating complex data relationships and understanding the structure of the data model.

## Features

- **Interactive Graph Visualization**: View collections and their relationships in an interactive force-directed graph
- **Tree View**: Click on any node to see a hierarchical tree view of its connections
- **Relationship Details**: Hover over connections to see detailed information about the relationship
- **Zoom and Pan**: Navigate large graphs with zoom and pan controls
- **Highlighting**: Highlight connections and related nodes on hover
- **Tooltips**: Get detailed information about nodes and links via tooltips

## Project Structure

- **public/**: Contains all the front-end code
  - **css/**: CSS stylesheets
  - **data/**: JSON metadata files
  - **js/**: JavaScript code
    - **graph/**: Modular graph visualization components (see below)
  - **index.html**: Main HTML file

### Graph Visualization Components

The graph visualization is implemented as a modular component with the following structure:

- **core/**: Core functionality for initializing and creating the graph
- **renderers/**: Components responsible for rendering visual elements
- **layouts/**: Different layout strategies for positioning nodes
- **interactions/**: User interaction handling
- **styles/**: Visual styling for the graph
- **utils/**: Utility functions

For detailed information about each component, see the README.md files in the respective directories.

## Getting Started

1. Clone the repository
2. Serve the `public` directory with a web server
3. Access the application in your browser

Example using Python's built-in HTTP server:

```bash
cd public
python -m http.server 8000
```

Then visit http://localhost:8000 in your browser.

## Data Format

The visualization expects a JSON file located at `/data/metadata.json` with the following structure:

```json
{
  "graph_name": {
    "collection_name": {
      "properties": {
        "property_name": {
          "type": "table_column",
          "column_name": "column_name",
          "data_type": "data_type"
        },
        "relationship_name": {
          "type": "simple_join|compound|cartesian_product",
          "other_collection_name": "target_collection",
          "singular": true|false,
          "no_collisions": true|false,
          "reverse_relationship_name": "reverse_name"
        }
      },
      "table_path": "path/to/table",
      "unique_properties": ["id", "name"]
    }
  }
}
```

## Dependencies

- D3.js v7.x - For data visualization
