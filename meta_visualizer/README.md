# MetaViewer - Graph Visualization Tool

MetaViewer is a web-based graph visualization tool designed to display and explore relationships between collections defined in a user-provided JSON metadata schema. It provides an interactive interface using D3.js for navigating complex data relationships.

## Getting Started

### For Users

1.  Clone the repository.
2.  Install dependencies:
    ```bash
    npm install
    ```
3.  Start the server:
    ```bash
    ./serve.sh
    ```
4.  Visit `http://localhost:8000` (or the port specified in `serve.sh`) in your browser.
5.  Click the "Load JSON File" button and select your first metadata JSON file. The graph will be displayed. The metada JSON file should adhere to the schema defined in [metadata](https://github.com/bodo-ai/PyDough/blob/main/documentation/metadata.md).
6.  To load more graphs, click "Load JSON File" again and select another file.
7.  To switch between loaded graphs, click the hamburger icon (☰) in the header and select the desired graph filename from the dropdown.

### For Developers

1.  Clone the repository.
2.  Install dependencies:
    ```bash
    npm install
    ```
3.  Start the development server (which typically includes live reloading):
    ```bash
    npm start
    ```
4.  Visit `http://localhost:3000` (or the port configured for the development server) in your browser.
5.  The application will load. Use the "Load JSON File" button as described in the user section.

## Features

- **Load Multiple Graphs**: Upload multiple JSON files containing metadata schemas. Each file is treated as a separate graph.
- **Switch Between Graphs**: Use the hamburger menu (☰) in the top-left corner to select and view any of the loaded graphs.
- **Interactive Graph Visualization**: View collections and their relationships in a force-directed graph for the currently selected graph.
- **Tree View**: Click on any node to see a hierarchical tree view of its outgoing connections.
- **Relationship Details**: Hover over connections or nodes to see detailed information.
- **Zoom and Pan**: Navigate large graphs with zoom (mouse wheel, buttons) and pan (drag background) controls.
- **Highlighting**: Hovering highlights connected nodes and links.
- **Tooltips**: Detailed information about nodes and links appears on hover over indicators/links.
- **Node Dragging**: Manually reposition nodes.

## Visual Elements

The graph visualization displays the following information:

- **Nodes (Collections)**:

  - Each collection is represented as a rectangular box.
  - **Title**: The collection name is shown at the top.
  - **Table Path**: The source `table_path` is displayed below the title.
  - **Properties (Columns)**: Attributes with `type: "table_column"` are listed under "Properties:".
  - **Relationships (Subcollections)**: Outgoing relationships (like `simple_join`, `cartesian_product`) are listed under "Subcollections:".
  - **Tooltip Indicator**: A small circle in the bottom-right corner can be hovered over to show a detailed tooltip with all properties.

- **Links (Relationships)**:

  - Each defined relationship between collections is drawn as a line connecting the corresponding nodes.
  - **Direction & Type**: An arrowhead indicates the direction and type of relationship (different styles for simple join vs. cartesian product).
  - **Label**: The subcollection access name is displayed as a label along the link path.
  - **Tooltip**: Hovering directly over the link line shows a tooltip with relationship details.

- **Interactions & Highlighting**:
  - **Hover**: Hovering over a node highlights the node, its direct links, and connected nodes. Hovering over a link highlights the link and its source/target nodes.
  - **Click (Tree View)**: Clicking a node transitions the layout to a hierarchical tree view, showing only the clicked node (as root) and nodes reachable via outgoing links. Dragging any node again returns to the force layout.
  - **Zoom/Pan**: Standard mouse wheel/trackpad controls for zooming, and dragging the background for panning. Buttons are also available.

## Project Structure

- **`public/`**: Contains all the front-end code and assets.
  - **`css/`**: CSS stylesheets.
  - **`js/`**: JavaScript code.
    - **`graph/`**: Graph visualization components (see below).
    - **`main.js`**: Entry point that initializes the graph visualization.
  - **`index.html`**: Main HTML file.

### Graph Visualization Components (`public/js/graph/`)

The graph visualization is implemented as a modular component:

- **`core/`**: Handles initialization, data processing (JSON to nodes/links), and orchestration (`graphCreator`).
- **`renderers/`**: Creates SVG elements for nodes, links, and arrow markers.
- **`layouts/`**: Implements node positioning algorithms (force-directed, hierarchical tree).
- **`interactions/`**: Manages user input (zoom, pan, drag, hover, click) and feedback (highlighting, tooltips, tree view activation).
- **`styles/`**: Defines CSS styles injected for visual appearance.
- **`utils/`**: Provides utility functions (e.g., geometry calculations).

(See the README.md files in the respective subdirectories for more details.)

## Dependencies

- D3.js v7.x - Used for DOM manipulation, SVG rendering, force simulation, zoom/drag behaviors, and transitions.
