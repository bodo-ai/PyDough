// Tree Layout - Implements a hierarchical tree visualization layout
// This layout organizes nodes in a parent-child hierarchy with multiple levels

/**
 * Calculates and applies a hierarchical tree layout starting from a given root node.
 *
 * This function performs the following steps:
 * 1. Resets any existing tree layout properties on all nodes.
 * 2. Performs a Breadth-First Search (BFS) starting from the `rootNode`,
 *    following only outgoing links to discover reachable nodes.
 * 3. Assigns `treeLevel` (depth) and `treeParent` (ID of parent) to discovered nodes.
 * 4. Calculates a relative horizontal `treePosition` for nodes within each level to spread them out.
 * 5. Sets fixed positions (`fx`, `fy`) for all nodes:
 *    - Nodes in the tree are positioned according to their level (vertical) and position (horizontal).
 *    - Nodes *not* in the tree are moved far off-screen by setting high `fx`/`fy` values.
 * 6. Calls `highlightTreeView` to apply specific visual styles to tree nodes/links.
 * 7. Restarts the D3 force simulation so it respects the new `fx`/`fy` values.
 * 8. Calls `fitTreeToView` to adjust the SVG zoom/pan to focus on the generated tree.
 *
 * Note: This function modifies the `nodes` array in-place, adding/updating `treeLevel`,
 * `treePosition`, `treeParent`, `fx`, and `fy` properties on the node objects.
 *
 * @param {Object} rootNode - The node data object to use as the root of the tree.
 * @param {Array} nodes - The complete array of node data objects.
 * @param {Array} links - The complete array of link data objects.
 * @param {Object} node - The D3 selection of node elements (<g class="node">).
 * @param {Object} link - The D3 selection of link elements (<g class="link-group">).
 * @param {Object} simulation - The D3 force simulation instance.
 * @param {Object} svg - The main SVG element D3 selection.
 * @param {number} width - The width of the SVG canvas.
 * @param {number} height - The height of the SVG canvas.
 */
export function createTreeLayout(
  rootNode,
  nodes,
  links,
  node,
  link,
  simulation,
  svg,
  width,
  height
) {
  // Reset all tree positions
  nodes.forEach((n) => {
    n.treePosition = null; // Horizontal position within level
    n.treeLevel = null; // Vertical level in hierarchy (0 = root)
    n.treeParent = null; // ID of parent node
  });

  // Set root node
  const root = nodes.find((n) => n.id === rootNode.id);
  root.treeLevel = 0;
  root.treePosition = 0;

  // BFS to establish tree relationships - only for outgoing edges
  // Breadth-First Search ensures we process level by level
  const queue = [{ id: root.id, level: 0 }];
  const visited = new Set([root.id]);
  const nodesByLevel = new Map(); // Maps level number to array of nodes
  nodesByLevel.set(0, [root]);

  while (queue.length > 0) {
    const current = queue.shift();
    const nextLevel = current.level + 1;

    // Only process outgoing connections
    links.forEach((link) => {
      // Only consider outgoing connections (current is source)
      if (link.source.id === current.id && !visited.has(link.target.id)) {
        const targetNode = nodes.find((n) => n.id === link.target.id);

        if (targetNode) {
          // Set tree properties
          targetNode.treeLevel = nextLevel;
          targetNode.treeParent = current.id;
          targetNode.connectionType = "outgoing";

          // Add to level collection
          if (!nodesByLevel.has(nextLevel)) {
            nodesByLevel.set(nextLevel, []);
          }
          nodesByLevel.get(nextLevel).push(targetNode);

          // Add to processing queue and mark as visited
          queue.push({ id: targetNode.id, level: nextLevel });
          visited.add(targetNode.id);
        }
      }
    });
  }

  // Calculate horizontal positions for each level
  // This distributes nodes evenly across the width
  nodesByLevel.forEach((levelNodes, level) => {
    const levelWidth = levelNodes.length;
    levelNodes.forEach((node, i) => {
      // Calculate position with more even spacing
      // Centers nodes within their level (negative values on left, positive on right)
      node.treePosition = i - (levelWidth - 1) / 2;
    });
  });

  // Apply tree layout with increased spacing
  const LEVEL_VERTICAL_SPACING = 800; // Vertical distance between tree levels
  const HORIZONTAL_SPACING = 850; // Horizontal distance between sibling nodes

  // Get the maximum number of nodes at any level for horizontal centering
  let maxNodesInLevel = 0;
  nodesByLevel.forEach((nodes) => {
    maxNodesInLevel = Math.max(maxNodesInLevel, nodes.length);
  });

  nodes.forEach((node) => {
    if (node.treeLevel !== null) {
      // Fix nodes in place in a tree structure with better centering
      // Using fx/fy properties to override force simulation positions
      // These fixed positions ensure the tree structure is maintained and dragging is unnecessary
      node.fx = width / 2 + node.treePosition * HORIZONTAL_SPACING;
      node.fy = height / 5 + node.treeLevel * LEVEL_VERTICAL_SPACING;
    } else {
      // Move unconnected nodes far away
      node.fx = width * 2; // Move off-screen
      node.fy = height * 2;
    }
  });

  // Highlight only the nodes and links in the tree
  highlightTreeView(rootNode, nodes, links, node, link);

  // Restart simulation with fixed nodes
  // This updates the visualization with new positions
  simulation.alpha(1).restart();

  // Set zoom to fit the tree better
  fitTreeToView(nodes, svg, width, height);
}

/**
 * Applies visual highlighting styles specific to the tree view.
 * This function assumes that `treeLevel`, `treeParent`, etc., have already been calculated
 * and set on the node objects by `createTreeLayout`.
 *
 * Steps:
 * 1. Identifies all nodes and links that are part of the current tree structure.
 * 2. Resets any previous highlighting/styling on all nodes and links.
 * 3. Applies specific CSS classes to nodes based on their tree status and level:
 *    - `.node-highlighted` for all tree nodes.
 *    - `.node-primary` for the root node.
 *    - `.node-depth-1`, `.node-depth-2` for nodes at specific levels.
 *    - `.node-faded` for nodes *not* part of the tree.
 * 4. Applies specific styling to links that connect nodes within the tree:
 *    - Uses `.link-highlighted` class.
 *    - Sets marker to `arrowhead-highlighted`.
 *    - Adjusts label position (`dy`) and size/spacing for emphasis.
 * 5. Applies minor text style adjustments (letter-spacing) to tree nodes after a short delay.
 *
 * @param {Object} rootNode - The node data object used as the root of the tree.
 * @param {Array} nodes - The complete array of node data objects (with tree properties calculated).
 * @param {Array} links - The complete array of link data objects.
 * @param {Object} node - The D3 selection of node elements (<g class="node">).
 * @param {Object} link - The D3 selection of link elements (<g class="link-group">).
 */
function highlightTreeView(rootNode, nodes, links, node, link) {
  // Identify nodes and links that are part of the tree
  const treeNodes = new Set();
  const treeLinks = new Map(); // Maps link index to boolean

  // Identify all nodes in the tree
  nodes.forEach((n) => {
    if (n.treeLevel !== null) {
      treeNodes.add(n.id);
    }
  });

  // Identify links that are part of the tree (only outgoing links from parent to child)
  links.forEach((l, i) => {
    const sourceNode = nodes.find((n) => n.id === l.source.id);
    const targetNode = nodes.find((n) => n.id === l.target.id);

    // A link is part of the tree if the source is a parent and target is its child
    const isTreeLink =
      sourceNode.treeLevel !== null &&
      targetNode.treeLevel !== null &&
      targetNode.treeParent === sourceNode.id;

    treeLinks.set(i, isTreeLink);
  });

  // First reset all highlighting to ensure clean state
  node
    .classed("node-highlighted", false)
    .classed("node-primary", false)
    .classed("node-depth-1", false)
    .classed("node-depth-2", false)
    .classed("node-faded", false);

  // Reset all link styling
  link.each(function () {
    d3.select(this)
      .select("path")
      .classed("link-highlighted", false)
      .attr("marker-end", (d) => `url(#arrowhead-${d.type})`);

    d3.select(this)
      .select("text")
      .classed("link-label-highlighted", false)
      .attr("dy", 20)
      .style("font-size", "16px")
      .style("letter-spacing", "0.5px");
  });

  // Apply highlighting to nodes
  node
    .classed("node-highlighted", (n) => n.treeLevel !== null) // Highlight all tree nodes
    .classed("node-primary", (n) => n.id === rootNode.id) // Special highlight for root
    .classed("node-depth-1", (n) => n.treeLevel === 1) // Level 1 styling
    .classed("node-depth-2", (n) => n.treeLevel === 2) // Level 2 styling
    .classed("node-faded", (n) => n.treeLevel === null); // Fade non-tree nodes

  // Apply highlighting to links - only outgoing connections
  link.each(function (l, i) {
    const isTreeLink = treeLinks.get(i);

    if (isTreeLink) {
      // Highlight the path
      d3.select(this)
        .select("path")
        .classed("link-highlighted", true)
        .attr("marker-end", "url(#arrowhead-highlighted)");

      // Highlight and increase size of the label
      const textElement = d3.select(this).select("text");

      // Position text labels better in tree mode - use different dy values based on level
      const sourceLevel = nodes.find((n) => n.id === l.source.id).treeLevel;
      const targetLevel = nodes.find((n) => n.id === l.target.id).treeLevel;

      // If the link connects nodes with different tree levels, adjust vertical position
      if (sourceLevel !== null && targetLevel !== null) {
        // Use positive dy value for consistent positioning with other views
        textElement.attr("dy", 30); // Use positive value for positioning
      }

      textElement
        .classed("link-label-highlighted", true)
        .transition()
        .duration(200)
        .style("font-size", "26px") // Consistent with hover highlight size
        .style("letter-spacing", "1.2px"); // Match letter spacing with other highlighted links
    }
  });

  // Add a slight delay before applying additional text styles to ensure transitions complete
  setTimeout(() => {
    // Enhance text readability for highlighted nodes in the tree
    node.each(function (n) {
      if (n.treeLevel !== null) {
        // Select all text elements in this node
        d3.select(this)
          .selectAll("text")
          .style("letter-spacing", function () {
            // Apply larger spacing to titles, normal spacing to other text
            return d3.select(this).classed("node-title") ? "1px" : "0.8px";
          });
      }
    });
  }, 250);
}

/**
 * Calculates the appropriate zoom transform (scale and translate) to fit the
 * currently laid-out tree structure within the SVG viewport bounds and applies it.
 *
 * Steps:
 * 1. Filters the `nodes` array to include only those nodes that are part of the tree
 *    (i.e., have a non-null `treeLevel`).
 * 2. Calculates the bounding box (min/max X and Y coordinates) encompassing all tree nodes,
 *    using their fixed positions (`fx`, `fy`) and dimensions (`width`, `height`).
 * 3. Adds padding to the bounding box.
 * 4. Calculates the required scale factor to fit the padded bounding box within the
 *    SVG `width` and `height`, ensuring the scale doesn't exceed 1.0 and applying a small margin.
 * 5. Calculates the translation needed to center the bounding box within the viewport.
 * 6. Applies the calculated scale and translation to the main SVG element using a smooth
 *    zoom transition after a short delay (allowing the simulation tick to potentially update positions first).
 *
 * @param {Array} nodes - The complete array of node data objects (with tree properties and `fx`/`fy` calculated).
 * @param {Object} svg - The D3 selection of the main SVG element.
 * @param {number} width - The width of the SVG canvas.
 * @param {number} height - The height of the SVG canvas.
 */
function fitTreeToView(nodes, svg, width, height) {
  // Filter to only include tree nodes (those with a level assigned)
  const treeNodes = nodes.filter((n) => n.treeLevel !== null);
  if (treeNodes.length > 0) {
    // Calculate bounds
    let minX = Infinity,
      maxX = -Infinity,
      minY = Infinity,
      maxY = -Infinity;
    treeNodes.forEach((n) => {
      minX = Math.min(minX, n.fx - n.width / 2);
      maxX = Math.max(maxX, n.fx + n.width / 2);
      minY = Math.min(minY, n.fy - n.height / 2);
      maxY = Math.max(maxY, n.fy + n.height / 2);
    });

    // Add padding
    const padding = 100;
    minX -= padding;
    maxX += padding;
    minY -= padding;
    maxY += padding;

    // Calculate scale and translate to fit the tree
    // Constrains to 90% of the available space to maintain a margin
    const scale =
      Math.min(
        width / (maxX - minX),
        height / (maxY - minY),
        1.0 // Limit max scale
      ) * 0.9; // 90% to add some margin

    // Apply zoom transform after a slight delay to let nodes position themselves
    setTimeout(() => {
      const centerX = (minX + maxX) / 2;
      const centerY = (minY + maxY) / 2;
      svg
        .transition()
        .duration(750)
        .call(
          d3.zoom().transform,
          d3.zoomIdentity
            .translate(width / 2, height / 2) // Center in viewport
            .scale(scale) // Apply calculated scale
            .translate(-centerX, -centerY) // Offset to position tree properly
        );
    }, 500);
  }
}
