// Tree Layout - Implements a hierarchical tree visualization layout
// This layout organizes nodes in a parent-child hierarchy with multiple levels

/**
 * Create a tree layout with the given node as the root
 *
 * - D3 tree layouts position nodes in a hierarchical structure
 * - Nodes are arranged in levels with consistent vertical spacing
 * - Each level's nodes are positioned horizontally to balance the tree
 * - Fixed positions (fx, fy) are used to override force simulation
 *
 * @param {Object} rootNode - The root node for the tree
 * @param {Array} nodes - The array of nodes
 * @param {Array} links - The array of links
 * @param {Object} node - The D3 selection of nodes
 * @param {Object} link - The D3 selection of links
 * @param {Object} simulation - The D3 force simulation
 * @param {Object} svg - The SVG element
 * @param {number} width - The width of the SVG
 * @param {number} height - The height of the SVG
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
      node.fx = width / 2 + node.treePosition * HORIZONTAL_SPACING;
      node.fy = height / 5 + node.treeLevel * LEVEL_VERTICAL_SPACING; // Start higher in the viewport (1/5 instead of 1/3)
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
 * Highlight tree connections
 *
 * - Applies visual styling to differentiate tree elements
 * - Root node gets primary highlight
 * - Tree nodes and links are highlighted based on their level
 * - Non-tree elements are faded out
 *
 * @param {Object} rootNode - The root node for the tree
 * @param {Array} nodes - The array of nodes
 * @param {Array} links - The array of links
 * @param {Object} node - The D3 selection of nodes
 * @param {Object} link - The D3 selection of links
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
        // For links going down in the tree, position the label above the line
        textElement.attr("dy", -40); // Further increased for better visibility (-35 to -40)
      }

      textElement
        .classed("link-label-highlighted", true)
        .transition()
        .duration(200)
        .style("font-size", "26px"); // Consistent with hover highlight size
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
 * Fit the tree view to the viewport
 *
 * - Calculates the bounding box of all tree nodes
 * - Determines optimal zoom and pan to fit the tree
 * - Animates transition to the new view
 *
 * @param {Array} nodes - The array of nodes
 * @param {Object} svg - The SVG element
 * @param {number} width - The width of the SVG
 * @param {number} height - The height of the SVG
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
