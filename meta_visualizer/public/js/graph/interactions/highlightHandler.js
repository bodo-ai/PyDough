// Highlight Handler - Handles highlighting of nodes and connections

import {
  showNodeTooltip,
  showLinkTooltip,
  hideTooltip,
  isMouseOverTooltip,
  clearHoverElement,
} from "./tooltipHandler.js";
import { createTreeLayout } from "../layouts/treeLayout.js";

/**
 * Setup highlighting functionality for nodes and links.
 * Attaches mouseover/mouseout listeners to links for basic highlighting and tooltip display.
 * Also provides functions for more complex highlighting triggered by other events (e.g., node clicks).
 *
 * @param {Object} node - The D3 selection of node elements (<g class="node">).
 * @param {Object} link - The D3 selection of link elements (<g class="link-group">).
 * @param {Array} nodes - The complete array of node data objects.
 * @param {Array} links - The complete array of link data objects.
 * @param {Object} tooltip - The D3 selection of the tooltip element.
 * @param {Object} simulation - The D3 force simulation instance.
 * @param {Object} svg - The main SVG element D3 selection.
 * @param {number} width - The width of the SVG canvas.
 * @param {number} height - The height of the SVG canvas.
 * @returns {Object} An object containing callable functions:
 *   - `highlightConnections`: Function to highlight all nodes reachable from a given node via outgoing links.
 *   - `resetHighlighting`: Function to remove all highlighting from nodes and links.
 *   - `createTreeLayout`: Function to initiate the tree layout starting from a given root node.
 */
export function setupHighlighting(
  node,
  link,
  nodes,
  links,
  tooltip,
  simulation,
  svg,
  width,
  height
) {
  // Add mouseover functionality to links
  link
    .on("mouseover", function (event, d) {
      const linkId = `link-${d.source.id}-${d.target.id}-${d.name}`;
      showLinkTooltip(tooltip, d, event, linkId);
      highlightLink(this, true);
      highlightNodes(node, d.source.id, d.target.id, true);
    })
    .on("mouseout", function (event, d) {
      const linkId = `link-${d.source.id}-${d.target.id}-${d.name}`;
      // Only hide tooltip if not hovering over tooltip itself
      // We'll let the tooltip's own mouseout handler hide it when appropriate
      if (!isMouseOverTooltip(event, tooltip)) {
        hideTooltip(tooltip);
      }
      // Always clear the hover element reference
      clearHoverElement(linkId);
      highlightLink(this, false);
      highlightNodes(node, d.source.id, d.target.id, false);
    });

  return {
    highlightConnections: (event, d) =>
      highlightConnections(event, d, node, link, nodes, links, tooltip),
    resetHighlighting: () => resetHighlighting(node, link, tooltip),
    createTreeLayout: (rootNode) =>
      createTreeLayout(
        rootNode,
        nodes,
        links,
        node,
        link,
        simulation,
        svg,
        width,
        height
      ),
  };
}

/**
 * Highlight a link
 * @param {Object} linkElement - The link element to highlight
 * @param {boolean} highlight - Whether to highlight or unhighlight
 */
function highlightLink(linkElement, highlight) {
  // Highlight or unhighlight both forward and reverse paths
  d3.select(linkElement)
    .selectAll("path")
    .classed("link-highlighted", highlight)
    .style("stroke-width", highlight ? "3px" : null)
    .attr(
      "marker-end",
      (d) =>
        `url(#arrowhead-${
          highlight
            ? "highlighted"
            : d.type === "general_join"
            ? "general_join"
            : d.type === "cartesian_product"
            ? "cartesian_product"
            : d.type
        })`
    );

  // Highlight or unhighlight both forward and reverse labels
  d3.select(linkElement)
    .selectAll("text")
    .classed("link-label-highlighted", highlight)
    .attr("dy", highlight ? 30 : 20) // Use positive values for positioning
    .transition()
    .duration(200)
    .style("font-size", highlight ? "26px" : "16px") // Increase/decrease size
    .style("letter-spacing", highlight ? "1.2px" : "0.5px"); // Adjust letter spacing
}

/**
 * Highlight nodes
 * @param {Object} node - The D3 selection of nodes
 * @param {string} sourceId - The source node ID
 * @param {string} targetId - The target node ID
 * @param {boolean} highlight - Whether to highlight or unhighlight
 */
function highlightNodes(node, sourceId, targetId, highlight) {
  node
    .filter((n) => n.id === sourceId || n.id === targetId)
    .classed("node-highlighted", highlight);
}

/**
 * Highlight connections starting from a node
 * @param {Object} event - The mouse event
 * @param {Object} d - The node data
 * @param {Object} node - The D3 selection of nodes
 * @param {Object} link - The D3 selection of links
 * @param {Array} nodes - The array of nodes
 * @param {Array} links - The array of links
 * @param {Object} tooltip - The tooltip element
 */
function highlightConnections(event, d, node, link, nodes, links, tooltip) {
  // Reset previous highlighting
  resetHighlighting(node, link, tooltip);

  // BFS to find all connected nodes - only following outgoing edges
  const queue = [{ id: d.id, depth: 0 }];
  const visited = new Set([d.id]);
  const highlightedLinks = new Set();
  const highlightedNodes = new Set([d.id]); // Track highlighted nodes separately

  while (queue.length > 0) {
    const current = queue.shift();

    // Process only outgoing connections
    links.forEach((link, linkIndex) => {
      // Only process outgoing links (node is source) to unvisited nodes
      if (link.source.id === current.id && !visited.has(link.target.id)) {
        highlightedLinks.add(linkIndex);
        visited.add(link.target.id);
        highlightedNodes.add(link.target.id);
        queue.push({ id: link.target.id, depth: current.depth + 1 });
      }
    });
  }

  // Highlight the visited nodes
  node
    .classed("node-highlighted", (n) => highlightedNodes.has(n.id))
    .classed("node-primary", (n) => n.id === d.id);

  // Highlight all components of the links (paths and labels)
  link.each(function (l, i) {
    if (highlightedLinks.has(i)) {
      // Highlight the path
      d3.select(this)
        .select("path")
        .classed("link-highlighted", true)
        .attr("marker-end", `url(#arrowhead-highlighted)`);

      // Highlight and increase the size of the label
      d3.select(this)
        .select("text")
        .classed("link-label-highlighted", true)
        .attr("dy", 30) // Use positive value for positioning
        .transition()
        .duration(200)
        .style("font-size", "25px") // Increase font size to 25px
        .style("letter-spacing", "1.2px"); // Adjust letter spacing for better readability
    }
  });
}

/**
 * Reset highlighting for all nodes and links
 * @param {Object} node - The D3 selection of nodes
 * @param {Object} link - The D3 selection of links
 * @param {Object} tooltip - The tooltip element
 */
function resetHighlighting(node, link, tooltip) {
  node.classed("node-highlighted", false).classed("node-primary", false);

  // Reset link styling
  link.each(function (d) {
    // Reset path styling
    d3.select(this)
      .select("path")
      .classed("link-highlighted", false)
      .style("stroke-width", null)
      .attr(
        "marker-end",
        (d) =>
          `url(#arrowhead-${
            d.type === "general_join"
              ? "general_join"
              : d.type === "cartesian_product"
              ? "cartesian_product"
              : d.type
          })`
      );

    // Reset text styling and size
    d3.select(this)
      .select("text")
      .classed("link-label-highlighted", false)
      .attr("dy", 20) // Return to default position with positive value
      .transition()
      .duration(200)
      .style("font-size", "16px") // Return to normal size
      .style("letter-spacing", "0.5px"); // Return to normal letter spacing
  });
}
