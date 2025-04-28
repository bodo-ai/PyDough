// Highlight Handler - Handles highlighting of nodes and connections

import {
  showNodeTooltip,
  showLinkTooltip,
  hideTooltip,
} from "./tooltipHandler.js";
import { createTreeLayout } from "../layouts/treeLayout.js";

/**
 * Setup highlighting functionality for nodes and links
 * @param {Object} node - The D3 selection of nodes
 * @param {Object} link - The D3 selection of links
 * @param {Array} nodes - The array of nodes
 * @param {Array} links - The array of links
 * @param {Object} tooltip - The tooltip element
 * @param {Object} simulation - The D3 force simulation
 * @param {Object} svg - The SVG element
 * @param {number} width - The width of the SVG
 * @param {number} height - The height of the SVG
 * @returns {Object} An object containing highlighting functions
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
  // Add mouseover functionality to links with improved highlighting
  link
    .on("mouseover", function (event, d) {
      showLinkTooltip(tooltip, d, event);
      highlightLink(this, true);
      highlightNodes(node, d.source.id, d.target.id, true);
    })
    .on("mouseout", function (event, d) {
      hideTooltip(tooltip);
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
  // Highlight or unhighlight the path
  d3.select(linkElement)
    .select("path")
    .classed("link-highlighted", highlight)
    .style("stroke-width", highlight ? "3px" : null)
    .attr(
      "marker-end",
      (d) => `url(#arrowhead-${highlight ? "highlighted" : d.type})`
    );

  // Highlight or unhighlight the label
  d3.select(linkElement)
    .select("text")
    .classed("link-label-highlighted", highlight)
    .attr("dy", highlight ? -40 : -20) // Move further up when highlighted
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

  // BFS to find all connected nodes - ONLY following outgoing edges, not reverse
  const queue = [{ id: d.id, depth: 0 }];
  const visited = new Set([d.id]);
  const highlightedLinks = new Set();
  const highlightedNodes = new Set([d.id]); // Track highlighted nodes separately

  while (queue.length > 0) {
    const current = queue.shift();

    // ONLY process outgoing connections (where current node is the source)
    links.forEach((link, linkIndex) => {
      // Process outgoing links only (node is source)
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
        .transition()
        .duration(200)
        .style("font-size", "25px"); // Increase font size to 25px
    }
  });

  // Show tooltip with node info
  showNodeTooltip(tooltip, d, event);
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
      .attr("marker-end", (d) => `url(#arrowhead-${d.type})`);

    // Reset text styling and size
    d3.select(this)
      .select("text")
      .classed("link-label-highlighted", false)
      .attr("dy", -20) // Return to default position
      .transition()
      .duration(200)
      .style("font-size", "16px") // Return to normal size
      .style("letter-spacing", "0.5px"); // Return to normal letter spacing
  });

  // Hide tooltip
  hideTooltip(tooltip);
}
