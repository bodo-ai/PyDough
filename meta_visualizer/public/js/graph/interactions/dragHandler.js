// Drag Handler - Implements dragging functionality for nodes

/**
 * Setup drag functionality for nodes
 * @param {Object} node - The D3 selection of nodes
 * @param {Object} simulation - The D3 force simulation
 */
export function setupDrag(node, simulation) {
  node.call(
    d3
      .drag()
      .on("start", (event, d) => dragstarted(event, d, simulation))
      .on("drag", (event, d) => dragged(event, d))
      .on("end", (event, d) => dragended(event, d, simulation))
  );
}

/**
 * Handle the start of a drag operation
 * @param {Object} event - The drag event
 * @param {Object} d - The node data
 * @param {Object} simulation - The D3 force simulation
 */
function dragstarted(event, d, simulation) {
  if (!event.active) simulation.alphaTarget(0.3).restart();
  d.fx = d.x;
  d.fy = d.y;
}

/**
 * Handle the drag operation
 * @param {Object} event - The drag event
 * @param {Object} d - The node data
 */
function dragged(event, d) {
  d.fx = event.x;
  d.fy = event.y;
}

/**
 * Handle the end of a drag operation
 * @param {Object} event - The drag event
 * @param {Object} d - The node data
 * @param {Object} simulation - The D3 force simulation
 */
function dragended(event, d, simulation) {
  if (!event.active) simulation.alphaTarget(0);
  d.fx = null;
  d.fy = null;
}
