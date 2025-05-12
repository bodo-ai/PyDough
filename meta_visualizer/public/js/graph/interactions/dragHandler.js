// Drag Handler - Implements dragging functionality for nodes

/**
 * Setup drag functionality for nodes
 * @param {Object} node - The D3 selection of nodes
 * @param {Object} simulation - The D3 force simulation
 * @param {Object} state - Object containing application state, including isTreeView
 */
export function setupDrag(node, simulation, state) {
  node.call(
    d3
      .drag()
      .on("start", (event, d) => dragstarted(event, d, simulation, state))
      .on("drag", (event, d) => dragged(event, d, state))
      .on("end", (event, d) => dragended(event, d, simulation, state))
  );
}

/**
 * Handle the start of a drag operation
 * @param {Object} event - The drag event
 * @param {Object} d - The node data
 * @param {Object} simulation - The D3 force simulation
 * @param {Object} state - Object containing application state, including isTreeView
 */
function dragstarted(event, d, simulation, state) {
  // Exit tree view mode if we're in it, but preserve current positions
  if (state.isTreeView) {
    // Call the callback to exit tree view mode
    if (state.onExitTreeView) {
      state.onExitTreeView();
    }
  }

  if (!event.active) simulation.alphaTarget(0.3).restart();

  // Keep the current position as the fixed position
  d.fx = d.x;
  d.fy = d.y;
}

/**
 * Handle the drag operation
 * @param {Object} event - The drag event
 * @param {Object} d - The node data
 * @param {Object} state - Object containing application state, including isTreeView
 */
function dragged(event, d, state) {
  d.fx = event.x;
  d.fy = event.y;
}

/**
 * Handle the end of a drag operation
 * @param {Object} event - The drag event
 * @param {Object} d - The node data
 * @param {Object} simulation - The D3 force simulation
 * @param {Object} state - Object containing application state, including isTreeView
 */
function dragended(event, d, simulation, state) {
  if (!event.active) simulation.alphaTarget(0);

  // Keep positions fixed after drag in normal mode to prevent nodes from floating away
  d.fx = d.x;
  d.fy = d.y;
}
