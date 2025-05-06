// Force Layout - Implements a force-directed graph layout using D3.js
// D3 force simulations use physics-based calculations to position nodes in a network graph

/**
 * Setup the force simulation for the graph
 *
 * - D3 force layout uses physics simulation to position elements
 * - Forces can be gravity, collision, links, and charges
 * - The simulation runs continuously until it reaches equilibrium
 *
 * @param {Array} nodes - The array of nodes to position
 * @param {Array} links - The array of links connecting nodes
 * @param {number} width - The width of the SVG container
 * @param {number} height - The height of the SVG container
 * @returns {Object} The D3 force simulation instance that can be used to update positions
 */
export function setupForceSimulation(nodes, links, width, height) {
  return d3
    .forceSimulation(nodes) // Initialize the simulation with our nodes
    .force(
      "link",
      d3
        .forceLink(links) // Add link forces that pull connected nodes together
        .id((d) => d.id) // Specify which property to use as node identifier
        .distance(600) // Target distance between linked nodes (higher = more spread out)
    )
    .force("charge", d3.forceManyBody().strength(-6000)) // Add repulsion between nodes (negative = repel, positive = attract)
    .force("center", d3.forceCenter(width / 2, height / 2)) // Add centering force to keep the graph centered in view
    .force(
      "collision",
      d3.forceCollide().radius((d) => Math.sqrt(d.width * d.height) / 2 + 150) // Prevent node overlap by adding collision detection
    );
}
