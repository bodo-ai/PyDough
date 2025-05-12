// Link Renderer - Renders relationship links between database collections

/**
 * Creates visual links representing database relationships between collections
 * @param {Object} g - SVG group for graph visualization
 * @param {Array} links - Array of relationship data (simple_join or cartesian_product)
 * @param {Array} nodes - Array of node objects for looking up source and target
 * @returns {Object} D3 selection of link groups for force simulation updates
 */
export function createLinks(g, links, nodes) {
  console.log("Creating links:", links.length);
  // Process links to ensure source and target are node objects not just IDs
  links.forEach((link) => {
    // If source or target are string IDs, convert them to node objects
    if (typeof link.source === "string") {
      link.sourceId = link.source;
      link.source = nodes.find((node) => node.id === link.source);
    }
    if (typeof link.target === "string") {
      link.targetId = link.target;
      link.target = nodes.find((node) => node.id === link.target);
    }
  });

  // Create link groups with relationship-specific styling
  // Groups contain path, arrowhead, and label for each database relationship
  const link = g
    .selectAll(".link")
    .data(links)
    .enter()
    .append("g")
    .attr("class", (d) => `link-group link-${d.type}`);

  console.log("Created link groups:", link.size());

  // Draw relationship paths with directional arrowheads
  link
    .append("path")
    .attr("class", (d) => `link link-${d.type}`)
    .attr(
      "marker-end",
      (d) =>
        `url(#arrowhead-${d.type === "general_join" ? "general_join" : d.type})`
    )
    .attr("id", (d, i) => `link-${i}`)
    .attr("d", (d) => {
      // If path data exists, use it, otherwise calculate a default path
      if (d.path && d.path.forward) {
        return d.path.forward;
      }

      // Skip links with missing source or target node
      if (!d.source || !d.target) {
        console.error("Link missing source or target:", d);
        return "M0,0L0,0"; // Empty path
      }

      // Default path calculation
      const sourceX = d.source.x + d.source.width / 2;
      const sourceY = d.source.y + d.source.height / 2;
      const targetX = d.target.x + d.target.width / 2;
      const targetY = d.target.y + d.target.height / 2;
      const path = `M${sourceX},${sourceY}L${targetX},${targetY}`;
      console.log("Path:", path);
      return path;
    });

  // Add relationship labels with proper orientation
  link
    .append("text")
    .attr("class", (d) => `link-label`)
    .attr("dy", 20) // Position below the line with positive value
    .style("font-size", "16px")
    .style("font-weight", "bold")
    .style("pointer-events", "none") // Prevent the text from interfering with hover events
    .style("paint-order", "stroke") // Draw stroke first, then fill
    .style("stroke", "white") // White stroke for background/outline
    .style("stroke-width", "3px") // Thickness of background
    .style("stroke-linecap", "round") // Round the stroke ends
    .style("stroke-linejoin", "round") // Round the stroke corners
    .append("textPath")
    .attr("xlink:href", (d, i) => `#link-${i}`)
    .attr("startOffset", "50%")
    .attr("text-anchor", "middle")
    .attr("side", "left") // Place text on the left side of the path to flip orientation
    .text((d) => {
      return d.name;
    });

  console.log("Finished creating links");
  return link;
}
