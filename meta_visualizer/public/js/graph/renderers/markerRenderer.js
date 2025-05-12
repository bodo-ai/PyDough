// Marker Renderer - Renders arrow markers for links

/**
 * Setup arrow markers for different link types
 * @param {Object} svg - The SVG element
 */
export function setupMarkers(svg) {
  // Define arrow markers
  svg
    .append("defs")
    .selectAll("marker")
    .data([
      "standard",
      "simple_join",
      "cartesian_product",
      "general_join",
      "highlighted",
    ])
    .enter()
    .append("marker")
    .attr("id", (d) => `arrowhead-${d}`)
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 33)
    .attr("refY", 0)
    .attr("markerWidth", 10)
    .attr("markerHeight", 10)
    .attr("orient", "auto")
    .append("path")
    .attr("d", (d) => getMarkerPath(d))
    .attr("class", (d) =>
      d === "highlighted"
        ? "arrowhead-highlighted"
        : d === "general_join"
        ? "arrowhead-general_join"
        : d === "cartesian_product"
        ? "arrowhead-cartesian_product"
        : "arrowhead"
    );
}

/**
 * Get the SVG path for a marker based on its type
 * @param {string} type - The type of marker
 * @returns {string} The SVG path
 */
function getMarkerPath(type) {
  if (type === "simple_join") {
    return "M0,-5L10,0L0,5";
  } else if (type === "cartesian_product") {
    return "M0,-5L10,0L0,5M-2,-3L8,0L-2,3";
  } else {
    return "M0,-5L10,0L0,5";
  }
}
