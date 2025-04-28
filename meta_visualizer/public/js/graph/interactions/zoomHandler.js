// Zoom Handler - Implements zoom and pan functionality for the graph

/**
 * Setup zoom and pan behavior for the graph
 * @param {Object} svg - The SVG element
 * @param {Object} g - The SVG group element
 * @returns {Object} The D3 zoom behavior
 */
export function setupZoom(svg, g) {
  // Enable zoom and pan behavior with enhanced settings
  const zoom = d3
    .zoom()
    .scaleExtent([0.05, 8]) // Allow zooming out more (to 5% of original size)
    .on("zoom", (event) => {
      g.attr("transform", event.transform);
      // Log zoom level for debugging
      console.log("Zoom level:", event.transform.k);
    });

  // Apply zoom to SVG and enable proper event handling
  svg
    .call(zoom)
    .on("dblclick.zoom", null) // Disable double-click zoom to avoid accidental zoom
    .on("wheel", (event) => {
      // Prevent page scrolling when zooming the graph
      if (event.target.closest("svg")) {
        event.preventDefault();
      }
    });

  // Initialize with a more zoomed out view to show the whole graph
  svg.call(
    zoom.transform,
    d3.zoomIdentity
      .translate(svg.attr("width") / 2, svg.attr("height") / 2)
      .scale(0.4)
  );

  return zoom;
}

/**
 * Create zoom instructions and controls
 * @param {Object} svg - The SVG element
 * @param {number} width - The width of the SVG
 * @param {number} height - The height of the SVG
 */
export function createZoomControls(svg, width, height) {
  // Add zoom instructions text
  svg
    .append("text")
    .attr("class", "zoom-instructions")
    .attr("x", 20)
    .attr("y", height - 20)
    .style("font-size", "14px")
    .style("fill", "#666")
    .text("Use mouse wheel to zoom, drag to pan");

  // Add zoom controls
  const zoomControls = svg
    .append("g")
    .attr("class", "zoom-controls")
    .attr("transform", `translate(${width - 100}, ${height - 80})`);

  // Zoom in button
  createZoomButton(zoomControls, 0, 0, "+", 1.3, svg);

  // Zoom out button
  createZoomButton(zoomControls, 0, 40, "−", 0.7, svg);
}

/**
 * Create a zoom control button
 * @param {Object} container - The container element for the button
 * @param {number} x - The x coordinate
 * @param {number} y - The y coordinate
 * @param {string} label - The button label
 * @param {number} scaleFactor - The scale factor for zooming
 * @param {Object} svg - The SVG element
 */
function createZoomButton(container, x, y, label, scaleFactor, svg) {
  // Create button background
  container
    .append("rect")
    .attr("x", x)
    .attr("y", y)
    .attr("width", 30)
    .attr("height", 30)
    .attr("rx", 5)
    .attr("fill", "#f8f9fa")
    .attr("stroke", "#ddd")
    .style("cursor", "pointer")
    .on("click", () => {
      svg.transition().duration(300).call(d3.zoom().scaleBy, scaleFactor);
    });

  // Create button label
  container
    .append("text")
    .attr("x", x + 15)
    .attr("y", y + 20)
    .attr("text-anchor", "middle")
    .style("font-size", "18px")
    .style("font-weight", "bold")
    .style("user-select", "none")
    .style("cursor", "pointer")
    .text(label)
    .on("click", () => {
      svg.transition().duration(300).call(d3.zoom().scaleBy, scaleFactor);
    });
}
