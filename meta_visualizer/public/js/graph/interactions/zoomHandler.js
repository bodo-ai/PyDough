// Zoom Handler - Implements zoom and pan functionality for the graph

/**
 * Initializes and configures the D3 zoom behavior for the SVG canvas.
 * Sets up the zoom/pan limits, attaches the zoom listener to the main SVG group,
 * disables double-click zoom, prevents page scroll during zoom, and sets an initial transform.
 *
 * @param {Object} svg - The D3 selection of the main SVG element.
 * @param {Object} g - The D3 selection of the primary SVG group (`<g>`) that contains all graph elements (nodes, links).
 *                  This is the group whose transform will be updated by the zoom behavior.
 * @returns {Object} The configured D3 zoom behavior object, which can be used to programmatically control zoom/pan.
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
 * Creates and appends zoom instruction text and zoom control buttons (+/-) to the SVG canvas.
 * These elements are positioned in corners and are not affected by the zoom/pan transforms applied to the main graph group `g`.
 *
 * @param {Object} svg - The D3 selection of the main SVG element where controls will be added.
 * @param {number} width - The width of the SVG canvas, used for positioning controls.
 * @param {number} height - The height of the SVG canvas, used for positioning controls.
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
 * Creates a single zoom button (background rectangle and text label) within a given container group.
 * Attaches a click handler to trigger a zoom transition on the main SVG element.
 *
 * @param {Object} container - The D3 selection of the SVG group (`<g>`) where the button should be appended.
 * @param {number} x - The x-coordinate for the button's top-left corner, relative to the container.
 * @param {number} y - The y-coordinate for the button's top-left corner, relative to the container.
 * @param {string} label - The text label for the button (e.g., "+", "−").
 * @param {number} scaleFactor - The multiplicative factor to apply when the button is clicked (e.g., 1.3 for zoom in, 0.7 for zoom out).
 * @param {Object} svg - The D3 selection of the main SVG element, which the zoom action targets.
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
