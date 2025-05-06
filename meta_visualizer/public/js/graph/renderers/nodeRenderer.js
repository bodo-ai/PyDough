// Node Renderer - Renders node elements (collection boxes)

// --- Layout Constants ---
const paddingX = 20;
const nodeTitleY = 35;
const tablePathY = 60;
const dividingLineY = 70;
const propertiesHeaderY = 90;
const columnStartY = 110; // Y position of the first column item
const subcollectionHeaderYOffset = 20; // Vertical distance from last column item to subcollection header
const subcollectionStartYOffset = 20; // Vertical distance from subcollection header to first subcollection item
const lineHeight = 22; // Vertical distance between lines (columns/subcollections)
const bottomPadding = 15; // Space below last item, including tooltip indicator
const tooltipOffset = 20; // Offset for tooltip from bottom-right corner
const minHeight = 150; // Minimum node height
// --- End Layout Constants ---

/**
 * Calculate the required height for a node based on its content.
 * @param {Object} d - The node data object.
 * @returns {number} The calculated height.
 */
function calculateNodeHeight(d) {
  // Start with the Y position of the "Properties:" header
  let lastElementY = propertiesHeaderY;

  // Calculate Y position of the baseline of the last column item
  if (d.columns.length > 0) {
    // Baseline of the first column is columnStartY, last is offset by (length-1)*lineHeight
    lastElementY = columnStartY + (d.columns.length - 1) * lineHeight;
  }

  // Calculate Y position of the baseline of the last subcollection item
  if (d.subcollections.length > 0) {
    // Y position of the subcollection header
    const subcollectionHeaderBaseY =
      d.columns.length > 0
        ? columnStartY + d.columns.length * lineHeight // Below last column
        : propertiesHeaderY; // Below "Properties:" if no columns
    const subcollectionHeaderY =
      subcollectionHeaderBaseY + subcollectionHeaderYOffset;

    // Y position of the first subcollection item's baseline
    const firstSubcollectionItemY =
      subcollectionHeaderY + subcollectionStartYOffset;

    // Y position of the last subcollection item's baseline
    lastElementY =
      firstSubcollectionItemY + (d.subcollections.length - 1) * lineHeight;
  }

  // Calculate total height: baseline of last element + padding below it
  // Adding roughly half a line height adjusts from baseline to approximate bottom of text.
  let calculatedHeight = lastElementY + lineHeight / 2 + bottomPadding;

  // Ensure minimum height is respected
  return Math.max(calculatedHeight, minHeight);
}

/**
 * Create nodes for the graph
 * @param {Object} g - The SVG group element
 * @param {Array} nodes - The array of nodes
 * @param {Object} tooltipHandlers - Object containing tooltip handlers and tooltip element
 * @returns {Object} The D3 selection of nodes
 */
export function createNodes(g, nodes, tooltipHandlers) {
  const {
    tooltip,
    showNodeTooltip,
    hideTooltip,
    isMouseOverTooltip,
    clearHoverElement,
    scheduleHide,
    cancelHide,
  } = tooltipHandlers || {};

  // Calculate required height for each node before creating elements
  nodes.forEach((d) => {
    // Store original height if needed, otherwise overwrite or use a new property
    d.calculatedHeight = calculateNodeHeight(d);
  });

  // Create the node containers
  const node = g
    .selectAll(".node")
    .data(nodes)
    .enter()
    .append("g")
    .attr("class", "node")
    .attr("id", (d) => `node-${d.id}`);

  // Add rectangles for the collection boxes
  node
    .append("rect")
    .attr("width", (d) => d.width)
    .attr("height", (d) => d.calculatedHeight)
    .attr("rx", 10)
    .attr("ry", 10);

  // Add collection titles
  node
    .append("text")
    .attr("x", paddingX)
    .attr("y", nodeTitleY)
    .attr("class", "node-title")
    .style("font-size", "18px")
    .style("font-weight", "bold")
    .text((d) => d.name);

  // Add table path
  node
    .append("text")
    .attr("x", paddingX)
    .attr("y", tablePathY)
    .attr("class", "table-path")
    .style("font-size", "13px")
    .text((d) => d.tablePath);

  // Add dividing line
  node
    .append("line")
    .attr("x1", paddingX)
    .attr("y1", dividingLineY)
    .attr("x2", (d) => d.width - paddingX)
    .attr("y2", dividingLineY)
    .attr("stroke", "#333")
    .attr("stroke-width", 1);

  // Add properties header
  addPropertiesHeader(node);

  // Add columns
  addColumnsList(node);

  // Add subcollections header and list
  addSubcollectionsList(node);

  // Add tooltip indicator in bottom right corner with event handlers
  const tooltipIndicator = node
    .append("g")
    .attr("class", "tooltip-indicator-group")
    // Position tooltip relative to calculated height and width
    .attr(
      "transform",
      (d) =>
        `translate(${d.width - tooltipOffset}, ${
          d.calculatedHeight - tooltipOffset
        })`
    );

  // Only add event handlers if tooltip handlers are provided
  if (
    tooltip &&
    showNodeTooltip &&
    hideTooltip &&
    isMouseOverTooltip &&
    clearHoverElement &&
    scheduleHide &&
    cancelHide
  ) {
    tooltipIndicator
      .on("mouseover", function (event, d) {
        // Cancel any pending hide operations
        cancelHide();
        // Show tooltip for this indicator
        const nodeId = `node-${d.id}`;
        showNodeTooltip(tooltip, d, event, nodeId);
      })
      .on("mouseout", function (event, d) {
        const nodeId = `node-${d.id}`;
        // Schedule tooltip to hide after a short delay
        // Tooltip mouseover will cancel this if the mouse moves onto it.
        scheduleHide(tooltip, nodeId, 100); // 100ms delay
      });
  }

  tooltipIndicator
    .append("circle")
    .attr("class", "tooltip-indicator")
    .attr("r", 8)
    .attr("fill", "#1f77b4")
    .attr("opacity", 0.7);

  // Add question mark in the indicator
  tooltipIndicator
    .append("text")
    .attr("class", "tooltip-indicator-text")
    .attr("text-anchor", "middle")
    .attr("dy", "0.35em")
    .attr("fill", "white")
    .attr("font-size", "12px")
    .attr("font-weight", "bold")
    .text("?");

  return node;
}

/**
 * Add the properties header to nodes
 * @param {Object} node - The D3 selection of nodes
 */
function addPropertiesHeader(node) {
  node
    .append("text")
    .attr("x", paddingX)
    .attr("y", propertiesHeaderY)
    .attr("class", "properties-header")
    .style("font-size", "15px")
    .style("font-weight", "bold")
    .text("Properties:");
}

/**
 * Add the columns list to nodes
 * @param {Object} node - The D3 selection of nodes
 */
function addColumnsList(node) {
  node
    .selectAll(".column-item")
    .data((d) => d.columns.map((column) => ({ column, parent: d })))
    .enter()
    .append("text")
    .attr("x", paddingX + 10) // Indent properties slightly
    .attr("y", (d, i) => columnStartY + i * lineHeight)
    .attr("class", "column-item")
    .style("font-size", "13px")
    .text((d) => `${d.column.name} (${d.column.dataType})`);
}

/**
 * Add the subcollections header and list to nodes
 * @param {Object} node - The D3 selection of nodes
 */
function addSubcollectionsList(node) {
  // Add subcollections header
  node
    .append("text")
    .attr("x", paddingX)
    .attr("y", (d) => {
      // Calculate Y position based on whether there are columns
      const baseY =
        d.columns.length > 0
          ? columnStartY + d.columns.length * lineHeight // Below last column
          : propertiesHeaderY; // Below "Properties:" if no columns
      return baseY + subcollectionHeaderYOffset;
    })
    .attr("class", "properties-header") // Reuse class for styling consistency
    .style("font-size", "15px")
    .style("font-weight", "bold")
    .text("Subcollections:");

  // Add subcollection items
  node
    .selectAll(".subcollection-item")
    .data((d) => d.subcollections.map((subcol) => ({ subcol, parent: d })))
    .enter()
    .append("text")
    .attr("x", paddingX + 10) // Indent subcollections slightly
    .attr("y", (d, i) => {
      const parent = d.parent;
      // Calculate Y position for subcollection header based on column count
      const subcollectionHeaderBaseY =
        parent.columns.length > 0
          ? columnStartY + parent.columns.length * lineHeight
          : propertiesHeaderY;
      const subcollectionHeaderY =
        subcollectionHeaderBaseY + subcollectionHeaderYOffset;
      // Calculate Y position for this subcollection item
      const currentItemY =
        subcollectionHeaderY + subcollectionStartYOffset + i * lineHeight;
      return currentItemY;
    })
    .attr(
      "class",
      (d) =>
        `subcollection-item subcollection-${d.subcol.type} ${
          d.subcol.isReverse ? "subcollection-reverse" : ""
        }`
    )
    .style("font-size", "13px")
    .text((d) => {
      // Always show relationships as outgoing (→) to indicate they can be accessed from this node
      return `${d.subcol.name} → ${d.subcol.target}`;
    });
}
