// Tooltip Handler - Manages tooltip display on hover over nodes and links

// Track which element (node or link) the mouse is currently over
let currentHoverElement = null;
// ID for tooltip auto-hide timeout - Managed globally now
let pendingHideTimeoutId = null;

/**
 * Creates and appends the main tooltip div element to the document body.
 * The tooltip is initially hidden and configured with mouse event handlers
 * to prevent auto-hiding when the mouse is moved over the tooltip itself.
 *
 * @returns {Object} The D3 selection of the created tooltip div.
 */
export function createTooltip() {
  const tooltip = d3
    .select("body")
    .append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);

  tooltip
    .on("mouseover", function () {
      // Cancel any pending hide when mouse enters the tooltip
      cancelHide();
      // Ensure it's visible and interactive
      d3.select(this).transition().duration(0).style("opacity", 0.9);
      d3.select(this).style("pointer-events", "auto");
    })
    .on("mouseout", function (event) {
      // Hide immediately when mouse leaves the tooltip itself
      hideTooltip(d3.select(this));
      // Clear hover tracking associated with this tooltip session
      if (currentHoverElement) {
        clearHoverElement(currentHoverElement);
      }
    });

  return tooltip;
}

/**
 * Shows and populates the tooltip with details about a graph node.
 * Calculates the tooltip position based on the mouse event coordinates.
 *
 * @param {Object} tooltip - The D3 selection of the tooltip element.
 * @param {Object} node - The data object for the node being hovered.
 * @param {Object} event - The D3 mouse event that triggered the tooltip.
 * @param {string} elementId - A unique identifier for the hovered element (e.g., "node-123") used for tracking.
 */
export function showNodeTooltip(tooltip, node, event, elementId) {
  // Update the current hover element
  currentHoverElement = elementId;

  // Make tooltip visible and interactive
  tooltip.transition().duration(200).style("opacity", 0.9);
  tooltip.style("pointer-events", "auto"); // Ensure tooltip is interactive when shown

  // Create a table-like structured tooltip that's still compact
  let tooltipContent = `<div class="tooltip-title">${node.name}</div>`;

  // Create a table structure for the data
  tooltipContent += `<table class="tooltip-table">
    <tr>
      <td class="tooltip-label">Path:</td>
      <td>${node.tablePath}</td>
    </tr>
    <tr>
      <td class="tooltip-label">Unique by:</td>
      <td>${formatUniqueProperties(node.uniqueProperties)}</td>
    </tr>
  </table>`;

  // Add columns with smaller font
  if (node.columns && node.columns.length > 0) {
    tooltipContent += `<div class="tooltip-section">
      <div class="tooltip-section-title">Columns</div>
      <div class="tooltip-small-content">`;

    // Only show first 8 columns with smaller font to keep compact
    const displayColumns = node.columns.slice(0, 8);
    displayColumns.forEach((col) => {
      tooltipContent += `<div class="tooltip-column">${col.name} <span class="tooltip-datatype">(${col.dataType})</span></div>`;
    });

    // If more columns exist, add a count
    if (node.columns.length > 8) {
      tooltipContent += `<div class="tooltip-more">+${
        node.columns.length - 8
      } more</div>`;
    }

    tooltipContent += `</div></div>`;
  }

  // Add relationships with smaller font
  if (node.sub_collections && node.sub_collections.length > 0) {
    tooltipContent += `<div class="tooltip-section">
      <div class="tooltip-section-title">Relationships</div>
      <div class="tooltip-small-content">`;

    // Only show first 5 relationships with smaller font to keep compact
    const displayRelations = node.sub_collections.slice(0, 5);
    displayRelations.forEach((rel) => {
      tooltipContent += `<div class="tooltip-relation">${rel.name} <span class="tooltip-relation-type">(${rel.type} → ${rel.target.id})</span></div>`;
    });

    // If more relationships exist, add a count
    if (node.sub_collections.length > 5) {
      tooltipContent += `<div class="tooltip-more">+${
        node.sub_collections.length - 5
      } more</div>`;
    }

    tooltipContent += `</div></div>`;
  }

  tooltip
    .html(tooltipContent)
    .style("left", event.pageX + 10 + "px")
    .style("top", event.pageY - 28 + "px");
}

/**
 * Formats an array of unique properties (or arrays of properties) into a compact string.
 * Used for display within the node tooltip.
 *
 * @param {Array<string>|Array<Array<string>>} properties - The unique properties array from node data.
 * @returns {string} A formatted string representation (e.g., "prop1, prop2 | prop3, prop4" or "None").
 */
function formatUniqueProperties(properties) {
  if (!properties || properties.length === 0) return "None";

  return Array.isArray(properties[0])
    ? properties.map((arr) => arr.join(", ")).join(" | ")
    : properties.join(", ");
}

/**
 * Shows and populates the tooltip with details about a graph link (relationship).
 * Calculates the tooltip position based on the mouse event coordinates.
 *
 * @param {Object} tooltip - The D3 selection of the tooltip element.
 * @param {Object} link - The data object for the link being hovered.
 * @param {Object} event - The D3 mouse event that triggered the tooltip.
 * @param {string} elementId - A unique identifier for the hovered element (e.g., "link-a-b-rel") used for tracking.
 */
export function showLinkTooltip(tooltip, link, event, elementId) {
  // Update the current hover element
  currentHoverElement = elementId;

  tooltip.transition().duration(200).style("opacity", 0.9);

  // Create a table-like structured tooltip
  let tooltipContent = `<div class="tooltip-title">${link.source.id}.${link.name} → ${link.target.id}</div>`;

  // Create a table structure for the data
  tooltipContent += `<table class="tooltip-table">
    <tr>
      <td class="tooltip-label">Type:</td>
      <td>${link.type}</td>
    </tr>`;

  // Add minimal relationship properties
  if (
    link.type === "simple join" ||
    link.type === "cartesian product" ||
    link.type === "reverse" ||
    link.type === "general join"
  ) {
    tooltipContent += `
    <tr>
      <td class="tooltip-label">Singular:</td>
      <td><span class="${link.data.singular ? "true-value" : "false-value"}">${
      link.data.singular ? "Yes" : "No"
    }</span></td>
    </tr>
    <tr>
      <td class="tooltip-label">Always Matches:</td>
      <td><span class="${link.data["always matches"] ? "true-value" : "false-value"}">${
      link.data["always matches"] ? "Yes" : "No"
    }</span></td>
    `;

    // If it's a simple join, add the keys
    if (link.type === "simple join" && link.data.keys) {
      tooltipContent += `
      <tr>
        <td class="tooltip-label">Keys:</td>
        <td>`
      
      Object.entries(link.data.keys).forEach(([lhsKey, rhsKeys], index) => {
        rhsKeys.forEach((rhsKey) => {
          if (index > 0) { tooltipContent += `<br>`; }
          tooltipContent += `
          self.${lhsKey} = other.${rhsKey}
          `;
        });
      });
      tooltipContent += `</td>
      </tr>`;
    }

    // If it's a general join, add the condition
    if (link.type === "general join" && link.data.condition) {
      tooltipContent += `
      <tr>
        <td class="tooltip-label">Condition:</td>
        <td>${link.data.condition}</td>
      </tr>`;
    }

    // If it's a reverse, add the reverse info
    if (link.type === "reverse") {
      tooltipContent += `
      <tr>
        <td class="tooltip-label">Reverse of:</td>
        <td>${link.data["original parent"]}.${link.data["original property"]}</td>
      </tr>`;
    }
  }

  tooltipContent += `</table>`;

  // Add reverse relationship note at the bottom if needed
  if (link.isReverse) {
    tooltipContent += `<div class="tooltip-hint">(Reverse relationship)</div>`;
  }

  tooltip
    .html(tooltipContent)
    .style("left", event.pageX + 10 + "px")
    .style("top", event.pageY - 28 + "px");
}

/**
 * Clears the global tracking variable for the currently hovered element,
 * but only if the provided elementId matches the currently tracked one.
 * This prevents clearing the hover state if the mouse quickly moves between elements.
 *
 * @param {string} elementId - The unique identifier of the element whose hover state might need clearing.
 */
export function clearHoverElement(elementId) {
  // Only clear if we're still tracking the same element
  if (currentHoverElement === elementId) {
    currentHoverElement = null;
  }
}

/**
 * Hides the tooltip by transitioning its opacity to 0 and disabling pointer events.
 *
 * @param {Object} tooltip - The D3 selection of the tooltip element.
 */
export function hideTooltip(tooltip) {
  tooltip.transition().duration(200).style("opacity", 0);
  tooltip.style("pointer-events", "none");
}

/**
 * Cancels any scheduled tooltip hide operation (clears the pending timeout).
 * This is typically called when the mouse enters the tooltip itself or another graph element.
 */
export function cancelHide() {
  if (pendingHideTimeoutId) {
    clearTimeout(pendingHideTimeoutId);
    pendingHideTimeoutId = null;
  }
}

/**
 * Schedules the tooltip to be hidden after a specified delay.
 * If a hide operation is already scheduled, it is cancelled first.
 * When the hide operation completes, the associated hover element tracking is cleared.
 *
 * @param {Object} tooltip - The D3 selection of the tooltip element.
 * @param {string} elementId - The unique identifier of the element associated with this hide action.
 * @param {number} delay - The delay in milliseconds before hiding the tooltip.
 */
export function scheduleHide(tooltip, elementId, delay) {
  // Clear any existing timer first
  cancelHide();

  pendingHideTimeoutId = setTimeout(() => {
    hideTooltip(tooltip);
    clearHoverElement(elementId); // Clear tracking when hiding completes
    pendingHideTimeoutId = null;
  }, delay);
}

/**
 * Checks if the current mouse event coordinates are within the bounds of the tooltip element.
 *
 * @param {Object} event - The D3 mouse event (containing clientX, clientY).
 * @param {Object} tooltip - The D3 selection of the tooltip element.
 * @returns {boolean} True if the mouse coordinates are inside the tooltip's bounding rectangle, false otherwise.
 */
export function isMouseOverTooltip(event, tooltip) {
  const tooltipNode = tooltip.node();
  if (!tooltipNode) return false;

  const tooltipBounds = tooltipNode.getBoundingClientRect();
  const x = event.clientX;
  const y = event.clientY;

  return (
    x >= tooltipBounds.left &&
    x <= tooltipBounds.right &&
    y >= tooltipBounds.top &&
    y <= tooltipBounds.bottom
  );
}

/**
 * Immediately hides any visible tooltip, cancels pending hide operations,
 * and resets the hover element tracking state.
 * Useful for situations like view changes or graph reloads where tooltips should disappear instantly.
 */
export function forceHideTooltips() {
  // Reset tracking variables
  currentHoverElement = null;

  // Clear any pending timeouts
  if (pendingHideTimeoutId) {
    clearTimeout(pendingHideTimeoutId);
    pendingHideTimeoutId = null;
  }

  // Hide the tooltip immediately
  const tooltip = d3.select("body").select(".tooltip");
  if (!tooltip.empty()) {
    tooltip.transition().duration(0).style("opacity", 0);
  }
}

/**
 * Checks if the mouse event coordinates are over the small tooltip indicator
 * circle rendered in the bottom-right corner of a node.
 *
 * @param {Object} event - The D3 mouse event (containing clientX, clientY).
 * @param {Object} node - The data object for the node whose indicator is being checked.
 * @returns {boolean} True if the mouse is within the indicator's bounds (plus a small buffer), false otherwise.
 */
export function isMouseInBottomRightQuadrant(event, node) {
  // Get the node element
  const nodeElement = document.getElementById(`node-${node.id}`);
  if (!nodeElement) return false;

  // Find the tooltip indicator circle within the node
  const indicator = nodeElement.querySelector(".tooltip-indicator");
  if (!indicator) return false;

  // Get indicator bounds
  const indicatorBounds = indicator.getBoundingClientRect();

  // Calculate distance from mouse to center of indicator
  const dx = event.clientX - (indicatorBounds.left + indicatorBounds.width / 2);
  const dy = event.clientY - (indicatorBounds.top + indicatorBounds.height / 2);
  const distance = Math.sqrt(dx * dx + dy * dy);

  // Check if mouse is within the indicator circle (with a small buffer)
  // The indicator has radius 8, we add a few pixels buffer for easier interaction
  return distance <= 12;
}
