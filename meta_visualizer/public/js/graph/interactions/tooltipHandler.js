// Tooltip Handler - Manages tooltip display on hover over nodes and links

/**
 * Create a tooltip div for the graph
 * @returns {Object} The D3 selection of the tooltip
 */
export function createTooltip() {
  return d3
    .select("body")
    .append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);
}

/**
 * Show a tooltip with node information
 * @param {Object} tooltip - The tooltip element
 * @param {Object} node - The node data
 * @param {Object} event - The mouse event
 */
export function showNodeTooltip(tooltip, node, event) {
  tooltip.transition().duration(200).style("opacity", 0.9);

  let tooltipContent = `<strong>${node.name}</strong><br>`;
  tooltipContent += `Path: ${node.tablePath}<br>`;
  tooltipContent += `Unique by: ${
    Array.isArray(node.uniqueProperties[0])
      ? node.uniqueProperties
          .map((arr) => "[" + arr.join(", ") + "]")
          .join(" or ")
      : node.uniqueProperties.join(", ")
  }<br>`;

  // Add list of columns
  if (node.columns.length > 0) {
    tooltipContent += `<br><strong>Table Columns:</strong><br>`;
    node.columns.forEach((col) => {
      tooltipContent += `- ${col.name} (${col.dataType})<br>`;
    });
  }

  // Add list of subcollections with clearer direction indicators
  if (node.subcollections.length > 0) {
    tooltipContent += `<br><strong>Relationships:</strong><br>`;
    const forwardSubs = node.subcollections.filter((sub) => !sub.isReverse);
    const reverseSubs = node.subcollections.filter((sub) => sub.isReverse);

    if (forwardSubs.length > 0) {
      tooltipContent += `<em>Parent → Child:</em><br>`;
      forwardSubs.forEach((sub) => {
        tooltipContent += `- ${sub.name} (${sub.type} → ${sub.target})<br>`;
        tooltipContent += `&nbsp;&nbsp;Singular: ${
          sub.singular ? "Yes" : "No"
        }, No Collisions: ${sub.noCollisions ? "Yes" : "No"}<br>`;
      });
    }

    if (reverseSubs.length > 0) {
      tooltipContent += `<em>Child → Parent:</em><br>`;
      reverseSubs.forEach((sub) => {
        tooltipContent += `- ${sub.name} (${sub.type} ← from ${sub.target})<br>`;
        tooltipContent += `&nbsp;&nbsp;Singular: ${
          sub.singular ? "Yes" : "No"
        }, No Collisions: ${sub.noCollisions ? "Yes" : "No"}<br>`;
      });
    }
  }

  tooltip
    .html(tooltipContent)
    .style("left", event.pageX + 10 + "px")
    .style("top", event.pageY - 28 + "px");
}

/**
 * Show a tooltip with link information
 * @param {Object} tooltip - The tooltip element
 * @param {Object} link - The link data
 * @param {Object} event - The mouse event
 */
export function showLinkTooltip(tooltip, link, event) {
  tooltip.transition().duration(200).style("opacity", 0.9);

  // Enhanced tooltip with more detailed relationship info
  let tooltipContent = `<strong>${link.source.id}.${link.name} → ${link.target.id}</strong><br>`;
  tooltipContent += `<span class="relationship-type">Type: ${link.type}</span><br>`;

  if (link.isReverse) {
    tooltipContent += `<div class="reverse-relationship">
      <span class="reverse-note">This is a reverse relationship of:<br>
      <strong>${link.originalCollection || link.target.id}.${
      link.originalRelationship || "unknown"
    }</strong></span>
    </div>`;
  }

  if (link.type === "simple_join") {
    tooltipContent += `<div class="relationship-details">`;
    tooltipContent += `Singular: <span class="${
      link.singular ? "true-value" : "false-value"
    }">${link.singular ? "Yes" : "No"}</span><br>`;
    tooltipContent += `No Collisions: <span class="${
      link.noCollisions ? "true-value" : "false-value"
    }">${link.noCollisions ? "Yes" : "No"}</span><br>`;

    if (!link.isReverse && link.propertyData) {
      tooltipContent += `Keys: <span class="keys-value">${JSON.stringify(
        link.propertyData.keys || []
      )}</span><br>`;
    }
    tooltipContent += `</div>`;
  } else if (link.type === "compound") {
    tooltipContent += `<div class="relationship-details">`;
    if (!link.isReverse) {
      tooltipContent += `Primary: <span class="primary-value">${
        link.primary || "N/A"
      }</span><br>`;
      tooltipContent += `Secondary: <span class="secondary-value">${
        link.secondary || "N/A"
      }</span><br>`;
      if (link.propertyData && link.propertyData.inherited_properties) {
        tooltipContent += `Inherited Properties: <span class="inherited-value">${JSON.stringify(
          link.propertyData.inherited_properties
        )}</span><br>`;
      }
    }
    tooltipContent += `Singular: <span class="${
      link.singular ? "true-value" : "false-value"
    }">${link.singular ? "Yes" : "No"}</span><br>`;
    tooltipContent += `No Collisions: <span class="${
      link.noCollisions ? "true-value" : "false-value"
    }">${link.noCollisions ? "Yes" : "No"}</span><br>`;
    tooltipContent += `</div>`;
  }

  tooltip
    .html(tooltipContent)
    .style("left", event.pageX + 10 + "px")
    .style("top", event.pageY - 28 + "px");
}

/**
 * Hide the tooltip
 * @param {Object} tooltip - The tooltip element
 */
export function hideTooltip(tooltip) {
  tooltip.transition().duration(500).style("opacity", 0);
}
