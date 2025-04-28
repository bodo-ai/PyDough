// Node Renderer - Renders node elements (collection boxes)

/**
 * Create nodes for the graph
 * @param {Object} g - The SVG group element
 * @param {Array} nodes - The array of nodes
 * @returns {Object} The D3 selection of nodes
 */
export function createNodes(g, nodes) {
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
    .attr("height", (d) => d.height)
    .attr("rx", 10)
    .attr("ry", 10);

  // Add collection titles
  node
    .append("text")
    .attr("x", 20)
    .attr("y", 35)
    .attr("class", "node-title")
    .style("font-size", "18px")
    .style("font-weight", "bold")
    .text((d) => d.name);

  // Add table path
  node
    .append("text")
    .attr("x", 20)
    .attr("y", 60)
    .attr("class", "table-path")
    .style("font-size", "13px")
    .text((d) => d.tablePath);

  // Add dividing line
  node
    .append("line")
    .attr("x1", 20)
    .attr("y1", 70)
    .attr("x2", (d) => d.width - 20)
    .attr("y2", 70)
    .attr("stroke", "#333")
    .attr("stroke-width", 1);

  // Add properties header
  addPropertiesHeader(node);

  // Add columns
  addColumnsList(node);

  // Add subcollections header and list
  addSubcollectionsList(node);

  return node;
}

/**
 * Add the properties header to nodes
 * @param {Object} node - The D3 selection of nodes
 */
function addPropertiesHeader(node) {
  node
    .append("text")
    .attr("x", 20)
    .attr("y", 90)
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
    .attr("x", 30)
    .attr("y", (d, i) => 110 + i * 22)
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
    .attr("x", 20)
    .attr("y", (d) => 110 + d.columns.length * 22)
    .attr("class", "properties-header")
    .style("font-size", "15px")
    .style("font-weight", "bold")
    .text("Subcollections:");

  // Add subcollection items
  node
    .selectAll(".subcollection-item")
    .data((d) => d.subcollections.map((subcol) => ({ subcol, parent: d })))
    .enter()
    .append("text")
    .attr("x", 30)
    .attr("y", (d, i, nodes) => {
      const parent = d.parent;
      return 130 + parent.columns.length * 22 + i * 22;
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
      // More intuitive display of relationships
      if (d.subcol.isReverse) {
        return `${d.subcol.name} ← ${d.subcol.target}`;
      } else {
        return `${d.subcol.name} → ${d.subcol.target}`;
      }
    });
}
