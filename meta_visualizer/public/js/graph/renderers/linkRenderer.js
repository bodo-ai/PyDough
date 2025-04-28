// Link Renderer - Renders links between nodes

/**
 * Create links between nodes
 * @param {Object} g - The SVG group element
 * @param {Array} links - The array of links
 * @returns {Object} The D3 selection of links
 */
export function createLinks(g, links) {
  // Create the links container
  const link = g
    .selectAll(".link")
    .data(links)
    .enter()
    .append("g")
    .attr(
      "class",
      (d) => `link-group link-${d.type} ${d.isReverse ? "link-reverse" : ""}`
    );

  // Add the actual path for each link
  link
    .append("path")
    .attr(
      "class",
      (d) => `link link-${d.type} ${d.isReverse ? "link-reverse" : ""}`
    )
    .attr("marker-end", (d) => `url(#arrowhead-${d.type})`)
    .attr("id", (d, i) => `link-${i}`);

  // Add labels to the links showing the property name
  link
    .append("text")
    .attr(
      "class",
      (d) => `link-label ${d.isReverse ? "link-label-reverse" : ""}`
    )
    .attr("dy", -20)
    .style("font-size", "16px")
    .style("font-weight", "bold")
    .append("textPath")
    .attr("xlink:href", (d, i) => `#link-${i}`)
    .attr("startOffset", "50%")
    .attr("text-anchor", "middle")
    .text((d) => d.name);

  return link;
}
