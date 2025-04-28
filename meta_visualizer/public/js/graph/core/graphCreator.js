// Graph Creator - Orchestrates the creation of the graph visualization
// This module is the central coordinator that brings together all visualization components

import { processMetadata } from "./dataProcessor.js";
import { setupMarkers } from "../renderers/markerRenderer.js";
import { createNodes } from "../renderers/nodeRenderer.js";
import { createLinks } from "../renderers/linkRenderer.js";
import { setupZoom, createZoomControls } from "../interactions/zoomHandler.js";
import { setupDrag } from "../interactions/dragHandler.js";
import { setupHighlighting } from "../interactions/highlightHandler.js";
import { createTooltip } from "../interactions/tooltipHandler.js";
import { setupForceSimulation } from "../layouts/forceLayout.js";
import { applyGraphStyles } from "../styles/graphStyles.js";

/**
 * Create the graph visualization from metadata
 *
 * This is the main orchestration function that:
 * 1. Processes the raw metadata into nodes and links
 * 2. Sets up the SVG container and visualization groups
 * 3. Initializes interactive components (zoom, drag, tooltips)
 * 4. Creates visual elements (nodes, links, markers)
 * 5. Sets up the force simulation for automatic layout
 * 6. Configures event handlers for interactivity
 *
 * @param {Object} data - The metadata from the JSON file containing database schema
 */
export function createGraph(data) {
  console.log("Creating graph visualization...");

  // Process the metadata into D3-compatible node and link structures
  const { nodes, links, graphName } = processMetadata(data);

  // Get the dimensions of the container for proper sizing
  const container = document.getElementById("graph");
  if (!container) {
    console.error("Graph container not found!");
    return;
  }

  // Clear any existing visualization
  container.innerHTML = "";

  const width = container.clientWidth || 1000;
  const height = container.clientHeight || 800;
  console.log("Container dimensions:", width, height);

  // Create the main SVG container that will hold all visualization elements
  const svg = d3
    .select("#graph")
    .append("svg")
    .attr("width", width)
    .attr("height", height);

  // Validate the graph data before proceeding
  if (!graphName) {
    handleNoData(svg, width, height, "No metadata found");
    return;
  }

  const collections = data[graphName] || {};
  // Ensure there are collections to display
  if (Object.keys(collections).length === 0) {
    handleNoData(svg, width, height, "No collections found in metadata");
    return;
  }

  // Create a tooltip element for displaying additional information on hover
  const tooltip = createTooltip();

  // Create a group element for coordinated zoom/pan operations
  // All visualization elements will be added to this group for unified transformation
  const g = svg.append("g");

  // Initialize arrow markers for link endpoints
  // These define the different arrowheads based on relationship types
  setupMarkers(svg);

  // Configure zoom and pan behavior for the visualization
  setupZoom(svg, g);

  // Add zoom control buttons and instructions to the visualization
  createZoomControls(svg, width, height);

  // Initialize the force-directed simulation that will position nodes
  // Force simulation uses physics-based forces to arrange nodes and links
  const simulation = setupForceSimulation(nodes, links, width, height);

  // Render the links (relationships) between collections
  // These are SVG paths with directional markers
  const link = createLinks(g, links);

  // Render the nodes (collections) with their properties
  // Each node is a complex SVG group with multiple elements
  const node = createNodes(g, nodes);

  // Make nodes draggable to allow manual positioning
  // This enhances the user experience when exploring the graph
  setupDrag(node, simulation);

  // Configure highlight behavior for connected nodes and links
  // This helps visualize relationships when hovering over elements
  const { highlightConnections, resetHighlighting, createTreeLayout } =
    setupHighlighting(
      node,
      link,
      nodes,
      links,
      tooltip,
      simulation,
      svg,
      width,
      height
    );

  // State variables to track interactive modes
  let activeNode = null; // Currently selected node for tree view
  let isTreeView = false; // Whether we're in tree view mode

  // Configure interactive behavior for nodes (hover and click events)
  node
    .on("mouseover", (event, d) => {
      // Only highlight connections if not in tree view mode
      if (!isTreeView) {
        highlightConnections(event, d);
      }
    })
    .on("mouseout", (event, d) => {
      // Reset highlighting when mouse leaves node
      if (!isTreeView) {
        resetHighlighting();
      }
    })
    .on("click", (event, d) => {
      // Toggle between force layout and tree layout views
      // If clicking the same node again, return to force layout
      if (activeNode === d.id) {
        isTreeView = false;
        activeNode = null;
        resetHighlighting();

        // Release fixed positions to allow force simulation to resume
        nodes.forEach((node) => {
          node.fx = null; // Clear fixed x-coordinate
          node.fy = null; // Clear fixed y-coordinate
          node.treePosition = null; // Clear tree positioning data
          node.treeLevel = null;
          node.treeParent = null;
        });

        // Restart the force simulation with high alpha for movement
        simulation.alpha(1).restart();
      } else {
        // Switch to tree view centered on the clicked node
        isTreeView = true;
        activeNode = d.id;

        // Arrange nodes in a hierarchical tree layout
        createTreeLayout(d);
      }
    });

  // Update visual elements on each simulation tick
  // This is the animation loop that moves nodes and links
  simulation.on("tick", () => {
    // Update link paths to follow their connected nodes
    link.selectAll("path").attr("d", calculateLinkPath(nodes));

    // Update node positions based on simulation
    node.attr("transform", (d) => `translate(${d.x},${d.y})`);
  });

  // Start the simulation if we have nodes to display
  if (nodes.length > 0) {
    console.log("Starting force simulation");
    simulation.alpha(1).restart(); // High alpha means more movement
  } else {
    handleNoData(svg, width, height, "No nodes found to display");
  }

  // Apply CSS styling to the visualization elements
  applyGraphStyles();
}

/**
 * Calculate the path between nodes with accurate edge connection points
 *
 * This function determines where a link should connect to node rectangles.
 * It calculates the exact intersection points with the node boundaries,
 * creating visually accurate connections with proper arrow placement.
 *
 * @param {Array} nodes - The array of all node objects
 * @returns {Function} A function that calculates the path for a specific link
 */
function calculateLinkPath(nodes) {
  return (d) => {
    // Find the source and target node objects for this link
    const sourceNode = nodes.find((n) => n.id === d.source.id);
    const targetNode = nodes.find((n) => n.id === d.target.id);

    if (!sourceNode || !targetNode) return "";

    // Get node centers as starting reference points
    const sourceX = sourceNode.x + sourceNode.width / 2;
    const sourceY = sourceNode.y + sourceNode.height / 2;
    const targetX = targetNode.x + targetNode.width / 2;
    const targetY = targetNode.y + targetNode.height / 2;

    // Calculate the angle between source and target centers
    const angle = Math.atan2(targetY - sourceY, targetX - sourceX);

    // Calculate source intersection point with the node boundary
    // This involves determining which edge of the rectangle the line intersects
    let sx = sourceX,
      sy = sourceY;
    if (Math.abs(Math.cos(angle)) > Math.abs(Math.sin(angle))) {
      // Intersect with left or right edge
      sx = sourceNode.x + (Math.cos(angle) > 0 ? sourceNode.width : 0);
      sy =
        sourceNode.y +
        sourceNode.height / 2 +
        Math.tan(angle) * (sx - sourceNode.x - sourceNode.width / 2);
    } else {
      // Intersect with top or bottom edge
      sy = sourceNode.y + (Math.sin(angle) > 0 ? sourceNode.height : 0);
      sx =
        sourceNode.x +
        sourceNode.width / 2 +
        (sy - sourceNode.y - sourceNode.height / 2) / Math.tan(angle);
    }

    // Calculate target intersection point with the node boundary
    // Uses the same approach but in reverse direction
    let tx = targetX,
      ty = targetY;
    const revAngle = Math.atan2(sourceY - targetY, sourceX - targetX);
    if (Math.abs(Math.cos(revAngle)) > Math.abs(Math.sin(revAngle))) {
      // Intersect with left or right edge
      tx = targetNode.x + (Math.cos(revAngle) > 0 ? targetNode.width : 0);
      ty =
        targetNode.y +
        targetNode.height / 2 +
        Math.tan(revAngle) * (tx - targetNode.x - targetNode.width / 2);
    } else {
      // Intersect with top or bottom edge
      ty = targetNode.y + (Math.sin(revAngle) > 0 ? targetNode.height : 0);
      tx =
        targetNode.x +
        targetNode.width / 2 +
        (ty - targetNode.y - targetNode.height / 2) / Math.tan(revAngle);
    }

    // Return SVG path string connecting the two intersection points
    return `M${sx},${sy}L${tx},${ty}`;
  };
}

/**
 * Handle empty or invalid data scenarios with a user-friendly message
 *
 * Displays an informative message in the center of the SVG when no
 * visualization data is available or when errors occur.
 *
 * @param {Object} svg - The SVG element where the message will be displayed
 * @param {number} width - The width of the SVG
 * @param {number} height - The height of the SVG
 * @param {string} message - The message to display to the user
 */
function handleNoData(svg, width, height, message) {
  console.warn(message);
  svg
    .append("text")
    .attr("x", width / 2)
    .attr("y", height / 2)
    .attr("text-anchor", "middle")
    .style("font-size", "20px")
    .text(message);
}
