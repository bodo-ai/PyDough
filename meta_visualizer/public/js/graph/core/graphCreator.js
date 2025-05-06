// Graph Creator - Orchestrates the creation of the graph visualization
// This module is the central coordinator that brings together all visualization components

import { processMetadata } from "./dataProcessor.js";
import { setupMarkers } from "../renderers/markerRenderer.js";
import { createNodes } from "../renderers/nodeRenderer.js";
import { createLinks } from "../renderers/linkRenderer.js";
import { setupZoom, createZoomControls } from "../interactions/zoomHandler.js";
import { setupDrag } from "../interactions/dragHandler.js";
import { setupHighlighting } from "../interactions/highlightHandler.js";
import {
  createTooltip,
  showNodeTooltip,
  hideTooltip,
  isMouseOverTooltip,
  clearHoverElement,
  forceHideTooltips,
  scheduleHide,
  cancelHide,
  isMouseInBottomRightQuadrant,
} from "../interactions/tooltipHandler.js";
import { setupForceSimulation } from "../layouts/forceLayout.js";
import { applyGraphStyles } from "../styles/graphStyles.js";
import { calculateLinkPath } from "../utils/geometryUtils.js";

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

  // Debug logging
  console.log("Processed metadata:");
  console.log("Number of nodes:", nodes.length);
  console.log("Number of links:", links.length);
  console.log("Links:", links);

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
  const link = createLinks(g, links, nodes);

  // Debug logging for link creation
  console.log("Created link elements:", link.size());

  // Render the nodes (collections) with their properties
  // Each node is a complex SVG group with multiple elements
  const tooltipHandlers = {
    tooltip: tooltip,
    showNodeTooltip,
    hideTooltip,
    isMouseOverTooltip,
    clearHoverElement,
    scheduleHide,
    cancelHide,
  };
  const node = createNodes(g, nodes, tooltipHandlers);

  // State variables to track interactive modes
  let activeNode = null; // Currently selected node for tree view
  let isTreeView = false; // Whether we're in tree view mode

  // Create a state object to share with drag handler
  const state = {
    isTreeView,
    // Track if we've already added tree view tooltips
    treeViewTooltipsAdded: false,
    // Callback to exit tree view mode while keeping current positions
    onExitTreeView: () => {
      console.log("Exiting tree view mode due to node drag");
      // Force hide any visible tooltips
      forceHideTooltips();

      isTreeView = false;
      activeNode = null;

      // Don't reset node positions since we want to keep the layout
      // Mark all nodes as not part of the tree anymore
      nodes.forEach((node) => {
        // Keep fx/fy positions but clear tree metadata
        node.treePosition = null;
        node.treeLevel = null;
        node.treeParent = null;
      });

      // Remove tree-specific highlighting
      node
        .classed("node-highlighted", false)
        .classed("node-primary", false)
        .classed("node-depth-1", false)
        .classed("node-depth-2", false)
        .classed("node-faded", false);

      // Reset link styling
      link.each(function () {
        d3.select(this)
          .select("path")
          .classed("link-highlighted", false)
          .attr("marker-end", (d) => `url(#arrowhead-${d.type})`);

        d3.select(this)
          .select("text")
          .classed("link-label-highlighted", false)
          .attr("dy", 20)
          .style("font-size", "16px")
          .style("letter-spacing", "0.5px");
      });

      // Update the state for drag handler
      updateDragBehavior();
    },
  };

  // Make nodes draggable to allow manual positioning
  // This enhances the user experience when exploring the graph
  setupDrag(node, simulation, state);

  // Function to update drag behavior when tree view state changes
  const updateDragBehavior = () => {
    // Update the shared state object
    state.isTreeView = isTreeView;
  };

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

  // Configure interactive behavior for nodes (hover and click events)
  node
    .on("mouseover", (event, d) => {
      // Highlight connections if not in tree view mode
      if (!isTreeView) {
        highlightConnections(event, d);
      }
    })
    .on("mouseout", (event, d) => {
      const nodeId = `node-${d.id}`;

      // Always clear hover element tracking when leaving node
      // But only if we're not hovering over the tooltip indicator or tooltip
      const isOnTooltipIndicator =
        event.relatedTarget &&
        (event.relatedTarget.classList.contains("tooltip-indicator") ||
          event.relatedTarget.parentNode.classList.contains(
            "tooltip-indicator-group"
          ));

      // Use the tooltip element we have in scope
      const isOnTooltip = isMouseOverTooltip(event, tooltip);

      if (!isOnTooltipIndicator && !isOnTooltip) {
        clearHoverElement(nodeId);
      }

      // Reset highlighting when mouse leaves node
      if (!isTreeView) {
        resetHighlighting();
      }
    })
    .on("click", (event, d) => {
      // If already in tree view mode and clicking a different node
      if (isTreeView && activeNode !== d.id) {
        // Force hide any visible tooltips
        forceHideTooltips();

        // First reset the existing tree layout
        resetTreeLayout(false); // Keep fixed positions when changing root node

        // Make the clicked node the new root for tree view
        activeNode = d.id;
        createTreeLayout(d);
        return;
      }

      // Toggle between force layout and tree layout views
      // If clicking the same node again, return to force layout
      if (activeNode === d.id) {
        // Force hide any visible tooltips
        forceHideTooltips();

        isTreeView = false;
        activeNode = null;
        resetHighlighting();

        // Release fixed positions to allow force simulation to resume
        resetTreeLayout(true);

        // Update drag behavior for new state
        updateDragBehavior();

        // Restart the force simulation with high alpha for movement
        simulation.alpha(1).restart();
      } else {
        // Force hide any visible tooltips
        forceHideTooltips();

        // Switch to tree view centered on the clicked node
        isTreeView = true;
        activeNode = d.id;

        // Update drag behavior for new state
        updateDragBehavior();

        // Arrange nodes in a hierarchical tree layout
        createTreeLayout(d);

        // Ensure we have tooltip functionality on all nodes in tree view
        if (!state.treeViewTooltipsAdded) {
          state.treeViewTooltipsAdded = true;

          // Clear any previous event handlers if they exist
          svg
            .selectAll(".node")
            .on("mouseover.tooltip", null)
            .on("mouseout.tooltip", null);
        }
      }
    });

  // Helper function to reset the tree layout
  function resetTreeLayout(clearFixedPositions = true) {
    // Release fixed positions to allow force simulation to resume
    nodes.forEach((node) => {
      if (clearFixedPositions) {
        node.fx = null;
        node.fy = null;
      }
      node.treePosition = null; // Clear tree positioning data
      node.treeLevel = null;
      node.treeParent = null;
    });
  }

  // Update visual elements on each simulation tick
  // This is the animation loop that moves nodes and links
  simulation.on("tick", () => {
    // Update link paths to follow their connected nodes
    link.each(function (d) {
      // Find the source and target node objects for this link
      // Needed here because the 'd' object only holds IDs initially
      const sourceNode = nodes.find((n) => n.id === d.source.id);
      const targetNode = nodes.find((n) => n.id === d.target.id);
      const paths = calculateLinkPath(sourceNode, targetNode); // Use the utility function
      // Update path
      d3.select(this).select("path").attr("d", paths.forward);
    });

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
