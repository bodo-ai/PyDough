// Graph Initializer - Handles loading multiple JSON graph data files and switching between them.
import { createGraph } from "./graphCreator.js";

// Store loaded graphs (filename -> data)
const loadedGraphs = new Map();
// Track the name of the currently displayed graph
let currentGraphName = null;

/**
 * Initialize the graph visualization: setup file upload, hamburger menu, and display initial prompt.
 */
export function initGraphVisualization() {
  setupFileUpload();
  setupHamburgerMenu();
  displayLoadPrompt(); // Display initial prompt
}

/**
 * Setup file upload button: Handles file selection, JSON parsing, storing graph data,
 * updating the dropdown, and displaying the newly loaded graph.
 */
function setupFileUpload() {
  const fileInput = document.getElementById("jsonFileInput");
  const loadButton = document.getElementById("loadJsonButton");

  loadButton.addEventListener("click", () => {
    fileInput.click();
  });

  fileInput.addEventListener("change", (event) => {
    const file = event.target.files[0];
    if (file) {
      if (file.type === "application/json" || file.name.endsWith(".json")) {
        const reader = new FileReader();

        reader.onload = (e) => {
          try {
            const data = JSON.parse(e.target.result);
            const graphName = file.name;
            console.log(
              `JSON file '${graphName}' loaded and parsed successfully.`
            );

            // Store the graph data
            loadedGraphs.set(graphName, data);

            // Update the dropdown list
            updateGraphListDropdown();

            // Display the newly loaded graph
            displayGraph(graphName);

            // Reset file input to allow loading the same file again if needed
            fileInput.value = "";
          } catch (error) {
            console.error("Error parsing JSON file:", error);
            alert(
              "Error parsing JSON file. Please check that it's valid JSON."
            );
          }
        };

        reader.onerror = () => {
          console.error("Error reading file");
          alert("Error reading file. Please try again.");
        };

        reader.readAsText(file);
      } else {
        alert("Please select a JSON file.");
      }
    }
  });
}

/**
 * Setup hamburger menu interaction: Toggles dropdown visibility on button click
 * and closes dropdown when clicking outside.
 */
function setupHamburgerMenu() {
  const hamburgerBtn = document.getElementById("hamburger-btn");
  const dropdown = document.getElementById("graph-list-dropdown");

  hamburgerBtn.addEventListener("click", (event) => {
    event.stopPropagation(); // Prevent click from immediately closing the menu
    dropdown.classList.toggle("show");
  });

  // Close the dropdown if the user clicks outside of it
  window.addEventListener("click", (event) => {
    if (
      !dropdown.contains(event.target) &&
      !hamburgerBtn.contains(event.target)
    ) {
      if (dropdown.classList.contains("show")) {
        dropdown.classList.remove("show");
      }
    }
  });
}

/**
 * Update the graph list in the hamburger dropdown menu.
 * Populates the list with names of loaded graphs and handles click events to switch graphs.
 */
function updateGraphListDropdown() {
  const graphList = document.getElementById("graph-list");
  const dropdown = document.getElementById("graph-list-dropdown");

  // Clear existing list items
  graphList.innerHTML = "";

  if (loadedGraphs.size === 0) {
    const li = document.createElement("li");
    li.textContent = "No graphs loaded";
    li.style.fontStyle = "italic";
    li.style.color = "#888";
    graphList.appendChild(li);
    return;
  }

  // Add newly loaded graphs
  loadedGraphs.forEach((data, graphName) => {
    const li = document.createElement("li");
    li.textContent = graphName;
    if (graphName === currentGraphName) {
      li.style.fontWeight = "bold"; // Highlight current graph
    }
    li.addEventListener("click", () => {
      displayGraph(graphName);
      dropdown.classList.remove("show"); // Close dropdown after selection
    });
    graphList.appendChild(li);
  });
}

/**
 * Display the selected graph.
 * Clears the container (via createGraph) and renders the graph corresponding to the given name.
 * @param {string} graphName - The name (key) of the graph to display in the loadedGraphs map.
 */
function displayGraph(graphName) {
  if (!loadedGraphs.has(graphName)) {
    console.error(`Graph '${graphName}' not found.`);
    return;
  }

  console.log(`Displaying graph: ${graphName}`);
  currentGraphName = graphName;
  const data = loadedGraphs.get(graphName);

  // Create the new graph (createGraph clears the container internally)
  createGraph(data);

  // Update the dropdown to potentially highlight the new current graph
  updateGraphListDropdown();
}

/**
 * Display initial prompt message in the graph container when no graph is loaded.
 */
function displayLoadPrompt() {
  const container = document.getElementById("graph");
  if (!container) {
    console.error("Graph container not found!");
    return;
  }

  // Clear any existing content (like a previous graph)
  d3.select(container).selectAll("*").remove();

  const width = container.clientWidth || 1000;
  const height = container.clientHeight || 800;

  const svg = d3
    .select("#graph")
    .append("svg")
    .attr("width", width)
    .attr("height", height);

  // Add a message prompting the user to load a file
  svg
    .append("text")
    .attr("x", width / 2)
    .attr("y", height / 2 - 20)
    .attr("text-anchor", "middle")
    .style("font-size", "24px")
    .style("fill", "#555")
    .text("No graph loaded");

  svg
    .append("text")
    .attr("x", width / 2)
    .attr("y", height / 2 + 20)
    .attr("text-anchor", "middle")
    .style("font-size", "18px")
    .style("fill", "#777")
    .text("Click 'Load JSON File' to visualize your graph data");
}
