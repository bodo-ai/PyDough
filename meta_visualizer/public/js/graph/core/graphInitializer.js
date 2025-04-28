// Graph Initializer - Handles fetching data and initializing the graph
import { createGraph } from "./graphCreator.js";

/**
 * Initialize the graph visualization by setting up file upload functionality
 */
export function initGraphVisualization() {
  // Set up the file upload button functionality
  setupFileUpload();

  // Display initial prompt to load a file
  displayLoadPrompt();
}

/**
 * Setup file upload button and file input handling
 */
function setupFileUpload() {
  const fileInput = document.getElementById("jsonFileInput");
  const loadButton = document.getElementById("loadJsonButton");

  // Trigger file input when button is clicked
  loadButton.addEventListener("click", () => {
    fileInput.click();
  });

  // Handle file selection
  fileInput.addEventListener("change", (event) => {
    const file = event.target.files[0];
    if (file) {
      // Check if file is JSON
      if (file.type === "application/json" || file.name.endsWith(".json")) {
        const reader = new FileReader();

        reader.onload = (e) => {
          try {
            const data = JSON.parse(e.target.result);
            console.log("JSON file loaded, parsing successful");

            // Create new graph with uploaded data
            createGraph(data);
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
 * Display initial prompt to load a file
 */
function displayLoadPrompt() {
  const container = document.getElementById("graph");
  if (!container) {
    console.error("Graph container not found!");
    return;
  }

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
