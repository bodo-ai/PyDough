// Graph Styles - CSS styles for the graph components

/**
 * Apply CSS styles to the graph
 */
export function applyGraphStyles() {
  const style = document.createElement("style");
  style.textContent = `
    /* Base font */
    #graph {
      font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, Oxygen, Ubuntu, sans-serif;
    }
    
    .node rect {
      stroke: #2c3e50;
      stroke-width: 2px;
      fill: #ecf0f1;
      transition: stroke 0.3s ease, stroke-width 0.3s ease, fill 0.3s ease;
    }
    
    .node-highlighted rect {
      stroke: #27ae60; /* Green highlight */
      stroke-width: 1px;
      fill: #e8f8f5; /* Lighter green fill */
    }
    
    .node-primary rect {
      stroke: #1e8449; /* Darker green primary highlight */
      stroke-width: 1px;
      fill: #d1f2eb; /* Slightly darker green fill */
    }
    
    /* Base text styling for all node text */
    .node text {
      font-size: 14px;
      letter-spacing: 0.3px;
    }
    
    /* Highlighted node text */
    .node-highlighted text, .node-primary text {
      fill: #000; /* Almost black */
      letter-spacing: 0.8px; /* Increased letter spacing */
    }
    
    /* Node titles */
    .node-title {
      font-size: 18px;
      font-weight: bold;
      fill: #2c3e50;
    }
    
    .node-highlighted .node-title, .node-primary .node-title {
      fill: #111; /* Almost black */
      font-size: 22px;
      letter-spacing: 1px; /* More spacing for titles */
    }
    
    .node-primary .node-title {
      font-size: 24px;
      letter-spacing: 1.2px;
    }
    
    /* Headers for properties sections */
    .properties-header {
      font-size: 16px;
      font-weight: bold;
      fill: #34495e;
    }
    
    .node-highlighted .properties-header, .node-primary .properties-header {
      fill: #111; /* Almost black */
      font-size: 18px;
      letter-spacing: 0.8px;
    }
    
    /* Subcollection items */
    .subcollection-item {
      font-size: 14px;
    }
    
    .node-highlighted .subcollection-item, .node-primary .subcollection-item {
      font-size: 16px;
      letter-spacing: 0.8px;
      fill: #222; /* Dark but not full black */
    }
    
    /* Column items */
    .column-item {
      font-size: 14px;
    }
    
    .node-highlighted .column-item, .node-primary .column-item {
      font-size: 16px;
      letter-spacing: 0.7px;
      fill: #222; /* Dark but not full black */
    }
    
    /* Links styling */
    .link {
      stroke-width: 1.8px;
      fill: none;
      transition: stroke-width 0.3s ease, stroke 0.3s ease;
    }
    
    .link-forward {
      stroke: #1f77b4; /* Blue color for forward links */
      stroke-width: 2px;
      stroke-dasharray: none;
    }
    
    .link-reverse {
      stroke: #ff7f0e; /* Orange color for reverse links */
      stroke-width: 2px;
      stroke-dasharray: 4, 4; /* Dashed line for reverse links */
    }
    
    /* Add style for cartesian_product links */
    .link.link-cartesian_product {
      stroke: #8e44ad; /* Purple */
      stroke-width: 2px;
      stroke-dasharray: none; /* Usually solid */
    }
    
    /* Add style for general_join links */
    .link.link-general_join {
      stroke: #ff7f0e; /* Orange color */
      stroke-width: 2px;
      stroke-dasharray: none;
    }
    
    .link-highlighted {
      stroke: #1e8449; /* Darker green highlight */
      stroke-width: 3px;
    }
    
    .arrowhead {
      fill: #7f8c8d;
      transition: fill 0.3s ease;
    }
    
    /* Style for cartesian_product arrowhead */
    .arrowhead-cartesian_product {
      fill: #8e44ad; /* Purple */
    }
    
    /* Style for general_join arrowhead */
    .arrowhead-general_join {
      fill: #ff7f0e; /* Orange color */
    }
    
    .arrowhead-highlighted {
      fill: #1e8449; /* Darker green matching stroke */
    }
    
    /* Link labels */
    .link-label {
      font-weight: bold;
      font-size: 16px;
      letter-spacing: 0.5px;
      transition: fill 0.3s ease, font-size 0.3s ease;
    }
    
    .link-label-forward {
      fill: #1f77b4; /* Blue color matching forward links */
    }
    
    .link-label-reverse {
      fill: #ff7f0e; /* Orange color matching reverse links */
    }
    
    .link-label-highlighted {
      fill: #145a32; /* Very dark green */
      font-size: 26px;
      letter-spacing: 1.2px; /* Increased letter spacing */
    }
    
    /* Relationship type colors */
    .subcollection-simple_join {
      fill: #16a085;
    }
    
    .subcollection-compound {
      fill: #8e44ad;
    }
    
    .subcollection-cartesian_product {
      fill: #2980b9;
    }
    
    .subcollection-reverse {
      font-style: italic;
    }
    
    /* Faded nodes */
    .node-faded rect {
      opacity: 0.3;
    }
    
    .node-faded text {
      opacity: 0.3;
    }
    
    /* Tooltip styling */
    .tooltip {
      position: absolute;
      padding: 10px;
      background: rgba(255, 255, 255, 1.0);
      border: 1px solid #aab;
      border-radius: 8px;
      pointer-events: auto;
      font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, sans-serif;
      font-size: 13px;
      box-shadow: 0 4px 10px rgba(0,0,0,0.2);
      max-width: 300px;
      z-index: 1000;
      line-height: 1.4;
      letter-spacing: 0.2px;
    }
    
    .tooltip-title {
      color: #2c3e50;
      font-size: 15px;
      font-weight: bold;
      margin-bottom: 5px;
      letter-spacing: 0.4px;
      border-bottom: 1px solid #eee;
      padding-bottom: 3px;
    }
    
    .tooltip-table {
      border-collapse: collapse;
      width: 100%;
      margin: 4px 0;
    }
    
    .tooltip-table td {
      padding: 2px 4px;
      vertical-align: top;
    }
    
    .tooltip-label {
      font-weight: bold;
      color: #555;
      white-space: nowrap;
      padding-right: 10px;
      width: 1%;
    }
    
    .tooltip-hint {
      font-style: italic;
      color: #7f8c8d;
      font-size: 12px;
      margin-top: 4px;
      text-align: center;
    }
    
    .relationship-type {
      font-weight: bold;
      color: #34495e;
    }
    
    .reverse-relationship {
      margin: 10px 0;
      padding: 10px;
      background: #f8f9fa;
      border-left: 3px solid #3498db;
      border-radius: 4px;
    }
    
    .reverse-note {
      font-style: italic;
      color: #7f8c8d;
    }
    
    .relationship-details {
      margin-top: 10px;
      padding-top: 10px;
      border-top: 1px dotted #ccc;
    }
    
    .true-value {
      color: #2ecc71; /* Brighter green */
      font-weight: bold;
    }
    
    .false-value {
      color: #2980b9; /* Dark Blue */
    }
    
    .primary-value, .secondary-value, .keys-value, .inherited-value {
      font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
      background: #f5f5f5;
      padding: 2px 5px;
      border-radius: 3px;
      border: 1px solid #eee;
      letter-spacing: 0.5px;
    }
    
    .tooltip-section {
      margin-top: 6px;
      border-top: 1px solid #eee;
      padding-top: 3px;
    }
    
    .tooltip-section-title {
      font-weight: bold;
      color: #555;
      font-size: 12px;
      margin-bottom: 2px;
    }
    
    .tooltip-small-content {
      font-size: 11px;
      max-height: 120px;
      overflow-y: auto;
      padding-right: 2px;
    }
    
    /* Add scrollbar styling */
    .tooltip-small-content::-webkit-scrollbar {
      width: 6px;
    }
    
    .tooltip-small-content::-webkit-scrollbar-track {
      background: #f0f0f0;
      border-radius: 10px;
    }
    
    .tooltip-small-content::-webkit-scrollbar-thumb {
      background: #ccc;
      border-radius: 10px;
    }
    
    .tooltip-small-content::-webkit-scrollbar-thumb:hover {
      background: #aaa;
    }
    
    .tooltip-column, .tooltip-relation {
      line-height: 1.2;
      margin-bottom: 1px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    
    .tooltip-datatype, .tooltip-relation-type {
      color: #777;
      font-size: 10px;
    }
    
    .tooltip-more {
      font-style: italic;
      color: #777;
      font-size: 10px;
      margin-top: 2px;
    }
    
    /* Tooltip indicator styles */
    .tooltip-indicator-group {
      cursor: help;
    }
    
    .tooltip-indicator {
      transition: transform 0.2s ease, opacity 0.2s ease, fill 0.2s ease;
    }
    
    .tooltip-indicator-group:hover .tooltip-indicator {
      transform: scale(1.2);
      fill: #2980b9; /* Keep this blue hover, or change to green? Keeping blue for now. */
      filter: drop-shadow(0px 0px 2px rgba(0, 0, 0, 0.3));
    }
    
    .tooltip-indicator-text {
      pointer-events: none;
      user-select: none;
    }
    
    /* Legend line colors */
    .legend-line {
      display: inline-block;
      width: 30px;
      height: 3px;
      margin-right: 10px;
      vertical-align: middle;
    }
    
    .simple_join-line {
      background-color: #1f77b4; /* Blue */
    }
    
    .compound-line {
      background-color: #8e44ad; /* Purple */
    }
    
    .cartesian_product-line {
      background-color: #8e44ad; /* Purple */
    }

    .general_join-line {
      background-color: #ff7f0e; /* Orange */
    }
    
    .reverse-line {
      background-color: #ff7f0e; /* Orange */
    }
  `;
  document.head.appendChild(style);
}
