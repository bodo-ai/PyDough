// Graph Styles - CSS styles for the graph components

/**
 * Apply CSS styles to the graph
 */
export function applyGraphStyles() {
  const style = document.createElement("style");
  style.textContent = `
    /* Base font for the entire visualization */
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
      stroke: #3498db;
      stroke-width: 3px;
      fill: #e3f2fd;
    }
    
    .node-primary rect {
      stroke: #2980b9;
      stroke-width: 4px;
      fill: #bbdefb;
    }
    
    /* Base text styling for all node text */
    .node text {
      font-size: 14px;
      letter-spacing: 0.3px;
    }
    
    /* Highlighted node text */
    .node-highlighted text, .node-primary text {
      fill: #111; /* Almost black */
      font-weight: bold;
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
      font-weight: 800; /* Extra bold */
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
      font-weight: 800; /* Extra bold */
    }
    
    /* Subcollection items */
    .subcollection-item {
      font-size: 14px;
    }
    
    .node-highlighted .subcollection-item, .node-primary .subcollection-item {
      font-weight: bold;
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
      font-weight: bold;
    }
    
    /* Links styling */
    .link {
      stroke: #7f8c8d;
      stroke-width: 1.8px;
      fill: none;
      transition: stroke-width 0.3s ease, stroke 0.3s ease;
    }
    
    .link-highlighted {
      stroke: #1a5082; /* Darker blue */
      stroke-width: 3px;
    }
    
    .arrowhead {
      fill: #7f8c8d;
      transition: fill 0.3s ease;
    }
    
    .arrowhead-highlighted {
      fill: #1a5082; /* Darker blue matching stroke */
    }
    
    /* Link labels (text on arrows) */
    .link-label {
      fill: #34495e;
      font-weight: bold;
      font-size: 16px;
      letter-spacing: 0.5px;
      text-shadow: 
        0px 0px 5px #fff,
        0px 0px 5px #fff;
      transition: fill 0.3s ease, font-size 0.3s ease;
    }
    
    .link-label-highlighted {
      fill: #1a365d; /* Very dark blue */
      font-weight: 800; /* Extra bold */
      font-size: 26px;
      letter-spacing: 1.2px; /* Increased letter spacing */
      text-shadow: 
        0px 0px 6px #fff,
        0px 0px 6px #fff,
        0px 0px 8px #fff,
        0px 0px 8px #fff;
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
      padding: 16px;
      background: rgba(255, 255, 255, 0.97);
      border: 1px solid #aab;
      border-radius: 8px;
      pointer-events: none;
      font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, sans-serif;
      font-size: 15px; /* Increased from 14px */
      box-shadow: 0 4px 10px rgba(0,0,0,0.2);
      max-width: 550px;
      z-index: 1000;
      line-height: 1.5;
      letter-spacing: 0.3px;
    }
    
    .tooltip strong {
      color: #2c3e50;
      font-size: 17px; /* Increased from 16px */
      letter-spacing: 0.4px;
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
      color: #27ae60;
      font-weight: bold;
    }
    
    .false-value {
      color: #e74c3c;
    }
    
    .primary-value, .secondary-value, .keys-value, .inherited-value {
      font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
      background: #f5f5f5;
      padding: 2px 5px;
      border-radius: 3px;
      border: 1px solid #eee;
      letter-spacing: 0.5px;
    }
  `;
  document.head.appendChild(style);
}
