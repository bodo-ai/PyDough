// Geometry Utils - Utility functions for geometric calculations

/**
 * Calculate the intersection point of a line with a rectangle
 * @param {Object} node - The node (rectangle)
 * @param {number} targetX - The target x coordinate
 * @param {number} targetY - The target y coordinate
 * @returns {Object} The intersection point {x, y}
 */
export function calculateIntersection(node, targetX, targetY) {
  const centerX = node.x + node.width / 2;
  const centerY = node.y + node.height / 2;

  // Calculate angle between center of node and target point
  const angle = Math.atan2(targetY - centerY, targetX - centerX);

  // Calculate intersection point on rectangle border
  let x, y;

  if (Math.abs(Math.cos(angle)) > Math.abs(Math.sin(angle))) {
    // Intersect with left or right edge
    x = node.x + (Math.cos(angle) > 0 ? node.width : 0);
    y = centerY + Math.tan(angle) * (x - centerX);
  } else {
    // Intersect with top or bottom edge
    y = node.y + (Math.sin(angle) > 0 ? node.height : 0);
    x = centerX + (y - centerY) / Math.tan(angle);
  }

  return { x, y };
}

/**
 * Calculate the distance between two points
 * @param {number} x1 - The x coordinate of the first point
 * @param {number} y1 - The y coordinate of the first point
 * @param {number} x2 - The x coordinate of the second point
 * @param {number} y2 - The y coordinate of the second point
 * @returns {number} The distance between the points
 */
export function calculateDistance(x1, y1, x2, y2) {
  return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
}

/**
 * Calculate the center point of a node
 * @param {Object} node - The node
 * @returns {Object} The center point {x, y}
 */
export function calculateCenter(node) {
  return {
    x: node.x + node.width / 2,
    y: node.y + node.height / 2,
  };
}

/**
 * Calculate the midpoint between two points
 * @param {number} x1 - The x coordinate of the first point
 * @param {number} y1 - The y coordinate of the first point
 * @param {number} x2 - The x coordinate of the second point
 * @param {number} y2 - The y coordinate of the second point
 * @returns {Object} The midpoint {x, y}
 */
export function calculateMidpoint(x1, y1, x2, y2) {
  return {
    x: (x1 + x2) / 2,
    y: (y1 + y2) / 2,
  };
}
